# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
from fibois.items import JobStatus
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.keywords import last_posted
from fibois.keywords import job_expired_messages
from fibois.scrapestack import ScrapestackRequest


#Worker for marking expired jobs
class ExpiredSpider(scrapy.Spider):
    name = 'expired'
    allowed_domains = []
    dirname = 'expired'
    handle_httpstatus_list = [400, 404, 410, 401, 403, 408, 500, 502, 503, 504]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    lua_src = pkgutil.get_data('fibois', 'lua/html-render.lua')
    debug = False
    jobboard = False
    request_stat = {}
    response_stat = {}
    render_js_boards = [
        'ec.europa.eu',
        'www.apec.fr',
        'cadremploi.fr',
        'glassdoor.fr',
        'lefigaro.fr'
    ]

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, noproxy=False, limit=False, keywords_limit=False, drain=False, delta=False, debug=False, jobboard=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True
        if keywords_limit:
            self.keywords_limit = int(keywords_limit)
        if debug:
            self.debug = True
        if noproxy:
            self.use_scrapestack = False
        if jobboard:
            self.jobboard = jobboard

    def start_requests(self):
        if self.debug:
            self.logger.info('RUN DEBUG!!!!!!!!!!')
            test_data = [
                {
                    'url': 'https://lbmexecutivesearch.com/offre/technicien-materiel-electroportatif-h-f/', #duplicate
                    'id': 'OXG2T3kBrrLKlcc4SlsK'
                },
                {
                    'url': 'https://lbmexecutivesearch.com/offre/technicien-materiel-electroportatif-h-f-3/', #ok
                    'id': 'UEmcbnkBPRtF16HoZMZ_'
                },
                {
                    'url': 'https://lbmexecutivesearch.com/offre/technicienne-hygieniste-h-f/', #expired
                    'id': 'M2ipK3kBrrLKlcc4vnhN'
                },
            ]
            for data_item in test_data:
                url = data_item['url']
                _id = data_item['id']
                index = self.es_exporter.index
                es = self.es_exporter.es
                job = es.get(index=index, id=_id,  _source=['url', 'jobboard', 'url_status', 'scrapping_date'])
                pprint(job)
                if self.use_scrapestack:
                    if job['_source']['jobboard'] in ['onf.fr']:
                        url = 'http://www1.onf.fr/carrieres/sommaire/postuler/postuler/@@index.html'
                        request = ScrapestackRequest(url, callback=self.open_form, access_key=self.scrapestack_access_key)
                    else:
                        request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
                else:
                    if job['_source']['jobboard'] in ['onf.fr']:
                        url = 'http://www1.onf.fr/carrieres/sommaire/postuler/postuler/@@index.html'
                        request = scrapy.Request(url, callback=self.open_form, dont_filter=True)
                    else:
                        request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
                request.meta['job'] = job
                self._request_count(request)
                yield request
        else:
            es = self.es_exporter.es
            index = self.es_exporter.index
            if self.jobboard:
                query_body = {
                                "query": {
                                    "bool": {
                                        "must": [
                                          {
                                              "term": {
                                                  "jobboard.keyword": self.jobboard
                                              }
                                          },
                                          {
                                            "term": {
                                                  "url_status": "valid"
                                                }
                                          }
                                        ]
                                    }
                                }
                            }
            else:
                query_body = {
                              "query": {
                                "bool": {
                                  "should": [
                                    {
                                      "term": {
                                            "url_status": "valid"
                                          }
                                    },
                                    {
                                      "bool": {
                                        "must_not": [
                                            {
                                              "exists": {
                                                 "field": "url_status"
                                              }
                                           }
                                        ]
                                      }
                                    }
                                  ]
                                }
                              }
                            }
            total = es.count(index=index, body=query_body)['count']
            cnt = 0
            changed = 0
            self.logger.info('Total: {total}'.format(total=total))
            count = total
            if count:
                page = es.search(index=index, scroll='1h', size=1000, body=query_body, _source=['url', 'jobboard', 'url_status', 'scrapping_date'])
                jobs = page['hits']['hits']
                for job in jobs:
                    data = job['_source']
                    url = data['url']
                    jobboard = data['jobboard']
                    # request job
                    if jobboard in self.render_js_boards:
                        if self.use_scrapestack:
                            request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
                        else:
                            args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                            request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
                    elif jobboard in ['onf.fr']:
                        url = 'http://www1.onf.fr/carrieres/sommaire/postuler/postuler/@@index.html'
                        request = scrapy.Request(url, callback=self.open_form, dont_filter=True)
                    else:
                        request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
                    request.meta['job'] = job
                    self._request_count(request)
                    yield request
                sid = page['_scroll_id']
                scroll_size = page['hits']['total']
                # Start scrolling
                while (scroll_size > 0):
                    # print('Scrolling...')
                    try:
                        page = es.scroll(scroll_id=sid, scroll='1h')
                    except Exception as ex:
                        self.logger.warning(ex)
                        continue
                    # Update the scroll ID
                    sid = page['_scroll_id']
                    # Get the number of results that we returned in the last scroll
                    scroll_size = len(page['hits']['hits'])
                    # logger.info('{n} jobs fetched'.format(n=scroll_size))
                    jobs = page['hits']['hits']
                    for job in jobs:
                        data = job['_source']
                        url = data['url']
                        jobboard = data['jobboard']
                        # request job
                        if jobboard in self.render_js_boards:
                            if self.use_scrapestack:
                                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
                            else:
                                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
                        elif jobboard in ['onf.fr']:
                            url = 'http://www1.onf.fr/carrieres/sommaire/postuler/postuler/@@index.html'
                            request = scrapy.Request(url, callback=self.open_form, dont_filter=True)
                        else:
                            request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
                        request.meta['job'] = job
                        self._request_count(request)
                        yield request

    def open_form(self, response):
        url = 'https://www.altays-progiciels.com/clicnjob/RechOffre.php?NoSociete=157&NoSocFille=0&NoModele=97&NoLangue=1&NoSource=1&Autonome=1'
        request = scrapy.Request(url, callback=self.open_job_list, dont_filter=True)
        request.meta['job'] = response.meta['job']
        yield request

    def open_job_list(self, response):
        url = 'https://www.altays-progiciels.com/clicnjob/ListeOffreCand.php?NoTableOffreLieePl=0&NoTypContratl=0&NoNivEtl=0&NoPaysl=0&RefOffrel=&RechPleinTexte='
        request = scrapy.Request(url, callback=self.get_onf_job, dont_filter=True)
        request.meta['job'] = response.meta['job']
        yield request

    def get_onf_job(self, response):
        job = response.meta['job']
        data = job['_source']
        url = data['url']
        request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
        request.meta['job'] = response.meta['job']
        yield request

    def parse_job_detail_page(self, response):
        #print(response.text)
        #data = json.loads(response.text)
        # checking is job expired
        self._response_count(response)
        job = response.meta['job']
        _id = job['_id']
        url = job['_source']['url']
        jobboard = job['_source']['jobboard']
        l = ItemLoader(item=JobStatus(), response=response)
        expired_status = self.is_job_expired(response, jobboard, job)
        if expired_status is not None:
            if expired_status:
                url_status = 'expired'
            else:
                url_status = 'valid'
            l.add_value('_id', _id)
            l.add_value('url', url)
            l.add_value('url_status', url_status)
            self.logger.info('{url}: {code} {url_status}'.format(url=response.url, code=response.status, url_status=url_status))
            yield l.load_item()
        else:
            self.logger.warning('{url}: {code} {jobboard}'.format(url=response.url, code=response.status, jobboard=jobboard))

    def is_job_expired(self, response, jobboard, job):
        url = job['_source']['url']
        self.logger.info('response code: {code}, url_origin: {url_origin}, url: {url}'.format(code=response.status, url_origin=url, url=response.url))
        if response.status in [200]:
            for message in job_expired_messages:
                if message in response.text:
                    return True
            if jobboard == 'lbmexecutivesearch':
                if url != response.url:
                    return True
            if jobboard == 'onf.fr':
                control_text = ''.join(response.css('td[class="detail-offre-titre"] span[class="titre1"]').xpath('text()').extract())
                if control_text == u"Détail de l'offre":
                    print 'VALID'
                    return False
                else:
                    print 'NOT VALID'
                    return True
            return False
        elif response.status in [401, 403, 408, 500, 502, 503, 504]:
            return None
        else:
            return True

    def _request_count(self, request):
        job = request.meta['job']
        jobboard = job['_source']['jobboard']
        if jobboard in self.request_stat:
            self.request_stat[jobboard] += 1
        else:
            self.request_stat[jobboard] = 1

    def _response_count(self, response):
        job = response.meta['job']
        jobboard = job['_source']['jobboard']
        if jobboard in self.response_stat:
            self.response_stat[jobboard] += 1
        else:
            self.response_stat[jobboard] = 1

    def rundebug(self):
        self.logger.info('RUN DEBUG!!!!!!!!!!')








