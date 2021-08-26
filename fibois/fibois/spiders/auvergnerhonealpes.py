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
from fibois.items import JobItem
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.scrapestack import ScrapestackRequest

class AuvergnerhonealpesSpider(scrapy.Spider):
    name = 'auvergnerhonealpes'
    allowed_domains = ['nostalentsnosemplois.auvergnerhonealpes.fr']
    dirname = 'auvergnerhonealpes'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
    #keywords = occupations[108:109]
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, noproxy=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True
        if keywords_limit:
            self.keywords_limit = int(keywords_limit)
        if keywords:
            if keywords == 'companies':
                self.keywords = companies
                self.keywords_type = 'companies'
        if self.keywords_limit is not None:
            self.keywords = self.keywords[0:self.keywords_limit]
        if noproxy:
            self.use_scrapestack = False

    def start_requests(self):
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            url = 'https://nostalentsnosemplois.auvergnerhonealpes.fr/jobsearch/offers?what={keyword}'.format(keyword=urllib.quote_plus(keyword))
            if self.delta:
                url = '{url}&_since_=day'.format(url=url) #&_since_=week
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.url)
        h1 = ' '.join([' '.join(response.css('h1[class="mj-title-small"]').xpath('text()').extract()).strip(), ' '.join(response.css('h1[class="mj-title-small"] span').xpath('text()').extract()).strip()])
        count = 0
        keyword = response.meta['keyword']
        try:
            count = int(' '.join(response.css('h1[class="mj-title-small"]').xpath('text()').extract()).strip().split(' ')[0].replace('+', '').encode('ascii','ignore'))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        self.logger.debug(h1)
        self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/15.0))
            self.logger.info('{pages} pages for keyword={keyword}'.format(pages=self.pagination[keyword]['pages'], keyword=keyword))
        links = response.css('ul[class="mj-offers-list"] a[class="block-link"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('ul[class="mj-offers-list"] article').extract()
        page_results = {'job_board': 'nostalentsnosemplois.auvergnerhonealpes.fr', 'job_board_url': 'https://nostalentsnosemplois.auvergnerhonealpes.fr', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://nostalentsnosemplois.auvergnerhonealpes.fr/{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = response.meta['keyword']
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h2[class="title"]').xpath('text()').extract()).strip()
                try:
                    job['contract_info'] = offer_block.css('div[class="info"] ul h3').xpath('text()').extract()[0]
                except Exception as ex:
                    pass
                try:
                    if len(offer_block.css('div[class="info"] ul h3').xpath('text()').extract()) > 2:
                        job['contract_duration'] = offer_block.css('div[class="info"] ul h3').xpath('text()').extract()[1]
                except Exception as ex:
                    pass
                try:
                    job['location'] = offer_block.css('div[class="info"] ul h3').xpath('text()').extract()[-1:]
                except Exception as ex:
                    pass
                job['publish_date'] = ' '.join(offer_block.css('div[class="tags"] div[class="published-date"]').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('div[class="logo  announcer"] span p').xpath('text()').extract()).strip()
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, dont_filter=True, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        next_btn = len(response.css('a[class="pagination-action"] span[class="fa fa-angle-double-right"]'))
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages'] or next_btn > 0:
            self.pagination[keyword]['page'] += 1
            url = 'https://nostalentsnosemplois.auvergnerhonealpes.fr/jobsearch/offers?what={keyword}&page={page}'.format(keyword=urllib.quote_plus(keyword), page=self.pagination[keyword]['page'])
            self.logger.info(url)
            self.logger.info('fetch page={page} for keyword="{keyword}"'.format(page=self.pagination[keyword]['page'], keyword=keyword))
            if self.delta:
                url = '{url}&_since_=day'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, dont_filter=True)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'nostalentsnosemplois.auvergnerhonealpes.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = response.css('div[class="mj-offer-details mj-block "] header').extract()
        html_content = ' '.join(response.css('div[class="mj-offer-details mj-block "] section').extract()).replace(' '.join(response.css('div[class="mj-offer-details mj-block "] section[class="offer-apply-form"]').extract()), '')
        content = self.h.handle(html_content).strip()
        #content = self.h.handle(content).encode('utf-8').strip()
        l.add_value('content', content)
        l.add_value('source', 'scrapy')
        l.add_value('header', header)
        l.add_value('html_content', html_content)
        yield l.load_item()
        job_details = {'html': html_content, 'title': header, 'url': job['url']}
        if not self.drain:
            self.es_exporter.insert_job_details_html(job_details)

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date







