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
from fibois.keywords import last_posted
from fibois.scrapestack import ScrapestackRequest

class ApecSpider(scrapy.Spider):
    name = 'apec.fr'
    allowed_domains = []
    dirname = 'apec.fr'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
    #keywords = occupations[144:145]
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    lua_src = pkgutil.get_data('fibois', 'lua/html-render.lua')

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
            page = self.pagination[keyword]['page']
            url = 'https://www.apec.fr/candidat/recherche-emploi.html/emploi?motsCles={keyword}&page={page}&sortsType=DATE'.format(keyword=urllib.quote_plus(keyword), page=page - 1)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = self.h.handle(' '.join(response.css('div[class="number-candidat"]').extract())).strip()
        keyword = response.meta['keyword']
        self.logger.info(h1)
        try:
            count = int(' '.join(response.css('div[class="number-candidat"] span').xpath('text()').extract()).replace(' ', '').encode('ascii','ignore'))
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        links = response.css('a[queryparamshandling="merge"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('div[class="container-result"] div[class="card-body"]').extract()
        page_results = {'job_board': 'apec.fr', 'job_board_url': 'https://www.apec.fr', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = 'https://www.apec.fr{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h2[class="card-title fs-16"]').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('p[class="card-offer__company mb-10"]').xpath('text()').extract()).strip()
                job['salary'] = ' '.join(offer_block.css('ul[class="details-offer"] li img[alt="Salaire texte"]').xpath('parent::li').xpath('text()').extract())
                job['contract_info'] = ' '.join(offer_block.css('ul[class="details-offer important-list"] li img[alt="type de contrat"]').xpath('parent::li').xpath('text()').extract()).strip()
                job['publish_date'] = ' '.join(offer_block.css('ul[class="details-offer important-list"] li img[alt="date de publication"]').xpath('parent::li').xpath('text()').extract()).strip()
                job['location'] = ' '.join(offer_block.css('ul[class="details-offer important-list"] li img[alt="localisation"]').xpath('parent::li').xpath('text()').extract()).strip()
                page_results['offers'].append(job)
                dates.append(job['publish_date'])
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['job'] = job
            yield request
        try:
            next_link = ' '.join(response.css('ul[class="pagination"] li').xpath(u"//a[text()='Suiv.']/text()").extract())
            if len(next_link) > 0:
                newest_date = self.get_newest_date(dates)
                now = datetime.datetime.now()
                delta_days = (now - newest_date).days
                if delta_days < 2 or not self.delta:
                    self.pagination[keyword]['page'] += 1
                    page = self.pagination[keyword]['page']
                    url = 'https://www.apec.fr/candidat/recherche-emploi.html/emploi?motsCles={keyword}&page={page}&sortsType=DATE'.format(keyword=urllib.quote_plus(keyword), page=page - 1)
                    if self.use_scrapestack:
                        request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
                    else:
                        args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                        request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
                    request.meta['keyword'] = keyword
                    yield request
        except Exception as ex:
            self.logger.info('pagination error: ' + str(ex))
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'www.apec.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        content = ' '.join(response.css('div[class="card-body"] div[class="row border-T"]').extract()).strip()
        #content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(content).strip()
        header = ' '.join(response.css('div[class="card-body"] div[class="card-offer__text"]').extract()).strip()
        html_content = ' '.join(response.css('div[class="card-body"]').extract()).strip()
        sector = ' '.join(response.xpath(u'//h4[text()="Secteur d’activité du poste"]/following-sibling::span[1]/text()').extract())
        experience_level = ' '.join(response.xpath(u"//h4[text() = 'Expérience']/following-sibling::span[1]/text()").extract())
        if len(sector) > 0:
            l.add_value('sector', sector)
        if len(experience_level) > 0:
            l.add_value('experience_level', experience_level)
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

    def get_newest_date(self, dates):
        newest_date = None
        if type(dates) is list:
            for date_str in dates:
                curr_date = self.str2datetime(date_str)
                if newest_date is None:
                    newest_date = curr_date
                else:
                    if newest_date < curr_date:
                        newest_date = curr_date
            if newest_date is None:
                return datetime.datetime(1970, 1, 1, 0, 0, 0)
            return newest_date
        return datetime.datetime.now()

    def str2datetime(self, date_str):
        d = list(map(lambda i: int(i.strip()), date_str.split('/')))
        dt = datetime.datetime(d[2], d[1], d[0], 0, 0, 0, 0)
        return dt









