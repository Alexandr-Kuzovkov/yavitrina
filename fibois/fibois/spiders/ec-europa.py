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

class EcEuropaSpider(scrapy.Spider):
    name = 'ec-europa'
    allowed_domains = ['ec.europa.eu', 'api.scrapestack.com']
    dirname = 'ec-europa'
    lua_src = pkgutil.get_data('fibois', 'lua/ec-europa.lua')
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta_days = 1
    keywords = occupations
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    delta = False
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
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, *args, **kwargs):
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
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}

    def start_requests(self):
        for keyword in self.keywords:
            url = 'https://ec.europa.eu/eures/portal/jv-se/search?page={page}&resultsPerPage=50&orderBy=MOST_RECENT&locationCodes=fr&availableLanguages=fr&keywordsEverywhere={keyword}'.format(page=1, keyword=urllib.quote_plus(keyword))
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h2 = ' '.join(response.css('h2[class="ecl-u-type-heading-2 ecl-u-mt-none"]').xpath('text()').extract())
        keyword = response.meta['keyword']
        self.logger.info(u'keyword: "{keyword}" - "{h2}"'.format(keyword=unicode(keyword, 'utf-8'), h2=h2))
        links = response.css('article h3 a').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('article').extract()
        dates = response.css('article div[id="jv-last-modification-date"] em').xpath('text()').extract()
        self.logger.info('count jobs: {count}'.format(count=len(links)))
        page_results = {'job_board': 'ec.europa.eu', 'job_board_url': 'https://ec.europa.eu', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://ec.europa.eu{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = response.meta['keyword']
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h3 a').xpath('text()').extract())
                job['company_name'] = ' '.join(offer_block.css('ul li[id="jv-employer-name"] span[class="ecl-u-type-s ecl-u-ml-xs"]').xpath('text()').extract())
                job['location'] = ' '.join(offer_block.css('ul li[id="jv-countries-regions"] span[class="ecl-u-type-s ecl-u-ml-xs"]').xpath('text()').extract())
                job['contract_type'] = ' '.join(offer_block.css('ul li[id="jv-category-type-code"] span[class="ecl-u-type-s ecl-u-ml-xs"]').xpath('text()').extract())
                job['category'] = ' '.join(offer_block.css('ul li[id="jv-category-type-code"] span[class="ecl-u-type-s ecl-u-ml-xs"]').xpath('text()').extract())
                job['publish_date'] = ' '.join(offer_block.css('div[id="jv-last-modification-date"] em').xpath('text()').extract())
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, options={'render_js': 1}, dont_filter=True)
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=True)
            request.meta['job'] = job
            yield request
        newest_date = self.get_newest_date(dates)
        now = datetime.datetime.now()
        delta_days = (now - newest_date).days
        page = self.pagination[keyword]['page']
        if self.limit is None or page < self.limit:
            if delta_days < self.delta_days or not self.delta:
                self.pagination[keyword]['page'] += 1
                url = 'https://ec.europa.eu/eures/portal/jv-se/search?page={page}&resultsPerPage=50&orderBy=MOST_RECENT&locationCodes=fr&availableLanguages=fr&keywordsEverywhere={keyword}'.format(page=page, keyword=urllib.quote_plus(keyword))
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
                else:
                    args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                    request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
                request.meta['keyword'] = keyword
                yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'ec.europa.eu')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        content = ''.join(response.css('section[id="sticky-menu-content"]').extract())
        #content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(content).strip()
        sector = ''.join(response.css('li[id="jv-employer-sector-codes"] strong').xpath('text()').extract())
        contract_info = ''.join(response.css('li[id="jv-position-schedule"] strong').xpath('text()').extract())
        job_type = ''.join(response.css('li[id="jv-position-type-code"] strong').xpath('text()').extract())
        category = ''.join(response.css('li[id="jv-job-categories-codes"] strong').xpath('text()').extract())
        profile = ''.join(response.css('div[id="sticky-menu-content-requirements"]').extract())
        #profile = self.h.handle(profile).encode('utf-8').strip()
        profile = self.h.handle(profile).strip()
        header = ' '.join(response.css('div[id="sticky-menu-content-overview"]').extract())
        html_content = ' '.join(response.css('section[id="sticky-menu-content"]').extract())
        if len(sector) > 0:
            l.add_value('sector', sector)
        if len(contract_info) > 0:
            l.add_value('contract_info', contract_info)
        if len(job_type) > 0:
            l.add_value('job_type', job_type)
        if len(category) > 0:
            l.add_value('category', category)
        if len(profile) > 0:
            l.add_value('profile', profile)
        l.add_value('content', content)
        l.add_value('source', 'scrapy')
        l.add_value('header', header)
        l.add_value('html_content', html_content)
        yield l.load_item()
        job_details = {'html': html_content, 'title': header, 'url': job['url']}
        if not self.drain:
            self.es_exporter.insert_job_details_html(job_details)

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

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date






