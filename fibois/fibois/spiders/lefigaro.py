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
from fibois.keywords import last_posted

class LefigaroSpider(scrapy.Spider):
    name = 'lefigaro'
    dirname = 'lefigaro'
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
    def __init__(self, keywords=False, limit=False, drain=False, delta=False, noproxy=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True
        if keywords:
            if keywords == 'companies':
                self.keywords = companies
                self.keywords_type = 'companies'
        if noproxy:
            self.use_scrapestack = False

    def start_requests(self):
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            url = 'https://emploi.lefigaro.fr/recherche?q={keyword}'.format(keyword=urllib.quote_plus(keyword))
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.get_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.get_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def get_job_list_page(self, response):
        link = ' '.join(response.css('a[class="search--bottom--body--results--show-more"]').xpath('@href').extract())
        if len(link) > 0 and 'offres-emplois' in link:
            url = 'https://emploi.lefigaro.fr{link}'.format(link=link)
            keyword = response.meta['keyword']
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.url)
        h1 = ' '.join(response.css('div[class="job-search--top--results"] b').xpath('text()').extract()).strip()
        count = 0
        keyword = response.meta['keyword']
        try:
            count = int(' '.join(response.css('div[class="job-search--top--results"] b').xpath('text()').extract()).strip().split(' ')[0])
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        self.logger.debug(h1)
        self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/20.0))
            self.logger.info('{pages} pages for keyword={keyword}'.format(pages=self.pagination[keyword]['pages'], keyword=keyword))
        links = response.css('a[class="search-result-job-card job-search--bottom--body--items--search-results--job-card"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('a[class="search-result-job-card job-search--bottom--body--items--search-results--job-card"]').extract()
        page_results = {'job_board': 'lefigaro.fr', 'job_board_url': 'https://emploi.lefigaro.fr', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = 'https://emploi.lefigaro.fr{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = response.meta['keyword']
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('span[class="search-result-job-card--details--title"]').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('div[class="search-result-job-card--details--info--logo"] + span').xpath('text()').extract()).strip()
                job['publish_date'] = ' '.join(offer_block.css('div[class="search-result-job-card--details--info"] span')[-1:].xpath('text()').extract()).strip()
                if len(job['publish_date']) > 0:
                    dates.append(job['publish_date'])
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['job'] = job
            yield request
        next_link = ' '.join(response.css('div[class="full-pagination--user"] li[class="page-item"] a[aria-label="Go to next page"]').xpath('@href').extract())
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages'] and len(next_link) > 0:
            if self.check_published_at(dates) or not self.delta:
                self.pagination[keyword]['page'] += 1
                url = 'https://emploi.lefigaro.fr{link}'.format(link=next_link)
                self.logger.info(url)
                self.logger.info('fetch page={page} for keyword="{keyword}"'.format(page=self.pagination[keyword]['page'], keyword=keyword))
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
        l.add_value('jobboard', 'lefigaro.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('div[class="job--header"]').extract()).strip()
        html_content = ' '.join(response.css('div[class="section job--content--section"]').extract()).strip()
        content = self.h.handle(html_content).strip()
        #content = self.h.handle(content).encode('utf-8').strip()
        l.add_value('content', content)
        location = ' '.join(response.css('div[class="jobs-header--info--details"] img[alt="pin"] + span').xpath('text()').extract())
        contract_type = ' '.join(response.css('div[class="jobs-header--info--details"] img[alt="contract"] + span').xpath('text()').extract())
        publish_date = '' #' '.join(response.css('div[class="jobs-header--info--details"] img[alt="clock"] + span').xpath('text()').extract())
        company_name = '' #' '.join(response.css('div[class="jobs-header--info--company"] b').xpath('text()').extract())
        if len(location) > 0:
            l.add_value('location', location)
        if len(contract_type) > 0:
            l.add_value('contract_type', contract_type)
        if len(publish_date) > 0:
            l.add_value('publish_date', publish_date)
        if len(company_name) > 0:
            l.add_value('company_name', company_name)
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

    def check_date(self, dates):

        return True

    def check_published_at(self, dates):
        if type(dates) is list:
            for date in dates:
                if date.strip() in last_posted:
                    return True
        return False







