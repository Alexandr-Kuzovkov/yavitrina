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
from fibois.keywords import lesjeudis_keywords as occupations
from fibois.keywords import companies
from fibois.keywords import last_posted
from fibois.scrapestack import ScrapestackRequest

class LesjeudisSpider(scrapy.Spider):
    name = 'lesjeudis'
    allowed_domains = ['www.lesjeudis.com', 'lesjeudis.com', 'api.scrapestack.com']
    dirname = 'lesjeudis'
    lua_src = pkgutil.get_data('fibois', 'lua/lesjeudis.lua')
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
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
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = int(delta)
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

    def start_requests(self):
        for keyword in self.keywords:
            url = 'https://www.lesjeudis.com/recherche?q={keyword}&sort=date'.format(keyword=urllib.quote_plus(keyword))
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.css('div[class="container clearfix"] h1').xpath('text()').extract())
        keyword = response.meta['keyword']
        self.logger.info(h1)
        links = response.css('div[id="jobs-content"] div[class="job"] a[class="job-title"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('div[id="jobs-content"] div[class="job"]').extract()
        dates = response.css('div[class="job-info"] div[class="date"] a').xpath('text()').extract()
        page_results = {'job_board': 'lesjeudis.com', 'job_board_url': 'https://www.lesjeudis.com', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://www.lesjeudis.com{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('a[class="job-title"]').xpath('text()').extract())
                job['company_name'] = ' '.join(offer_block.css('div[class="job-info"] i[class="fa fa-building-o"] + a').xpath('text()').extract())
                job['location'] = ' '.join(offer_block.css('div[class="job-info"] a i[class="fa fa-map-marker"]').xpath('parent::a').xpath('text()').extract())
                job['contract_info'] = ' '.join(offer_block.css('div[class="job-info"] div i[class="fa fa-suitcase"]').xpath('parent::div').xpath('text()').extract())
                job['publish_date'] = ' '.join(offer_block.css('div[class="job-info"] div[class="date"] a').xpath('text()').extract())
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        link_next = ''.join(response.css('div[id="jrp-pagination"] i[class="fa fa-chevron-right show-mobile"]').xpath('parent::a').xpath('@href').extract())
        if self.check_published_at(dates) or not self.delta:
            if len(link_next) > 0:
                url_next = 'https://www.lesjeudis.com{link}'.format(link=link_next)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url_next, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url_next, callback=self.parse_job_list_page)
                request.meta['keyword'] = keyword
                yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'lesjeudis.com')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        profile = ','.join(response.css('div[class="tags"] a').xpath('text()').extract())
        salary = ' '.join(response.css('i[class="fa bubble-icon fa-eur"]').xpath('parent::div').xpath('text()').extract())
        content = ' '.join(response.css('div[id="job-description"]').extract())
        #content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(content).strip()
        header = ' '.join([' '.join(response.css('div[id="jdp-title"]').extract()), ' '.join(response.css('div[id="jdp-title"] + div').extract()), ' '.join(response.css('div[id="jdp-title"] + div + div').extract())])
        html_content = ' '.join([' '.join(response.css('div[id="job-description"]').extract()), ' '.join(response.css('div[id="job-id"]').extract())])
        if len(profile) > 0:
            l.add_value('profile', profile)
        if len(salary) > 0:
            l.add_value('salary', salary)
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

    def check_published_at(self, dates):
        if type(dates) is list:
            for date in dates:
                if date.strip() in last_posted:
                    return True
        return False








