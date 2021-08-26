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

class keljobSpider(scrapy.Spider):
    name = 'keljob'
    allowed_domains = ['www.keljob.com', 'keljob.com']
    dirname = 'keljob'
    handle_httpstatus_list = [400, 404, 403]
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
            url = 'https://www.keljob.com/recherche?q={keyword}'.format(keyword=urllib.quote_plus(keyword))
            if self.delta:
                url = '{url}&d=1d'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.url)
        h1 = ' '.join(response.css('h1[class="search-counter no-margin-bottom"] strong').xpath('text()').extract())
        count = 0
        keyword = response.meta['keyword']
        try:
            count = int(' '.join(response.css('h1[class="search-counter no-margin-bottom"] strong').xpath('text()').extract()).replace(' ', ''))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        self.logger.debug(h1)
        self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/20.0))
            self.logger.info('{pages} pages for keyword={keyword}'.format(pages=self.pagination[keyword]['pages'], keyword=keyword))
        links = response.css('div[id="content-list-container"] h2[class="offre-title"] a').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('div[id="content-list-container"] div[class="column"] div[class="row"]').extract()
        page_results = {'job_board': 'keljob', 'job_board_url': 'https://www.keljob.com', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://www.keljob.com{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = response.meta['keyword']
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h2[class="offre-title"] a span[class="title"]').xpath('text()').extract())
                job['publish_date'] = self.h.handle(' '.join(offer_block.css('ul[class="offre-attributes"] li[class="offre-date"]').extract())).strip()
                job['contract_info'] = self.h.handle(' '.join(offer_block.css('ul[class="offre-attributes"] li[class="offre-contracts"]').extract())).strip()
                job['company_name'] = self.h.handle(' '.join(offer_block.css('ul[class="offre-attributes"] li[class="offre-company"]').extract())).strip()
                job['location'] = self.h.handle(' '.join(offer_block.css('ul[class="offre-attributes"] li[class="offre-location"]').extract())).strip()
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, dont_filter=True, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages']:
            self.pagination[keyword]['page'] += 1
            url = 'https://www.keljob.com/recherche?q={keyword}&page={page}'.format(keyword=urllib.quote_plus(keyword), page=self.pagination[keyword]['page'])
            self.logger.info(url)
            self.logger.info('fetch page={page} for keyword="{keyword}"'.format(page=self.pagination[keyword]['page'], keyword=keyword))
            if self.delta:
                url = '{url}&d=1d'.format(url=url)
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
        l.add_value('jobboard', 'keljob')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        content = ' '.join(response.css('div[id="content-container"]').extract())
        #content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(content).strip()
        sector = ' '.join(response.css('div[class="jobs-detail__header"] ul[class="sub-title__elements"] li')[-1:].xpath('text()').extract()).strip()

        profile = self.get_profile(response)
        header = ' '.join(response.css('div[class="jobs-detail__header"]').extract())
        html_content = ' '.join(response.css('div[id="content-container"]').extract())
        if len(sector) > 0:
            l.add_value('sector', sector)
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

    def get_profile(self, response):
        profile = ' '.join(response.xpath("//h2[contains(text(), 'Le profil')]").xpath('parent::section').css('div[class="job-paragraph"]').extract())
        return self.h.handle(profile).strip()

    def check_published_at(self, response):
        if not self.delta:
            return True
        dates = response.css('ul[class="result-list list-unstyled"] li p[class="date"]').xpath('text()').extract()
        #pprint(dates)
        if type(dates) is list:
            for date in dates:
                if date.strip() in [u"Publié aujourd'hui", u"Publié il y a 1 jours", u"Publié hier"]:
                    return True
        return False

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date







