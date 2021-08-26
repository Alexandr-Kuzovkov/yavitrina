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

class MeteojobSpider(scrapy.Spider):
    name = 'meteojob'
    allowed_domains = ['www.meteojob.com']
    dirname = 'meteojob'
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
            url = 'https://www.meteojob.com/jobsearch/offers?what={keyword}&sorting=DATE'.format(keyword=urllib.quote_plus(keyword))
            if self.delta:
                url = '{url}&_since_=day'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = self.h.handle(' '.join(response.css('h1[class="mj-title-small"]').extract())).strip()
        keyword = response.meta['keyword']
        self.logger.info(h1)
        count = 0
        try:
            count = int(' '.join(response.css('h1[class="mj-title-small"]').xpath('text()').extract()).strip()
                        .replace(' ', '').replace('+', '').replace("offresd'emploi", '')
                        .encode('ascii', 'ignore'))
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/15.0))
            self.logger.info('{pages} pages for keyword={keyword}'.format(pages=self.pagination[keyword]['pages'], keyword=keyword))
        links = response.css('ul[class="mj-offers-list"] article[class="mj-offer  "] a[class="block-link"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('ul[class="mj-offers-list"] article[class="mj-offer  "]').extract()
        page_results = {'job_board': 'meteojob', 'job_board_url': 'https://www.meteojob.com', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = 'https://www.meteojob.com/{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('a[class="block-link"] span').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('div[class="logo  "] img').xpath('@alt').extract()).strip()
                job['contract_info'] = ' '.join(offer_block.css('div[class="info"] h3').xpath('text()').extract()[:-1])
                job['location'] = ' '.join(offer_block.css('div[class="info"] h3').xpath('text()').extract()[-1:])
                job['publish_date'] = ' '.join(offer_block.css('div[class="published-date"]').xpath('text()').extract())
                page_results['offers'].append(job)
                dates.append(job['publish_date'])
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages']:
            self.pagination[keyword]['page'] += 1
            page=self.pagination[keyword]['page']
            url = 'https://www.meteojob.com/jobsearch/offers?what={keyword}&sorting=DATE&page={page}'.format(keyword=urllib.quote_plus(keyword), page=page)
            if self.delta:
                url = '{url}&_since_=day'.format(url=url)
            self.logger.info(url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, dont_filter=True, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'meteojob')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('div[class="mj-column-content"] div[class="mj-offer-details mj-block "] header').extract()).strip()
        html_content = '\n'.join([header, ' '.join(response.css('div[class="mj-column-content"] section')[:-1].extract()).strip()])
        # content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(html_content).strip()
        sector = ' '.join(response.css('div[class="mj-column-content"] div[class="mj-offer-details mj-block "] header').xpath(u'//span[normalize-space(text()) = "Secteur :"]/span/text()').extract()).strip()
        experience_level = ', '.join(filter(lambda j: len(j.strip()) > 0, map(lambda i: i.strip(), response.css('div[class="mj-column-content"] div[class="mj-offer-details mj-block "] header').xpath(u'//span[normalize-space(text()) = "Expérience requise :"]/span/text()').extract())))
        education_level = ', '.join(filter(lambda j: len(j.strip()) > 0, map(lambda i: i.strip(), response.css('div[class="mj-column-content"] div[class="mj-offer-details mj-block "] header').xpath(u'//span[normalize-space(text()) = "Niveau d\'études :"]/span/text()').extract())))
        if len(sector) > 0:
            l.add_value('sector', sector)
        if len(experience_level) > 0:
            l.add_value('experience_level', experience_level)
        if len(education_level) > 0:
            l.add_value('education_level', education_level)
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










