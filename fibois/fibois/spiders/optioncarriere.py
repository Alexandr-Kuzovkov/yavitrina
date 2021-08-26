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

class OptioncarriereSpider(scrapy.Spider):
    name = 'optioncarriere'
    allowed_domains = ['www.optioncarriere.com', 'optioncarriere.com', 'api.scrapestack.com']
    dirname = 'optioncarriere'
    lua_src = pkgutil.get_data('fibois', 'lua/pole-emploi.lua')
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    #keywords = occupations[-4:-3]
    keywords = occupations
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    retry = False

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, retry=False, *args, **kwargs):
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
        if retry:
            self.retry = True

    def start_requests(self):
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            page = self.pagination[keyword]['page']
            url = 'https://www.optioncarriere.com/recherche/emplois?s={keyword}&l=France&radius=100&sort=relevance&p={page}'.format(page=page, keyword=urllib.quote_plus(keyword))
            if self.delta:
                url += '&nw=1'
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        if hasattr(response.request, 'retry') and self.retry and u'504 Gateway Time-out' in response.text:
            retry_request = response.request.retry_request()
            self.logger.info('retry_request, status={status}; text="{text}"'.format(status=response.status, text=response.text))
            if retry_request is not None:
                self.logger.info('RETRY REQUEST: url={url}; attempt={attempt}'.format(url=retry_request.url_origin, attempt=retry_request.retry['attempt']))
            yield retry_request
        else:
            h1 = ' '.join(response.css('div[id="search-content"] header[class="row"] p[class="col col-xs-12 col-m-4 col-m-r cr"] span').xpath('text()').extract()).strip()
            count = 0
            try:
                count = int(h1.split('offres')[0].replace(' ', ''))
            except Exception as ex:
                self.logger.warning(ex)
            keyword = response.meta['keyword']
            self.logger.info(h1)
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
            if self.pagination[keyword]['pages'] is None:
                self.pagination[keyword]['pages'] = int(math.ceil(count/20.0))
            links = response.css('div[id="search-content"] ul article[class="job clicky"] h2 a').xpath('@href').extract()
            results_block = response.css('article[class="job clicky"]').extract()
            self.logger.debug(links)
            dates = map(lambda i: i.strip(), filter(lambda i: len(i.strip()) > 0, response.css('div[id="search-content"] ul article[class="job clicky"] footer ul[class="tags"] li span[class="badge badge-r badge-s badge-icon"]').xpath('text()').extract()))
            page_results = {'job_board': 'optioncarriere', 'job_board_url': 'https://www.optioncarriere.com', 'page_url': response.url, 'offers': []}
            for i in range(0, len(links)):
                url = 'https://www.optioncarriere.com{link}'.format(link=links[i])
                job = {'url': url}
                job['search_term'] = response.meta['keyword']
                job['keyword_type'] = self.keywords_type
                if len(results_block) > i:
                    job['html'] = results_block[i]
                    html = results_block[i]
                    body = html.encode('utf-8')
                    article_block = response.replace(body=body)
                    job['title'] = ' '.join(article_block.css('h2 a').xpath('@title').extract())
                    job['company_name'] = ' '.join(article_block.css('p[class="company"]').xpath('text()').extract())
                    location = ' '.join(article_block.css('ul[class="details"] li svg[class="icon"]').xpath('parent::li/text()').extract()).strip()
                    if len(location) == 0:
                        location = ' '.join(article_block.css('ul[class="location"] li svg[class="icon"]').xpath('parent::li/text()').extract()).strip()
                    job['location'] = location
                    publish_date = ' '.join(map(lambda i: i.strip(), filter(lambda i: len(i.strip()) > 0, article_block.css('footer ul[class="tags"] li span[class="badge badge-r badge-s badge-icon"]').xpath('text()').extract())))
                    if len(publish_date) == 0:
                        publish_date = ' '.join(map(lambda i: i.strip(), filter(lambda i: len(i.strip()) > 0, article_block.css('footer ul[class="tags"] li span[class="badge badge-r badge-s"]').xpath('text()').extract())))
                    job['publish_date'] = publish_date
                    page_results['offers'].append(job)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_detail_page, dont_filter=True, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
                request.meta['job'] = job
                yield request
            if self.pagination[keyword]['page'] < self.pagination[keyword]['pages'] and count > 0:
                if self.check_published_at(dates) or not self.delta:
                    self.pagination[keyword]['page'] += 1
                    page = self.pagination[keyword]['page']
                    url = 'https://www.optioncarriere.com/recherche/emplois?s={keyword}&l=France&radius=100&sort=relevance&p={page}'.format(page=page, keyword=urllib.quote_plus(keyword))
                    if self.delta:
                        url += '&nw=1'
                    if self.use_scrapestack:
                        request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                    else:
                        request = scrapy.Request(url, callback=self.parse_job_list_page)
                    request.meta['keyword'] = keyword
                    yield request
            if not self.drain:
                self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        if hasattr(response.request, 'retry') and self.retry and u'504 Gateway Time-out' in response.text:
            retry_request = response.request.retry_request()
            self.logger.info('retry_request, status={status}; text="{text}"'.format(status=response.status, text=response.text))
            if retry_request is not None:
                self.logger.info('RETRY REQUEST: url={url}; attempt={attempt}'.format(url=retry_request.url_origin, attempt=retry_request.retry['attempt']))
            yield retry_request
        else:
            job = response.meta['job']
            l = ItemLoader(item=JobItem(), response=response)
            for key, val in job.items():
                l.add_value(key, val)
            l.add_value('jobboard', 'optioncarriere')
            l.add_value('scrapping_date', self.get_scrapping_date())
            l.add_value('contact', '')
            l.add_value('url_origin', job['url'])
            salary = ' '.join(response.css('article[id="job"] svg[class="icon"]').xpath("*[name()='use' and @*='#icon-money']").xpath('parent::svg/parent::li').xpath('text()').extract()).strip()
            contract_type = ' '.join(response.css('article[id="job"] svg[class="icon"]').xpath("*[name()='use' and @*='#icon-contract']").xpath('parent::svg/parent::li').xpath('text()').extract()).strip()
            contract_duration = ' '.join(response.css('article[id="job"] svg[class="icon"]').xpath("*[name()='use' and @*='#icon-duration']").xpath('parent::svg/parent::li').xpath('text()').extract()).strip()
            content = ' '.join(response.css('article[id="job"] section[class="content"]').extract())
            content = self.h.handle(content).strip()
            header = ' '.join((response.css('article[id="job"] header').extract()))
            html_content = ' '.join((response.css('article[id="job"] section[class="content"]').extract()))

            if len(contract_type) > 0:
                l.add_value('contract_type', contract_type)
            if len(contract_duration) > 0:
                l.add_value('contract_duration', contract_duration)
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

    def check_published_at(self, dates):
        if type(dates) is list:
            for date in dates:
                if date.strip() in last_posted:
                    return True
        return False

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date







