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

class LbmexecutivesearchSpider(scrapy.Spider):
    name = 'lbmexecutivesearch'
    allowed_domains = []
    dirname = 'lbmexecutivesearch'
    handle_httpstatus_list = [400, 404, 401]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    scrapestack_access_key = ''
    use_scrapestack = False
    es_exporter = None
    lua_src = pkgutil.get_data('fibois', 'lua/html-render.lua')
    page = 1
    pages = None
    nonce = ''

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, drain=False, delta=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if delta:
            self.delta = True
        if drain:
            self.drain = True

    def start_requests(self):
        url = 'https://lbmexecutivesearch.com/offre/'
        if self.use_scrapestack:
            request = ScrapestackRequest(url, callback=self.get_jobs_list_ajax, access_key=self.scrapestack_access_key)
        else:
            request = scrapy.Request(url, callback=self.get_jobs_list_ajax, dont_filter=True)
        yield request

    def get_jobs_list_ajax(self, response):
        h1 = self.h.handle(' '.join(response.css('div[class="jobs-list__header__count"]').extract()).strip()).strip()
        self.logger.info('header={header}'.format(header=h1.encode('utf-8')))
        count = 0
        count_header = ' '.join(response.css('div[class="jobs-list__header__count"]').xpath('text()').extract()).strip()
        try:
            count = int(''.join(count_header.split(' ')[0:1]))
            self.logger.info('count={count}'.format(count=count))
            self.pages = int(math.ceil(count/9.0))
            self.logger.info('pages={pages}'.format(pages=self.pages))
            self.nonce = response.text[response.text.find('"api_nonce":')+13:response.text.find('"map_marker"')-2]
            self.logger.info('nonce={nonce}'.format(nonce=self.nonce))
        except Exception as ex:
            self.logger.warning(ex)
        url = 'https://lbmexecutivesearch.com/wordpress/wp-admin/admin-ajax.php?qualification=&location=&contract=&publish_date=&orderby=date&page={page}&action=offres&nonce={nonce}'.format(page=self.page, nonce=self.nonce)
        if self.use_scrapestack:
            request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
        else:
            request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
        yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        try:
            data = json.loads(response.text)
            #pprint(data)
            content = data['content']
            content = content.replace('\\', '')
            #print(content)
        except Exception as ex:
            self.logger.error(ex)
        else:
            html = content
            body = html.encode('utf-8')
            response = response.replace(body=body)
            links = response.css('a[class="last-jobs__item job"]').xpath('@href').extract()
            self.logger.debug(links)
            self.logger.debug('!!!links count={count}'.format(count=len(links)))
            results_blocks = response.css('a[class="last-jobs__item job"]').extract()
            page_results = {'job_board': 'lbmexecutivesearch', 'job_board_url': 'https://lbmexecutivesearch.com', 'page_url': response.url,'offers': []}
            for i in range(0, len(links)):
                url = links[i]
                job = {'url': url}
                if len(results_blocks) > i:
                    job['html'] = results_blocks[i]
                    html = results_blocks[i]
                    body = html.encode('utf-8')
                    offer_block = response.replace(body=body)
                    job['title'] = ' '.join(offer_block.css('h3[class="job__title"]').xpath('text()').extract()).strip()
                    job['publish_date'] = ' '.join(offer_block.css('span[class="job__metas"]').xpath('text()').extract()).strip().split(' - ')[0]
                    #job['id'] = ' '.join(offer_block.css('span[class="job__metas"]').xpath('text()').extract()).strip().split(' - ')[1]
                    items = offer_block.css('span[class="job__info"]')
                    if len(items) > 0:
                        location = ' '.join(
                            offer_block.css('span[class="job__info"]')[0].xpath('text()').extract()).strip()
                        job['location'] = self.clear_string(location)
                    if len(items) > 1:
                        contract_type = ' '.join(
                            offer_block.css('span[class="job__info"]')[1].xpath('text()').extract()).strip()
                        job['contract_type'] = self.clear_string(contract_type)
                    if len(items) > 2:
                        salary = ' '.join(offer_block.css('span[class="job__info"]')[2].xpath('text()').extract()).strip()
                        job['salary'] = self.clear_string(salary)
                page_results['offers'].append(job)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
                request.meta['job'] = job
                yield request
            if self.pages > self.page:
                self.page += 1
                url = 'https://lbmexecutivesearch.com/wordpress/wp-admin/admin-ajax.php?qualification=&location=&contract=&publish_date=&orderby=date&page={page}&action=offres&nonce={nonce}'.format(
                    page=self.page, nonce=self.nonce)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
                yield request
            if not self.drain:
                self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'lbmexecutivesearch')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        l.add_value('search_term', 'lbmexecutivesearch_jobboard')
        html_content = ' '.join(response.css('div[class="content-main__wrapper content-style"]').extract()).strip()
        content = html_content
        content = self.h.handle(content).strip()
        header = ' '.join(response.css('header[class="content-main__header"]').extract()).strip()
        description = ' '.join(response.xpath(u'//strong[contains(text(), "Descriptif du poste")]/parent::h2/following-sibling::p[1]/text()').extract())
        if len(description) > 0:
            l.add_value('description', description)
        description = ' '.join(response.xpath(u'//strong[contains(text(), "Descriptif du poste")]/parent::h2/following-sibling::ul[1]').extract())
        description = self.h.handle(description).strip()
        if len(description) > 0:
            l.add_value('description', description)
        profile = ' '.join(response.xpath(u'//strong[contains(text(), "Votre profil")]/parent::h2/following-sibling::ul').extract())
        profile = self.h.handle(profile).strip()
        if len(profile) > 0:
            l.add_value('profile', profile)
        mission = ' '.join(response.xpath(u'//strong[contains(text(), "Votre mission,")]/parent::h2/following-sibling::ul[1]').extract())
        mission = self.h.handle(mission).strip()
        if len(mission) > 0:
            l.add_value('description', mission)
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

    def clear_string(self, string):
        string = ' '.join(list(filter(lambda j: len(j) > 0, list(map(lambda i: i.strip(), string.split(' '))))))
        return string.replace(' ,', ',')









