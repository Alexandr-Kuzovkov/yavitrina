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

class IndeedSpider(scrapy.Spider):
    name = 'indeed.fr'
    allowed_domains = []
    dirname = 'indeed.fr'
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
            url = 'https://fr.indeed.com/jobs?q={keyword}'.format(keyword=urllib.quote_plus(keyword))
            if self.delta:
                url = '{url}&fromage=1'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.css('div[id="searchCountPages"]').xpath('text()').extract()).strip()
        keyword = response.meta['keyword']
        self.logger.info(h1)
        count = 0
        try:
            count = int( h1.split(' ')[-2:-1][0].encode('ascii','ignore'))
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/15.0))
            self.logger.info('{pages} pages for keyword={keyword}'.format(pages=self.pagination[keyword]['pages'], keyword=keyword))
        links = response.css('div[id="mosaic-provider-jobcards"] > a[target="_blank"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('div.job_seen_beacon').extract()
        page_results = {'job_board': 'indeed.fr', 'job_board_url': 'https://fr.indeed.com', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = ''.join(['https://www.indeed.fr', '/voir-emploi', links[i].replace('/rc/clk', '')])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h2.jobTitle > span').xpath('text()').extract()).strip()
                job['location'] = ' '.join(offer_block.css('div[class="companyLocation"]').xpath('text()').extract())
                job['publish_date'] = ' '.join(offer_block.css('span[class="date"]').xpath('text()').extract())
                job['salary'] = ' '.join(offer_block.css('span[class="salary-snippet"]').xpath('text()').extract())
                page_results['offers'].append(job)
                dates.append(job['publish_date'])
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        next_link = ' '.join(response.css('ul[class="pagination-list"] li a[aria-label="Suivant"]').xpath('@href').extract())
        if len(next_link) > 0:
            self.pagination[keyword]['page'] += 1
            page = self.pagination[keyword]['page']
            url = 'https://fr.indeed.com{next_link}'.format(next_link=next_link)
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
        l.add_value('jobboard', 'indeed.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('div[id="viewJobSSRRoot"] div[class="jobsearch-DesktopStickyContainer"]').extract()).strip()
        html_content = ' '.join(response.css('div.jobsearch-JobComponent-description').extract()).strip()
        # content = self.h.handle(content).encode('utf-8').strip()
        content = self.h.handle(' '.join(response.css('div[id="viewJobSSRRoot"]').extract()).strip()).strip()
        experience_level = ' '.join(response.css('div.jobsearch-JobComponent-description').xpath(u'//b[text()="Expérience: "]/parent::li/text()').extract())
        education_level = ' '.join(response.css('div.jobsearch-JobComponent-description').xpath(u'//b[text()="Formation: "]/parent::li/text()').extract())
        contract_type = ' '.join(response.css('div.jobsearch-JobComponent-description').xpath(u'//b[normalize-space(text()) = "Type de contrat"]/parent::div/following-sibling::div[1]/text()').extract()).strip()
        company_name = ' '.join(response.css('div.jobsearch-InlineCompanyRating > div > a').xpath('text()').extract())
        if len(company_name) == 0:
            company_name = ' '.join(response.css('div.jobsearch-InlineCompanyRating > div').xpath('text()').extract())
        if len(experience_level) > 0:
            l.add_value('experience_level', experience_level)
        if len(education_level) > 0:
            l.add_value('education_level', education_level)
        if len(contract_type) > 0:
            l.add_value('contract_type', contract_type)
        if len(company_name) > 0:
            l.add_value('company_name', company_name)
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










