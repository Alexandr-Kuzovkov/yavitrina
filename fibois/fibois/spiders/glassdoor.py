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

class GlassdoorSpider(scrapy.Spider):
    name = 'glassdoor'
    allowed_domains = []
    dirname = 'glassdoor.fr'
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
    debug = False
    start_time = None
    max_runtime = 25200 #7 hours

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, debug=False, noproxy=False, *args, **kwargs):
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
        if debug:
            self.debug = True
        if noproxy:
            self.use_scrapestack = False
        self.start_time = int(time.time())

    def start_requests(self):
        if self.debug:
            self.logger.warning('!!!!!!!!!DEBUG!!!!!!!!!')
            url = 'https://www.glassdoor.fr/job-listing/installateur-menuiseries-extérieures-pvc-alu-b-plast-coignieres-fasseau-menuiserie-JV_IC2944930_KO0,44_KE45,82.htm?jl=3813330081&pos=103&ao=1044077&s=58&guid=00000177ab37b2c8974ccbf301d481eb&src=GD_JOB_AD&t=SR&vt=w&ea=1&cs=1_684b81b0&cb=1613485290729&jobListingId=3813330081&ctt=1613485513461'
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page)
            job = {'url': url}
            request.meta['job'] = job
            yield request
        else:
            for keyword in self.keywords:
                self.pagination[keyword] = {'page': 1, 'pages': None}
            for keyword in self.keywords:
                url ='https://www.glassdoor.fr/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword={keyword}&sc.keyword={keyword}&locT=&locId=0&jobType='.format(keyword=urllib.quote_plus(keyword))
                if self.delta:
                    url = '{url}&fromAge=1'.format(url=url)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, dont_filter=True, access_key=self.scrapestack_access_key, options={'render_js': 1})
                else:
                    args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                    request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=True)
                request.meta['keyword'] = keyword
                yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.css('div[data-test="page-x-of-y"]').xpath('text()').extract()).strip()
        keyword = response.meta['keyword']
        self.logger.info(h1)
        try:
            count = int(h1.split(' ').pop().strip())
            self.logger.info('keyword: "{keyword}" - {count} pages found'.format(keyword=keyword, count=count))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        links = response.css('li.react-job-listing a[data-test="job-link"]').xpath('@href').extract()
        self.logger.debug(links)
        self.logger.info('{count} links found'.format(count=len(links)))
        if len(links) == 0 and self.check_runtime():
            self.logger.info('parse_job_list_page: repeat request to url "{url}"'.format(url=response.request.url))
            time.sleep(3)
            yield response.request
        else:
            results_block = response.css('li[data-test="jobListing"]').extract()
            page_results = {'job_board': 'glassdoor.fr', 'job_board_url': 'https://www.glassdoor.fr', 'page_url': response.url, 'offers': []}
            for i in range(0, len(links)):
                url = 'https://www.glassdoor.fr{link}'.format(link=links[i])
                job = {'url': url}
                job['search_term'] = unicode(response.meta['keyword'], 'utf8')
                job['keyword_type'] = self.keywords_type
                if len(results_block) > i:
                    job['html'] = results_block[i]
                    html = results_block[i]
                    body = html.encode('utf-8')
                    offer_block = response.replace(body=body)
                    job['title'] = ' '.join(offer_block.css('a[data-test="job-link"] span').xpath('text()').extract())
                    job['company_name'] = ' '.join(offer_block.css('div[class="d-flex justify-content-between align-items-start"] a span').xpath('text()').extract())
                    job['location'] = ' '.join(offer_block.css('a[data-test="job-link"] + div span').xpath('text()').extract())
                    job['publish_date'] = ' '.join(offer_block.css('div[data-test="job-age"]').xpath('text()').extract())
                    page_results['offers'].append(job)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
                else:
                    args = {'wait': 20.0, 'lua_source': self.lua_src, 'timeout': 3600}
                    request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=True)
                request.meta['job'] = job
                yield request
            try:
                pagdata = map(lambda i: int(i), filter(lambda i: i.strip().isdigit(),  h1.split(' ')))
                if len(pagdata) > 1 and pagdata[0] < pagdata[1]:
                    self.pagination[keyword]['page'] += 1
                    page = self.pagination[keyword]['page']
                    url = 'https://www.glassdoor.fr/Job/jobs.htm?suggestCount=0&suggestChosen=false&clickSource=searchBtn&typedKeyword={keyword}&sc.keyword={keyword}&locT=&locId=0&jobType=&p={page}'.format(keyword=urllib.quote_plus(keyword), page=page)
                    self.logger.info('fetch page={page} for keyword="{keyword}"'.format(page=self.pagination[keyword]['page'], keyword=keyword))
                    if self.delta:
                        url = '{url}&fromAge=1'.format(url=url)
                    if self.use_scrapestack:
                        request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1}, dont_filter=True)
                    else:
                        args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                        request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=True)
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
        l.add_value('jobboard', 'glassdoor.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('div[class="d-flex"] > div.d-flex').extract())
        html_content = ' '.join(response.css('div[id="JobDescriptionContainer"]').extract())
        content = header + '<br>' + html_content
        content = self.h.handle(content).strip()
        if len(html_content) == 0 and self.check_runtime():
            self.logger.info('parse_job_detail_page: repeat request to url "{url}"'.format(url=response.request.url))
            time.sleep(3)
            yield response.request
        else:
            salary = ' '.join(response.xpath(u'//p[contains(text(), "Salaire :")]/text()').extract())
            experience_level = ' '.join(response.xpath(u'//p[contains(text(), "Expérience :")]/following-sibling::ul[1]').extract())
            education_level = ' '.join(response.xpath(u'//p[contains(text(), "Formation :")]/following-sibling::ul[1]').extract())
            l.add_value('content', content)
            l.add_value('source', 'scrapy')
            l.add_value('header', header)
            l.add_value('html_content', html_content)
            if len(salary) > 0:
                l.add_value('salary', salary)
            if len(experience_level) > 0:
                l.add_value('experience_level', self.h.handle(experience_level).strip())
            if len(education_level) > 0:
                l.add_value('education_level', self.h.handle(education_level).strip())
            yield l.load_item()
            job_details = {'html': html_content, 'title': header, 'url': job['url']}
            if not self.drain:
                self.es_exporter.insert_job_details_html(job_details)

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date

    def check_runtime(self):
        now = int(time.time())
        if now - self.start_time < self.max_runtime:
            return True
        return False










