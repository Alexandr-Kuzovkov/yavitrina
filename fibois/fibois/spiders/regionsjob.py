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

class RegionsjobSpider(scrapy.Spider):
    name = 'regionsjob'
    allowed_domains = []
    dirname = 'regionsjob'
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
    period = 'all'

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
            self.period='h'
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
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            page = self.pagination[keyword]['page']
            url = 'https://www.regionsjob.com/emploi/recherche?k={keyword}&d={period}&p={page}'.format(keyword=urllib.quote_plus(keyword), page=page, period=self.period)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join([' '.join(response.css('h1[class="side nocard autonome"] span').xpath('text()').extract()).strip(), ' '.join(response.css('h1[class="side nocard autonome"] span + span').xpath('text()').extract()).strip()])
        keyword = response.meta['keyword']
        self.logger.info(h1)
        links = response.css('section[class="content nocard autonome"] ul li div[class="offer--content"] h3 a').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('section[class="content nocard autonome"] ul li').extract()
        page_results = {'job_board': 'www.regionsjob.com', 'job_board_url': 'https://www.regionsjob.com', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://www.regionsjob.com{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('div[class="offer--content"] h3 a').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('div[class="offer--content"] span[class="entname"]').xpath('text()').extract()).strip()
                job['location'] = ' '.join(offer_block.css('div[class="infos"] span[class="loc "] span').xpath('text()').extract()).strip()
                job['contract_info'] = ' '.join(offer_block.css('div[class="infos"] span[class="contract"] span').xpath('text()').extract()).strip()
                job['salary'] = ' '.join(offer_block.css('div[class="infos"] span[class="salaire"] span').xpath('text()').extract())
                job['publish_date'] = ' '.join(offer_block.css('div[class="offer--content"] div[class="lastinfo"] span[class="time"]').xpath('text()').extract()).strip()
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        try:
            pagdata = json.loads(' '.join(response.css('div[id="pagin"]').xpath('@data-pagination').extract()))
            if pagdata[u'PageNumber'] < int(math.ceil(pagdata[ u'TotalCount'] / pagdata[u'PageSize'])):
                self.pagination[keyword]['page'] += 1
                page = self.pagination[keyword]['page']
                url = 'https://www.regionsjob.com/emploi/recherche?k={keyword}&d={period}&p={page}'.format(keyword=urllib.quote_plus(keyword), page=page, period=self.period)
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_list_page)
                request.meta['keyword'] = keyword
                yield request
        except Exception as ex:
            self.logger.info('pagination decode error: ' + str(ex))
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'www.regionsjob.com')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        section = response.css('main section')
        if len(section) > 0:
            content = ''.join(response.css('main section')[0].extract()).strip()
            #content = self.h.handle(content).encode('utf-8').strip()
            content = self.h.handle(content).strip()
            header = ' '.join([' '.join(response.css('header section h1').extract()), ' '.join(response.css('header h1 + ul').extract())])
            html_content = ''.join(response.css('main section')[0].extract()).strip()
            sector = ' '.join(response.css('main section')[0].xpath('//span[text()="Secteur de l\'entreprise : "]').css('span + span').xpath('text()').extract()).strip()
            experience_level = ' '.join(response.xpath(u"//span[text() = 'Expérience requise : ']/following-sibling::span[1]/text()").extract())
            education_level = ' '.join(response.xpath(u'//span[text() = "Niveau d\'études : "]/following-sibling::span[1]/text()').extract())
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









