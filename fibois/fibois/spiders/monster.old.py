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

class MonsterSpiderOld(scrapy.Spider):
    name = 'monster.fr.old'
    allowed_domains = []
    dirname = 'monster.fr.old'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
    #keywords = occupations[4:5]
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

    def start_requests(self):
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            page = self.pagination[keyword]['page']
            url = 'https://www.monster.fr/emploi/recherche/?q={keyword}&cy=fr'.format(keyword=urllib.quote_plus(keyword))
            if self.delta:
                url = '{url}&tm=0'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(
            response.css('header[class="title"] h1[class="pivot "]').xpath('text()').extract() +
            response.css('header[class="title"] h2[class="figure"]').xpath('text()').extract()
        ).strip().replace('\n', '')
        keyword = response.meta['keyword']
        self.logger.info(h1)
        count = 0
        try:
            count = int(''.join(c for c in h1 if c.isdigit()))
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
            if self.pagination[keyword]['pages'] is None:
                self.pagination[keyword]['pages'] = int(math.ceil(count/12.0))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        links = response.css('section[class="card-content "] header[class="card-header"] h2[class="title"] a').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('section[class="card-content "]').extract()
        page_results = {'job_board': 'monster.fr', 'job_board_url': 'https://www.monster.fr', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = links[i].encode('utf-8')
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('header[class="card-header"] h2[class="title"] a').xpath('text()').extract()).strip()
                job['company_name'] = ' '.join(offer_block.css('div[class="company"] a').xpath('text()').extract()).strip()
                job['location'] = ' '.join(offer_block.css('div[class="location"] span').xpath('text()').extract()).strip()
                job['publish_date'] = ' '.join(offer_block.css('time').xpath('text()').extract()).strip()
                page_results['offers'].append(job)
                dates.append(job['publish_date'])
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True, options={'render_js': 1})
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['job'] = job
            yield request
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages'] and count > 0:
            self.pagination[keyword]['page'] += 1
            page = self.pagination[keyword]['page']
            url = 'https://www.monster.fr/emploi/recherche/?q={keyword}&cy=fr&page={page}'.format(keyword=urllib.quote_plus(keyword), page=page)
            if self.delta:
                url = '{url}&tm=0'.format(url=url)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key, options={'render_js': 1}, dont_filter=True)
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=True)
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
        l.add_value('jobboard', 'monster.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('header[class="card-content job-caption"] div[class="heading"]').extract()).strip()
        html_content = ' '.join(response.css('div[id="JobPreview"]').extract()).strip()
        content = header + '<br>' + html_content
        content = self.h.handle(content).strip()
        salary = ' '.join(response.css('div[id="JobSalary"] header + div').xpath('text()').extract()).strip()
        location = ' '.join(response.css('aside div[id="JobSummary"]').xpath(u'//dt[text()="Lieu de poste"]/following-sibling::dd[1]/text()').extract())
        contract_type = ' '.join(response.css('aside div[id="JobSummary"]').xpath(u'//dt[text()="Type de contrat"]/following-sibling::dd[1]/text()').extract())
        publish_date = ' '.join(response.css('aside div[id="JobSummary"]').xpath(u'//dt[text()="Publiée"]/following-sibling::dd[1]/text()').extract())
        sector = ' '.join(response.css('aside div[id="JobSummary"]').xpath(u'//dt[text()="Secteur d\'activité"]/following-sibling::dd[1]/text()').extract())
        profile = ' '.join(response.css('div[class="text field-value-vac_profile"]').extract())
        profile = self.h.handle(profile).strip()
        if len(salary) > 0:
            l.add_value('salary', salary)
        if len(location) > 0:
            l.add_value('location', location)
        if len(contract_type) > 0:
            l.add_value('contract_type', contract_type)
        if len(publish_date) > 0:
            l.add_value('publish_date', publish_date)
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

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date









