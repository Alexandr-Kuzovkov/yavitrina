# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import SpieItem
from jobscrapers.items import annotations_list
import time
import pkgutil
from scrapy_splash import SplashRequest
import math
import re
from jobscrapers.extensions import Geocode
import os
from jobscrapers.pipelines import clear_folder
from jobscrapers.items import categories2

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class IndeedJobsSpider(scrapy.Spider):

    name = "indeed-jobs"
    publisher = "Indeed"
    publisherurl = 'https://www.indeed.fr'
    url_index = None
    dirname = 'indeed-jobs'
    limit = False
    drain = False
    rundebug = False
    min_len = 100
    industries = {}
    industries2 = {}


    def __init__(self, limit=False, drain=False, debug=False, dirname=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if dirname:
            self.dirname = dirname

    def start_requests(self):
        allowed_domains = ["https://www.indeed.fr"]
        urls = []
        for category in categories2:
            urls.append('https://www.indeed.fr/emplois?l=France&{query}'.format(query=urllib.urlencode({'q': category.encode('utf-8')})))
            urls.append('https://www.indeed.fr/emplois?l=France&{query}'.format(query=urllib.urlencode({'q': category.encode('utf-8'), 'start': 1000})))
        self.logger.info('URLs: %i' % len(urls))
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://www.jobintree.com/emploi'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'country': self.country}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.debug, endpoint='execute', args=splash_args)
            yield request
        else:
            for url in urls:
                request = scrapy.Request(url, callback=self.get_jobs_list)
                parsed = urlparse.urlparse(url)
                request.meta['industry'] = '-'.join(urlparse.parse_qs(parsed.query)['q'])
                yield request

    def get_jobs_list(self, response):
        #pprint(response.url)
        self.count_industry(self.industries2, response.meta['industry'])
        job_uris = response.css('div.title a[data-tn-element="jobTitle"]').xpath('@href').extract()
        #pprint(uris)
        for job_uri in job_uris:
            if 'vjs=3' not in job_uri:
                continue
            if '/rc/clk' in job_uri:
                url = ''.join(['https://www.indeed.fr', '/voir-emploi', job_uri.replace('/rc/clk', '')])
            else:
                url = ''.join(['https://www.indeed.fr', job_uri])
            request = scrapy.Request(url, callback=self.parse_job)
            parsed = urlparse.urlparse(url)
            try:
                request.meta['industry'] = response.meta['industry']
                request.meta['name'] = '-'.join(urlparse.parse_qs(parsed.query)['jk'])
            except KeyError:
                pass
            yield request
        if 'start' not in response.url or 'start=1000' in response.url:
            url = ''.join(['https://www.indeed.fr'] + response.css('div[class="pagination"] a')[0:1].xpath('@href').extract())
            parsed = urlparse.urlparse(url)
            try:
                start = '-'.join(urlparse.parse_qs(parsed.query)['start'])
                category = response.meta['industry']
                url = 'https://www.indeed.fr/emplois?l=France&{query}'.format(query=urllib.urlencode({'q': category, 'start': start}))
                request = scrapy.Request(url, callback=self.get_jobs_list)
                request.meta['industry'] = response.meta['industry']
                yield request
            except Exception:
                pass

    def parse_job(self, response):
        l = ItemLoader(item=SpieItem())
        if 'name' in response.meta:
            l.add_value('name', response.meta['name'])
        l.add_value('industry', response.meta['industry'])
        self.count_industry(self.industries, response.meta['industry'])
        text = []
        text.append(u' '.join(response.css('div[class="jobsearch-JobComponent icl-u-xs-mt--sm"]').extract()).strip())
        l.add_value('text', u' '.join(text))
        yield l.load_item()

    def count_industry(self, industries, industry):
        if industry in industries:
            industries[industry] += 1
        else:
            industries[industry] = 1

    def debug(self, response):
        data = json.loads(response.text)
        pprint(data)



