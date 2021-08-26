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

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class JobintreeSpider(scrapy.Spider):

    name = "jobintree"
    publisher = "Jobintree"
    publisherurl = 'https://www.jobintree.com'
    url_index = None
    dirname = 'jobintree'
    limit = False
    drain = False
    rundebug = False
    min_len = 100

    def __init__(self, limit=False, drain=False, debug=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True


    def start_requests(self):
        allowed_domains = ["https://www.jobintree.com"]
        url = 'https://www.jobintree.com/emploi'
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://www.jobintree.com/emploi'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'country': self.country}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.debug, endpoint='execute', args=splash_args)
            yield request
        else:
            url = 'https://www.jobintree.com/emploi?v=1'
            request = scrapy.Request(url, callback=self.get_pages)
            yield request

    def get_pages(self, response):
        uris = response.css('div.pagination a.page').xpath('@href').extract()
        for uri in uris:
            url = ''.join(['https://www.jobintree.com', uri])
            request = scrapy.Request(url, callback=self.get_jobs_list)
            yield request

    def get_jobs_list(self, response):
        job_uris = response.css('a[class="title_lien_annonce"]').xpath('@href').extract()
        #pprint(uris)
        for job_uri in job_uris:
            url = ''.join(['https://www.jobintree.com', job_uri])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = job_uri.split('/').pop()
            yield request
        uris = response.css('div.pagination a.page').xpath('@href').extract()
        for uri in uris:
            url = ''.join(['https://www.jobintree.com', uri])
            request = scrapy.Request(url, callback=self.get_jobs_list)
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SpieItem())
        l.add_value('name', response.meta['name'])
        text = []
        text.append(u' '.join(response.css('div[class="bloc_general_left_content"] h1').extract()).strip())
        text.append(u' '.join(response.css('ul[class="container_enterprise_detail"]').extract()).strip())
        text.append(u"Postuler")
        text.append(u' '.join(response.css('div[class="annonce_content"]').extract()).strip())
        l.add_value('text', u' '.join(text))

        yield l.load_item()

    def debug(self, response):
        data = json.loads(response.text)
        pprint(data)



