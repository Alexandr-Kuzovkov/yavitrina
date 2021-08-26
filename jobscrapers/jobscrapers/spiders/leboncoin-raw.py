# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import LeboncoinItem
import time
import pkgutil
from scrapy_splash import SplashRequest
import scrapy_splash
import math
import re
from base64 import b64encode

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class LeboncoinRawSpider(scrapy.Spider):

    name = "leboncoin-raw"
    handle_httpstatus_list = [403]
    publisher = "Leboncoin"
    publisherurl = 'https://www.leboncoin.fr'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/leboncoin.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/leboncoin2.lua')
    url_index = None
    dirname = 'leboncoin-raw'
    limit = False
    drain = False
    concrete_url = False
    rundebug = False
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    min_len = 100
    
    def __init__(self, limit=False, drain=False, debug=False, concrete_url=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if concrete_url:
            self.concrete_url = concrete_url
        self.headers=self.getBasicAuthHeader('user', 'userpass')

    def start_requests(self):
        allowed_domains = ["https://www.leboncoin.fr"]
        count_job = 10069
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro&i'
        if self.rundebug:
            self.logger.info('Debug!!!')
            #url = 'https://2ip.ru/'
            #url = 'https://www.leboncoin.fr/offres_d_emploi/1564193942.htm/'
            #yield SplashRequest(url, self.debug, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})
            #yield SplashRequest(url, self.debug, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            yield SplashRequest(url, self.debug, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2, 'timeout': 3600})
        elif self.concrete_url:
            request = SplashRequest(self.concrete_url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600}, slot_policy=scrapy_splash.SlotPolicy.SINGLE_SLOT)
            request.meta['name'] = self.concrete_url[:-1].split('/').pop()
            yield request
        else:
            #request = scrapy.Request(url, callback=self.get_count_jobs)
            #headers = request.headers
            #headers['User-Agent'] = self.user_agent
            #request = request.replace(headers=headers)
            request = SplashRequest(url, self.get_count_jobs, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            yield request

    def get_count_jobs(self, response):
        #pprint(response.text)
        count_job = int(response.css('label[for="result_pro"] span span').xpath('text()').extract()[0].replace(' ', ''))
        self.logger.info('COUNT JOB: {count_job}'.format(count_job=count_job))
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro'
        if self.limit:
            max_page = int(math.ceil(self.limit / 40.0))
        for page in range(1, max_page + 1):
            if page > 1:
                url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro&page={page}'.format(page=page)
            #request = scrapy.Request(url, callback=self.get_jobs_list)
            #headers = request.headers
            #headers['User-Agent'] = self.user_agent
            #request = request.replace(headers=headers)
            request = SplashRequest(url, self.get_jobs_list, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            yield request

    def get_jobs_list(self, response):
        uris = response.css('a[class="clearfix trackable"]').xpath('@href').extract()
        #pprint(uris)
        for uri in uris:
            url = ''.join(['https://www.leboncoin.fr', uri])
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2, 'timeout': 3600})
            request.meta['name'] = uri[:-1].split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=LeboncoinItem())
        l.add_value('name', response.meta['name'])
        text = []
        text.append(u' '.join(response.css('div[data-qa-id="adview_spotlight_description_container"]').extract()).strip())
        text.append(u'Description')
        text.append(u' '.join(response.css('div[data-qa-id="adview_description_container"]').extract()).strip().replace(u'Signaler un abus', ''))
        text.append(u'Critères')
        text.append(u' '.join(response.css('div[data-qa-id="criteria_container"]').extract()).strip())
        text.append(u'Localisation')
        text.append(u' '.join(response.css('div[data-qa-id="adview_location_informations"] span').extract()).strip())
        l.add_value('text', u' '.join(text))

        yield l.load_item()



    def getBasicAuthHeader(self, username, password):
        userAndPass = b64encode(b'%s:%s' % (username, password)).decode('ascii')
        headers = {'Authorization': 'Basic %s' % userAndPass}
        return headers

    def debug(self, response):
        #pprint(response.css('div.ip span + big').xpath('text()').extract()[0])
        count_job = int(response.css('label[for="result_pro"] span span').xpath('text()').extract()[0].replace(' ', ''))
        self.logger.info('COUNT JOB: {count_job}'.format(count_job=count_job))
        pprint(response.text)
        #self.parse_job(response)


