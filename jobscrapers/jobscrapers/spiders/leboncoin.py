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
from jobscrapers.items import annotations_list
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

class LeboncoinSpider(scrapy.Spider):

    name = "leboncoin"
    handle_httpstatus_list = [403]
    publisher = "Leboncoin"
    publisherurl = 'https://www.leboncoin.fr'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/leboncoin.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/leboncoin2.lua')
    url_index = None
    dirname = 'leboncoin2'
    limit = False
    drain = False
    concrete_url = False
    rundebug = False
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    cities = []
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    order = [
        'name',
        'title',
        'salary',
        'company',
        'desc_l',
        'desc',
        'criteries',
        'contrat_type_l',
        'contrat_type',
        'category_l',
        'category',
        'jobduty_l',
        'jobduty',
        'experience_duration_l',
        'experience_duration',
        'education_l',
        'education',
        'position_scheduled_l',
        'position_scheduled',
        'location_l',
        'location'
    ]
    
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
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        self.headers=self.getBasicAuthHeader('user', 'userpass')

    def start_requests(self):
        allowed_domains = ["https://www.leboncoin.fr"]
        count_job = 10069
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro&jobfield=2&i'
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
        count_job = int(response.css('label[for="result_pro"] span span').xpath('text()').extract()[0].replace(' ', ''))
        self.logger.info('COUNT JOB: {count_job}'.format(count_job=count_job))
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro&jobfield=2'
        if self.limit:
            max_page = int(math.ceil(self.limit / 40.0))
        for page in range(1, max_page + 1):
            if page > 1:
                url = 'https://www.leboncoin.fr/recherche/?category=33&owner_type=pro&jobfield=2&page={page}'.format(page=page)
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
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'proxy': 'socks5://tor:9050'})
            request.meta['name'] = uri[:-1].split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=LeboncoinItem())
        l.add_value('name', response.meta['name'])
        try:
            data = json.loads(response.text)
        except Exception, ex:
            self.logger.error(response.text)
            raise Exception(ex)
        if 'title' in data and data['title'] is not None:
            l.add_value('title', data['title'])
            l.add_value('title', 'position')
        if 'salary' in data and data['salary'] is not None:
            l.add_value('salary', data['salary'])
            l.add_value('salary', 'salary')
        if 'company' in data and data['company'] is not None:
            l.add_value('company', data['company'])
            l.add_value('company', 'company')

        if 'desc' in data and data['desc'] is not None:
            l.add_value('desc_l', 'Description')
            l.add_value('desc_l', 'O')
            l.add_value('desc', data['desc'].replace('<br>', ' ').replace('&nbsp;', ''))
            l.add_value('desc', 'O')
        l.add_value('criteries', u'Critères')
        l.add_value('criteries', 'O')
        if 'contract_type' in data and data['contract_type'] is not None:
            l.add_value('contrat_type', data['contract_type'])
            l.add_value('contrat_type', 'contrat_type')
            l.add_value('contrat_type_l', u'TYPE DE CONTRAT')
            l.add_value('contrat_type_l', 'O')
        if 'category' in data and data['category'] is not None:
            l.add_value('category', data['category'])
            l.add_value('category', 'O')
            l.add_value('category_l', u"SECTEUR D'ACTIVITÉ")
            l.add_value('category_l', 'O')
        if 'jobduty' in data and data['jobduty'] is not None:
            l.add_value('jobduty', data['jobduty'])
            l.add_value('jobduty', 'experience')
            l.add_value('jobduty_l', u"FONCTION")
            l.add_value('jobduty_l', 'O')
        if 'experience' in data and data['experience'] is not None:
            l.add_value('experience_duration', data['experience'])
            l.add_value('experience_duration', 'experience_duration')
            l.add_value('experience_duration_l', u"EXPÉRIENCE")
            l.add_value('experience_duration_l', 'O')
        if 'education' in data and data['education'] is not None:
            l.add_value('education', data['education'])
            l.add_value('education', 'education')
            l.add_value('education_l', u"NIVEAU D'ÉTUDES")
            l.add_value('education_l', 'O')
        if 'job_type' in data and data['job_type'] is not None:
            l.add_value('position_scheduled', data['job_type'])
            l.add_value('position_scheduled', 'position_scheduled')
            l.add_value('position_scheduled_l', u"NTRAVAIL À")
            l.add_value('position_scheduled_l', 'O')
        if 'location' in data and data['location'] is not None:
            l.add_value('location', data['location'])
            l.add_value('location', 'city')
            l.add_value('location_l', u"Localisation")
            l.add_value('location_l', 'O')
        yield l.load_item()



    def getBasicAuthHeader(self, username, password):
        userAndPass = b64encode(b'%s:%s' % (username, password)).decode('ascii')
        headers = {'Authorization': 'Basic %s' % userAndPass}
        return headers

    def debug(self, response):
        #pprint(response.css('div.ip span + big').xpath('text()').extract()[0])
        #count_job = int(response.css('label[for="result_pro"] span span').xpath('text()').extract()[0].replace(' ', ''))
        #self.logger.info('COUNT JOB: {count_job}'.format(count_job=count_job))
        pprint(response.text)
        #self.parse_job(response)


