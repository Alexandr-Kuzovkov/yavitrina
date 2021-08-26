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

class SpieRawSpider(scrapy.Spider):

    name = "spie-raw"
    publisher = "Spie"
    publisherurl = 'https://www.join.spie-job.com'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/spie.lua')
    url_index = None
    dirname = 'spie-raw'
    limit = False
    drain = False
    rundebug = False
    country = 33 #index of country in select option
    min_len = 100


    
    def __init__(self, limit=False, drain=False, country=33, debug=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.country = country


    def start_requests(self):
        allowed_domains = ["https://www.join.spie-job.com"]
        url = 'https://www.join.spie-job.com/offre-de-emploi/liste-offres.aspx?page=1&LCID=1036&v=123'
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://www.join.spie-job.com/offre-de-emploi/liste-offres.aspx'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'country': self.country}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.debug, endpoint='execute', args=splash_args)
            yield request
        else:
            url = 'https://www.join.spie-job.com/offre-de-emploi/liste-offres.aspx'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'country': self.country}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args=splash_args)
            yield request

    def get_jobs_list(self, response):
        data = json.loads(response.text)
        links = data[0]
        criterie = data[1]
        criterie = criterie.replace('/', '-')
        self.logger.info('Fetched: {count} items'.format(count=len(links)))
        uris = links.values()
        files_dir = self.settings.get('FILES_DIR', '')
        clear_folder(os.path.sep.join([files_dir, self.dirname, criterie]))
        #pprint(uris)
        for uri in uris:
            url = ''.join(['https://www.join.spie-job.com', uri])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = uri.split('/').pop()
            request.meta['industry'] = criterie
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SpieItem())
        l.add_value('name', response.meta['name'])
        l.add_value('industry', response.meta['industry'])
        text = []
        text.append(u' '.join(response.css('h1[class="ts-offer-page__title ts-title ts-title--primary"] span').extract()).strip())
        text.append(u' '.join(response.css('div[class="ts-offer-page__cta is-top liensbas-ficheoffre margintop"]').extract()).strip())
        text.append(u"Détail de l'offre")
        text.append(u' '.join(response.css('div[id="contenu-ficheoffre"]').extract()).strip())
        l.add_value('text', u' '.join(text))

        yield l.load_item()


    def get_header_for_id(self, response, id):
        header_text = False
        header_count = len(response.css('div#contenu2 h3 + p'))
        for index in range(0, header_count):
            curr_id = response.css('div#contenu2 h3 + p')[index].xpath('@id').extract()[0]
            if id == curr_id:
                try:
                    header_text = response.css('div#contenu2 h2.JobDescription ~ h3')[index].xpath('text()').extract()[0].strip()
                except Exception:
                    pass
        return header_text


    def debug(self, response):
        data = json.loads(response.text)
        pprint(data)



