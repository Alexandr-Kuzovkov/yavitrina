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

#The same as PublicEmployerSpider but without annotation
class PublicEmployer2Spider(scrapy.Spider):

    name = "public-employer2"
    publisher = "Emploi-Public"
    publisherurl = 'https://www.place-emploi-public.gouv.fr/'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/public-employer.lua')
    url_index = None
    dirname = 'public-employer2'
    limit = False
    drain = False
    rundebug = False
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    reannotate_only = False
    orders = {}
    
    def __init__(self, limit=False, drain=False, debug=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1 and j[0] == 'FR', map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))


    def start_requests(self):
        allowed_domains = ["https://www.place-emploi-public.gouv.fr/"]
        url = 'https://www.place-emploi-public.gouv.fr'
        #self.logger.info(str(self.cities))
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://www.place-emploi-public.gouv.fr/offre-emploi/3-agentses-polyvalentses-de-restauration-et-d-entretien-reference-O06919055109'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = url.split('/').pop()
            yield request
        elif self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            url = 'https://www.place-emploi-public.gouv.fr/'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args=splash_args)
            yield request

    def get_jobs_list(self, response):
        data = json.loads(response.text)
        links = data.values()
        self.logger.info('Fetched: {count} items'.format(count=len(links)))
        files_dir = self.settings.get('FILES_DIR', '')
        clear_folder(os.path.sep.join([files_dir, self.dirname]))
        #pprint(uris)
        for url in links:
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = url.split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SpieItem())
        name = response.meta['name']
        l.add_value('name', name)
        text = ' '.join(response.css('section[class="block-hero single-offer"] div[class="row"]').extract()) + ' '.join(response.css('section[class="single-offer-content single"]').extract())
        l.add_value('text', text)
        yield l.load_item()

    def cut_tags(self, text):
        allowed_tags = []
        all_tags_re = re.compile('<.*?>')
        all_tags = all_tags_re.findall(text)
        # pprint(all_tags)
        all_tags = map(lambda i: i.split(' ')[0].replace('<', '').replace('>', '').replace('/', ''), all_tags)
        # pprint(list(set(all_tags)))
        for tag in all_tags:
            if tag not in allowed_tags:
                if tag in ['table', 'tbody', 'thead', 'header', 'footer', 'nav', 'section', 'article', 'aside',
                           'address', 'figure', 'td', 'th', 'tr', 'img', 'div', 'br', 'strong', 'p', 'h2', 'h3',
                           'ul', 'li', 'span', 'b']:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), ' ', text)
        text = text.replace('&amp;', '&')
        return self.rm_spaces(text)

    def rm_spaces(self, text):
        while not text.find('  ') == -1:
            text = text.replace('  ', '')
        return text

    def debug(self, response):
        data = json.loads(response.text)
        pprint(data)

