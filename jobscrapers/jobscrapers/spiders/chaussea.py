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

class ChausseaSpider(scrapy.Spider):

    name = "chaussea"
    publisher = "Chaussea"
    publisherurl = 'https://jobs.chaussea.com/'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/chaussea.lua')
    url_index = None
    dirname = 'chaussea'
    limit = False
    drain = False
    rundebug = False
    country = 31 #index of country in select option
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    reannotate_only = False
    order = [
        'name',
        'title',
        'title2',
        'title3',
        'gen_info',
        'index',
        'info_l',
        'info',
        'desc_header',
        'title4_l',
        'title4',
        'title5_l',
        'title5',
        'contrat_type_l',
        'contrat_type',
        'desc1_l',
        'desc1',
        'desc2_l',
        'desc2',
        'location_l',
        'location',
        'city_l',
        'city'
    ]
    
    def __init__(self, limit=False, drain=False, country=31, debug=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.country = country
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        if ra:
            self.reannotate_only = True

    def start_requests(self):
        allowed_domains = ["https://jobs.chaussea.com/"]
        url = 'https://jobs.chaussea.com/offre-de-emploi/liste-offres.aspx'
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://jobs.chaussea.com/offre-de-emploi/liste-offres.aspx'
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'country': self.country}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.debug, endpoint='execute', args=splash_args)
            yield request
        elif self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            url = 'https://jobs.chaussea.com/offre-de-emploi/liste-offres.aspx'
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
            url = ''.join(['https://jobs.chaussea.com', uri])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = uri.split('/').pop()
            request.meta['industry'] = criterie
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SpieItem())
        l.add_value('name', response.meta['name'])
        l.add_value('industry', response.meta['industry'])
        # title#
        title = response.css('div#ctl00_ctl00_titreRoot_titre_titlePlaceHolder h1.ts-offer-page__title span')[0].xpath('text()').extract()[0].strip()
        title1 = title.split('-')[0].strip()
        l.add_value('title', title1)
        l.add_value('title', 'position')
        try:
            title2 = title.split('-')[1].strip()
            l.add_value('title2', title2)
            l.add_value('title2', 'O')
        except Exception:
            pass

        try:
            l.add_value('title3', response.css('h1[class="ts-title ts-title--secondary"]').xpath('text()').extract()[0].strip())
            l.add_value('title3', 'O')
        except Exception:
            pass

        try:
            l.add_value('gen_info', response.css('div[id="contenu-ficheoffre"] h2')[0].xpath('text()').extract()[0].strip())
            l.add_value('gen_info', 'O')
        except Exception:
            pass

        info_l = response.css('div#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_divEntityDesc h3').xpath('text()').extract()[0].strip()
        l.add_value('info_l', info_l)
        l.add_value('info_l', 'O')
        info = ' '.join(response.css('div#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_divEntityDesc').xpath('text()').extract()).strip()
        l.add_value('info', info)
        l.add_value('info', 'O')

        try:
            l.add_value('desc_header', response.css('div[id="contenu-ficheoffre"] h2[class="JobDescription"]')[0].xpath('text()').extract()[0].strip())
            l.add_value('desc_header', 'O')
        except Exception:
            pass

        try:
            l.add_value('title4', response.css('p#fldjobdescription_primaryprofile').xpath('text()').extract()[0])
            l.add_value('title4', 'experience')
            l.add_value('title4_l', self.get_header_for_id(response, 'fldjobdescription_primaryprofile'))
            l.add_value('title4_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('title5', response.css('p#fldjobdescription_jobtitle').xpath('text()').extract()[0])
            l.add_value('title5', 'position')
            l.add_value('title5_l', self.get_header_for_id(response, 'fldjobdescription_jobtitle'))
            l.add_value('title5_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('contrat_type', response.css('p#fldjobdescription_contract').xpath('text()').extract()[0])
            l.add_value('contrat_type', 'contrat_type')
            l.add_value('contrat_type_l', self.get_header_for_id(response, 'fldjobdescription_contract'))
            l.add_value('contrat_type_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('desc1', ' '.join(response.css('p#fldjobdescription_description1').xpath('text()').extract()))
            l.add_value('desc1', 'O')
            l.add_value('desc1_l', self.get_header_for_id(response, 'fldjobdescription_description1'))
            l.add_value('desc1_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('desc2', ' '.join(response.css('p#fldjobdescription_description2').xpath('text()').extract()))
            l.add_value('desc2', 'O')
            l.add_value('desc2_l', self.get_header_for_id(response, 'fldjobdescription_description2'))
            l.add_value('desc2_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('local_header', response.css('div[id="contenu-ficheoffre"] h2[class="Location"]')[0].xpath('text()').extract()[0].strip())
            l.add_value('local_header', 'O')
        except Exception:
            pass

        try:
            l.add_value('location', response.css('p#fldlocation_location_geographicalareacollection').xpath('text()').extract()[0])
            l.add_value('location', 'O')
            l.add_value('location_l', self.get_header_for_id(response, 'fldlocation_location_geographicalareacollection'))
            l.add_value('location_l', 'O')
        except Exception:
            pass

        #try:
        l.add_value('city', response.css('p#fldlocation_joblocation + div').xpath('text()').extract()[0].strip())
        l.add_value('city', 'city')
        l.add_value('city_l', self.get_header_for_id(response, 'fldlocation_joblocation'))
        l.add_value('city_l', 'O')
        #except Exception:
        #    pass

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

    def reannotate(self, response):
        self.logger.info('RUN REANNOTATE ONLY!!!')
        files_dir = self.settings.get('FILES_DIR', '')
        source_dir = os.path.sep.join([files_dir, self.dirname, 'src', self.reannotate_only])
        self.logger.info('Source dir: "%s"' % source_dir)
        list_of_files = os.listdir(source_dir)
        self.logger.info('%i files will reannotate' % len(list_of_files))
        for name in list_of_files:
            with open(os.path.sep.join([source_dir, name]), 'r') as fi:
                item = json.load(fi, 'utf-8')
                item['name'] = [name[:-4]]
                yield item


