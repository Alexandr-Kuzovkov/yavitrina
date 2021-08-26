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

class SpieSpider(scrapy.Spider):

    name = "spie"
    publisher = "Spie"
    publisherurl = 'https://www.join.spie-job.com'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/spie.lua')
    url_index = None
    dirname = 'spie'
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
        'index',
        'info',
        'company',
        'ref',
        'desc1',
        'title2_l',
        'title2',
        'contrat_type_l',
        'contrat_type',
        'position_scheduled_l',
        'position_scheduled',
        'contract_duration_l',
        'contract_duration',
        'category_l',
        'category',
        'mission_l',
        'mission',
        'experience_l',
        'experience',
        'location',
        'city_l',
        'city',
        'app_crit',
        'experience_duration_l',
        'experience_duration',
        'education_l',
        'education',
        'hard_skills_l',
        'hard_skills',
        'hard_skills2_l',
        'hard_skills2',
        'langs_l',
        'langs',
        'langs2_l',
        'langs2',
        'ambition_l',
        'ambition'
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
        elif self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
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
        # title#
        title = response.css('div#ctl00_ctl00_titreRoot_titre_titlePlaceHolder h1.ts-offer-page__title span')[0].xpath('text()').extract()[0].strip()
        l.add_value('title', title)
        l.add_value('title', 'position')
        #index = response.css('div#indexcourant').xpath('text()').extract()[0].strip()
        #l.add_value('index', index)
        #l.add_value('index', 'O')
        info = ' '.join(response.css('div#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_divEntityDesc').xpath('text()').extract()).strip()
        l.add_value('info', info)
        l.add_value('info', 'O')
        company = response.css('img#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_Logo').xpath('@title').extract()[0].replace('(logo)', '').strip()
        l.add_value('company', company)
        l.add_value('company', 'company')
        ref = ' '.join([
            response.css('div#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_divOfferReference h3').xpath('text()').extract()[0].strip(),
            response.css('div#ctl00_ctl00_corpsRoot_corps_composantDetailOffre_divOfferReference').xpath('text()').extract()[1].strip()
            ])
        l.add_value('ref', ref)
        l.add_value('ref', 'O')
        l.add_value('desc1', response.css('h2.JobDescription').xpath('text()').extract()[0])
        l.add_value('desc1', 'O')
        try:
            l.add_value('title2', response.css('p#fldjobdescription_jobtitle').xpath('text()').extract()[0])
            l.add_value('title2', 'position')
            l.add_value('title2_l', self.get_header_for_id(response, 'fldjobdescription_jobtitle'))
            l.add_value('title2_l', 'O')
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
            l.add_value('position_scheduled', response.css('p#fldjobdescription_jobtime').xpath('text()').extract()[0])
            l.add_value('position_scheduled', 'position_scheduled')
            l.add_value('position_scheduled_l', self.get_header_for_id(response, 'fldjobdescription_jobtime'))
            l.add_value('position_scheduled_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('contract_duration', response.css('p#fldjobdescription_contractlength').xpath('text()').extract()[0])
            l.add_value('contract_duration', 'contract_duration')
            l.add_value('contract_duration_l', self.get_header_for_id(response, 'fldjobdescription_contractlength'))
            l.add_value('contract_duration_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('category', response.css('p#fldjobdescription_professionalcategory').xpath('text()').extract()[0])
            l.add_value('category', 'O')
            l.add_value('category_l', self.get_header_for_id(response, 'fldjobdescription_professionalcategory'))
            l.add_value('category_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('mission', ' '.join(response.css('p#fldjobdescription_description1').xpath('text()').extract()))
            if 'France' in response.meta['industry']:
                l.add_value('mission', 'mission')
            else:
                l.add_value('mission', 'O')
            l.add_value('mission_l', self.get_header_for_id(response, 'fldjobdescription_description1'))
            l.add_value('mission_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('experience', ' '.join(response.css('p#fldjobdescription_description2').xpath('text()').extract()))
            if 'France' in response.meta['industry']:
                l.add_value('experience', 'experience')
            else:
                l.add_value('experience', 'O')
            l.add_value('experience_l', self.get_header_for_id(response, 'fldjobdescription_description2'))
            l.add_value('experience_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('location', response.css('h2.Location').xpath('text()').extract()[0])
            l.add_value('location', 'O')
        except Exception:
            pass

        try:
            l.add_value('city', response.css('p#fldlocation_joblocation + div').xpath('text()').extract()[0].strip())
            l.add_value('city', 'city')
            l.add_value('city_l', self.get_header_for_id(response, 'fldlocation_joblocation'))
            l.add_value('city_l', 'O')
        except Exception:
            pass


        l.add_value('app_crit', response.css('h2.ApplicantCriteria').xpath('text()').extract()[0].strip())
        l.add_value('app_crit', 'O')

        try:
            l.add_value('qualifi', response.css('p#fldapplicantcriteria_treenode_diploma').xpath('text()').extract()[0])
            l.add_value('qualifi', 'O')
            l.add_value('qualifi_l', self.get_header_for_id(response, 'fldapplicantcriteria_treenode_diploma'))
            l.add_value('qualifi_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('qualifi', response.css('p#fldapplicantcriteria_treenode_diploma').xpath('text()').extract()[0])
            l.add_value('qualifi', 'O')
            l.add_value('qualifi_l', self.get_header_for_id(response, 'fldapplicantcriteria_treenode_diploma'))
            l.add_value('qualifi_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('experience_duration', response.css('p#fldapplicantcriteria_experiencelevel').xpath('text()').extract()[0])
            if 'France' in response.meta['industry']:
                l.add_value('experience_duration', 'experience_duration')
            else:
                l.add_value('experience_duration', 'experience_duration')
            l.add_value('experience_duration_l', self.get_header_for_id(response, 'fldapplicantcriteria_experiencelevel'))
            l.add_value('experience_duration_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('education', response.css('p#fldapplicantcriteria_educationlevel').xpath('text()').extract()[0])
            l.add_value('education', 'education')
            l.add_value('education_l', self.get_header_for_id(response, 'fldapplicantcriteria_educationlevel'))
            l.add_value('education_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('hard_skills', ' '.join(response.css('p#fldapplicantcriteria_longtext1').xpath('text()').extract()))
            l.add_value('hard_skills', 'hard_skills')
            l.add_value('hard_skills_l', self.get_header_for_id(response, 'fldapplicantcriteria_longtext1'))
            l.add_value('hard_skills_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('hard_skills2', ' '.join(response.css('p#fldapplicantcriteria_longtext2').xpath('text()').extract()))
            l.add_value('hard_skills2', 'hard_skills')
            l.add_value('hard_skills2_l', self.get_header_for_id(response, 'fldapplicantcriteria_longtext2'))
            l.add_value('hard_skills2_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('langs', ' '.join(response.css('p#fldapplicantcriteria_requiredlanguagecollection').xpath('text()').extract()))
            l.add_value('langs', 'hard_skills')
            l.add_value('langs_l', self.get_header_for_id(response, 'fldapplicantcriteria_requiredlanguagecollection'))
            l.add_value('langs_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('langs2', ' '.join(response.css('p#fldapplicantcriteria_requiredlanguagecollection + ul li').xpath('text()').extract()))
            l.add_value('langs2', 'hard_skills')
            l.add_value('langs2_l', self.get_header_for_id(response, 'fldapplicantcriteria_requiredlanguagecollection'))
            l.add_value('langs2_l', 'O')
        except Exception:
            pass

        try:
            l.add_value('ambition', ' '.join(response.css('p#fldoffercustomblock1_longtext2').xpath('text()').extract()))
            l.add_value('ambition', 'O')
            l.add_value('ambition_l', self.get_header_for_id(response, 'fldoffercustomblock1_longtext2'))
            l.add_value('ambition_l', 'O')
        except Exception:
            pass
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


