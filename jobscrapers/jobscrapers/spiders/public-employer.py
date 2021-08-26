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

class PublicEmployerSpider(scrapy.Spider):

    name = "public-employer"
    publisher = "Emploi-Public"
    publisherurl = 'https://www.place-emploi-public.gouv.fr/'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/public-employer.lua')
    url_index = None
    dirname = 'public-employer'
    limit = False
    drain = False
    rundebug = False
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    reannotate_only = False
    orders = {}
    
    def __init__(self, limit=False, drain=False, country=31, debug=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.country = country
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1 and j[0] == 'FR', map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        if ra:
            self.reannotate_only = True

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
        self.add_element_by_selector(l, response, 'h1[class="single-title"]', 'title', 'position', name)
        location = ' '.join(response.css('span[class="single-info location"]').xpath('text()').extract())
        location1 = ','.join(location.split(',')[:-1])
        location2 = location.split(',').pop().strip()
        city = ' '.join(location2.split(' ')[:-1]).strip()
        number = ' '.join(location2.split(' ')[-1:]).strip()
        self.add_element(l, location1, 'location', 'O', name)
        if city.lower().replace(' ', '-') in self.cities or city.lower() in self.cities:
            self.add_element(l, city, 'city', 'city', name)
        else:
            self.add_element(l, city, 'city', 'O', name)
        self.add_element(l, number, 'number', 'O', name)
        self.add_element_by_selector(l, response, 'span[class="single-info organism"]', 'company', 'O', name)
        self.add_element_by_selector(l, response, 'div[class="block-offer-summary sticky"] h2[class="offer-title-section"]', 'title2', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Organisme de rattachement"]', 'title3', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Organisme de rattachement"]/following-sibling::p', 'company2', 'company', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Employeur"]', 'title3_1', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Employeur"]/following-sibling::p', 'company3', 'company', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Référence de l\'offre"]', 'title4', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Référence de l\'offre"]/following-sibling::p', 'ref', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Poste à pouvoir le"]', 'expired_l', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Poste à pouvoir le"]/following-sibling::p', 'expired', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Catégorie"]', 'category_l', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Catégorie"]/following-sibling::p', 'category', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Quotité de temps de travail"]', 'position_scheduled_l', 'O', name)
        self.add_element_by_xpath(l, response, u'//*[text()="Quotité de temps de travail"]/following-sibling::p', 'position_scheduled', 'position_scheduled', name)


        self.add_element(l, u'INFORMATIONS GÉNÉRALES', 'title5', 'O', name)
        self.add_element_by_selector_text(l, response, 'div[class="single-offer-content-part-columns"]', 'title6', 'O', name, option='first')
        self.add_element(l, u'DESCRIPTION DU POSTE', 'title7', 'O', name)
        text = ' '.join(response.css('div[class="single-offer-content-part"]').extract()).strip()
        text = self.cut_tags(text)
        education = ' '.join(response.xpath(u'//*[text()="Niveau d\'études minimum requis"]/following-sibling::p').xpath('text()').extract()).strip()
        if len(education) > 0:
            text = text.replace(education, '')
        self.add_element(l, text, 'desc1', 'O', name)
        if len(education) > 0:
            self.add_element(l, education, 'education', 'education', name)
        try:
            skill_l = ' '.join(response.css('div[class="single-offer-content-part-columns"]')[1].css('div[class="col-md-12"] h3[class="offer-subtitle-section"]').xpath('text()').extract()).strip()
            self.add_element(l, skill_l, 'skill_l', 'O', name)
            skills = ' '.join(response.css('div[class="single-offer-content-part-columns"]')[1].css('div[class="col-md-6"]').extract()).strip()
            skills = self.cut_tags(skills)
            self.add_element(l, skills, 'skills', 'hard_skills', name)
        except Exception:
            pass
        self.add_element_by_selector_text(l, response, 'div[class="block-highlight-content"]', 'text1', 'O', name)
        title8 = ' '.join(response.xpath(u'//*[text()="Localisation du poste"]/text()').extract())
        self.add_element(l, title8, 'title8', 'O', name)
        location = ' '.join(response.xpath(u'//*[text()="Localisation du poste"]/following-sibling::p/text()').extract())
        location1 = ','.join(location.split(',')[:-1])
        location2 = location.split(',').pop().strip()
        city = ' '.join(location2.split(' ')[:-1]).strip()
        number = ' '.join(location2.split(' ')[-1:]).strip()
        self.add_element(l, location1, 'location2', 'O', name)
        if city.lower().replace(' ', '-') in self.cities or city.lower() in self.cities:
            self.add_element(l, city, 'city2', 'city', name)
        else:
            self.add_element(l, city, 'city2', 'O', name)
        self.add_element(l, number, 'number2', 'O', name)
        yield l.load_item()

    def add_element_by_selector(self, item_loader, response, selector, key, annotation, name):
        try:
            el = ' '.join(response.css(selector).xpath('text()').extract()).strip()
            item_loader.add_value(key, el)
            item_loader.add_value(key, annotation)
            if name in self.orders:
                self.orders[name].append(key)
            else:
                self.orders[name] = [key]
            return True
        except Exception:
            return False

    def add_element_by_xpath(self, item_loader, response, selector, key, annotation, name):
        try:
            el = ' '.join(response.xpath(selector).xpath('text()').extract()).strip()
            item_loader.add_value(key, el)
            item_loader.add_value(key, annotation)
            if name in self.orders:
                self.orders[name].append(key)
            else:
                self.orders[name] = [key]
            return True
        except Exception:
            return False

    def add_element(self, item_loader, value, key, annotation, name):
        try:
            item_loader.add_value(key, value)
            item_loader.add_value(key, annotation)
            if name in self.orders:
                self.orders[name].append(key)
            else:
                self.orders[name] = [key]
            return True
        except Exception:
            return False

    def add_element_by_selector_text(self, item_loader, response, selector, key, annotation, name, option=None):
        try:
            if option is None:
                el = self.cut_tags(' '.join(response.css(selector).extract()).strip())
            elif option == 'first':
                el = self.cut_tags(' '.join(response.css(selector)[:1].extract()).strip())
            elif option == 'last':
                el = self.cut_tags(' '.join(response.css(selector)[1:].extract()).strip())
            else:
                el = self.cut_tags(' '.join(response.css(selector).extract()).strip())
            item_loader.add_value(key, el)
            item_loader.add_value(key, annotation)
            if name in self.orders:
                self.orders[name].append(key)
            else:
                self.orders[name] = [key]
            return True
        except Exception:
            return False

    def add_elements_by_selector(self, item_loader, response, selector, key, annotation, name):
        try:
            text = ' '.join(response.css(selector).xpath('text()').extract()).strip()
            if len(text) > 0:
                item_loader.add_value(key, text)
                item_loader.add_value(key, annotation)
                if name in self.orders:
                    self.orders[name].append(key)
                else:
                    self.orders[name] = [key]
                return True
        except Exception:
            return False

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


