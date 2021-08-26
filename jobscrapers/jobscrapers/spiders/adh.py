# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import SynergieItem
from jobscrapers.items import annotations_list
import math
import re
import os
import pkgutil
from jobscrapers.extensions import Geocode

r_title = re.compile('^.*F/H')
r_city = re.compile('F/H,.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class AdhSpider(scrapy.Spider):

    name = "adh"
    publisher = "ADH"
    publisherurl = 'http://jobs.adh.fr'
    url_index = None
    dirname = 'adh'
    limit = False
    drain = False
    reannotate_only = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')


    def __init__(self, limit=False, drain=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if ra:
            self.reannotate_only = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1 and j[0] == 'FR', map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))

    def start_requests(self):
        if self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            allowed_domains = ["http://jobs.adh.fr"]
            url = 'http://jobs.adh.fr/offres?n=123'
            request = scrapy.Request(url, callback=self.get_pages)
            yield request

    def get_pages(self, response):
        paginations = response.css('ul[class="pagination pagination-sm"] li a').xpath('text()').extract()
        max_page = int(paginations[-2])
        if self.limit:
            max_page = int(math.ceil(self.limit/5.0))
        self.logger.info('Max_page={max_page}'.format(max_page=max_page))
        for page in range(1, max_page + 1):
            url = 'http://jobs.adh.fr/offres'
            if page > 1:
                url = 'http://jobs.adh.fr/offres?page={page}'.format(page=page)
            request = scrapy.Request(url, callback=self.parse_page)
            request.meta['search_url'] = url
            yield request

    def parse_page(self, response):
        urls = response.css('div.media-head-intitule h4 a').xpath('@href').extract()
        for url in urls:
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['uri'] = url.split('/').pop()
            request.meta['id'] = url.split('/').pop(-2)
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SynergieItem())
        #title#
        name = '-'.join([response.meta['uri'], response.meta['id']])
        l.add_value('name', name)
        self.add_element_by_selector(l, response, 'h1[class="bleu mainTitle"]', 'title', 'position', name)
        self.add_element_by_selector(l, response, 'h4[class="media-heading"]', 'title2', 'O', name)
        self.add_elements_by_selector(l, response, 'div[class="media-metiers"] ul li', 'experience', 'experience', name)
        self.add_element_by_selector(l, response, 'p[class="chapeau"]', 'desc2', 'O', name)

        headers = map(lambda j: j.strip().replace(' :', ''), filter(lambda i: len(i.strip())>0, response.css('div[class="media-descriptions"] strong').xpath('text()').extract()))
        positions = []
        html = response.css('div[class="media-descriptions"]').extract()[0]
        text = self.cut_tags(html)
        text = text.strip().replace('\n', '').replace('\t', '')
        for header in headers:
            #positions.append((text.find(header.encode('utf-8')), text.find(header.encode('utf-8')) + len(header)))
            positions.append((text.find(header), text.find(header) + len(header)))
        texts = []
        for i in range(0, len(positions)):
            if i < len(positions) - 1:
                texts.append(text[positions[i][1]:positions[i + 1][0]].strip())
            else:
                texts.append(text[positions[i][1]:].strip())
        headers_map = {
                          u"Numéro de l'offre": 'id',
                          u'Lieu': 'location',
                          u"Description de l'entreprise": 'company_desription',
                          u'Profil du candidat': 'candidat_description'
        }
        for i in range(0, len(headers)):
            if headers[i] in headers_map:
                key = headers_map[headers[i]] + '_l'
                annotation = 'O'
                self.add_element(l, headers[i], key, annotation, name)
                key = headers_map[headers[i]]
                self.add_element(l, texts[i], key, annotation, name)
            else:
                raise Exception('Header "%s" not in headers_map' % headers[i])
        yield l.load_item()


    def add_element_by_selector(self, item_loader, response, selector, key, annotation, name):
        try:
            el = response.css(selector).xpath('text()').extract()[0].strip()
            el = el.replace(u'– #STARTUPS', '').strip()
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
                           'address', 'figure', 'td', 'th', 'tr', 'img', 'div', 'br', 'strong']:
                    text = re.sub("""<%s.*?>""" % (tag,), '', text)
                    text = re.sub("""<\/%s>""" % (tag,), '', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        return text

    def reannotate(self, response):
        self.logger.info('RUN REANNOTATE ONLY!!!')
        files_dir = self.settings.get('FILES_DIR', '')
        source_dir = os.path.sep.join([files_dir, self.dirname, 'src'])
        list_of_files = os.listdir(source_dir)
        self.logger.info('%i files will reannotate' % len(list_of_files))
        for name in list_of_files:
            with open(os.path.sep.join([source_dir, name]), 'r') as fi:
                item = json.load(fi, 'utf-8')
                item['name'] = [name[:-4]]
                yield item



