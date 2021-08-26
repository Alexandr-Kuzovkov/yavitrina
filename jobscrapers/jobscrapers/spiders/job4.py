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

class Job4Spider(scrapy.Spider):

    name = "job4"
    publisher = "Job4"
    publisherurl = 'https://job4.fr/'
    url_index = None
    dirname = 'job4'
    limit = False
    drain = False
    reannotate_only = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    min_item_len = 4
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')


    def __init__(self, limit=False, drain=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if ra:
            self.reannotate_only = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))

    def start_requests(self):
        if self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            allowed_domains = ["https://job4.fr"]
            url = 'https://job4.fr/offres-demplois'
            request = scrapy.Request(url, callback=self.parse_page)
            yield request

    def parse_page(self, response):
        urls = response.css('div[class="items js-coll-portfolio"]')[0].css('article a').xpath('@href').extract()
        for url in urls:
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['uri'] = url.split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SynergieItem())
        name = response.meta['uri']
        l.add_value('name', name)

        if not self.add_element_by_selector(l, response, 'div[class="single_job_listing"] ul li[class="location"] a', 'location', 'city', name):
            self.add_element_by_selector(l, response, 'div[class="single_job_listing"] ul li[class="location"]', 'location', 'city', name)

        if not self.add_element_by_selector(l, response, 'div[class="job_description"] h2 span', 'title', 'position', name):
            self.add_element_by_selector(l, response, 'div[class="job_description"] h2 span b', 'title', 'position', name)

        selectors = [
            'div[class="job_description"] h3 span strong',
            'div[class="job_description"] h3 span b',
            'div[class="job_description"] p span strong',
            'div[class="job_description"] p span b'
        ]
        for selector in selectors:
            headers = map(lambda j: j.strip().replace(' :', ''), filter(lambda i: len(i.strip())>0, response.css(selector).xpath('text()').extract()))
            if len(headers) > 0:
                break
        positions = []
        html = response.css('div[class="job_description"]').extract()[0]
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
                          u"TES MISSIONS DE BUSINESS DEVELOPER": 'mission',
                          u"TES MISSIONS DE HEAD OF SALES": 'mission',
                          u"CE QUE TU VAS ACCOMPLIR ?": 'mission',
                          u"CE QUE TU VAS ACCOMPLIR": 'mission',
                          u'TON FUTUR SALAIRE': 'salary',
                          u"PARLONS ARGENT ?": 'salary',
                          u"TON PROFIL DE BUSINESS DEVELOPER": 'company_desription',
                          u'TON PROFIL DE HEAD OF SALES': 'candidat_description'
        }
        for i in range(0, len(headers)):
            annotation = 'O'
            if headers[i] in headers_map:
                key = headers_map[headers[i]]+'_l'
                self.add_element(l, headers[i], key, annotation, name)
                key = headers_map[headers[i]]
                if key == 'salary':
                    annotation = 'salary'
                self.add_element(l, headers[i], key, annotation, name)
            else:
                key = 'desc' + str(i) + '_l'
                self.add_element(l, headers[i], key, annotation, name)
                key = 'desc' + str(i)
                self.add_element(l, texts[i], key, annotation, name)
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
                           'address', 'figure', 'td', 'th', 'tr', 'img', 'div', 'br', 'strong', 'p', 'h3', 'span', 'b',
                           'ul', 'li']:
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



