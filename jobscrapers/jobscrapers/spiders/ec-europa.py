# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import MonsterItem
from jobscrapers.items import PlainItem
from scrapy_splash import SplashRequest
from jobscrapers.items import onisep_data_template
from jobscrapers.items import onisep_act_template
from jobscrapers.items import onisep_comp_template
from jobscrapers.items import onisep_education_template
from jobscrapers.items import onisep_formation_template
from jobscrapers.items import annotations_list
import math
import re
import os
import pkgutil
from jobscrapers.extensions import Geocode
from jobscrapers.items import wood_jobs

r_title = re.compile('^.*F/H')
r_city = re.compile('F/H,.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class EcEuropaSpider(scrapy.Spider):

    name = "ec-europa"
    publisher = "ec-europa"
    publisherurl = 'https://ec.europa.eu'
    url_index = None
    dirname = 'ec-europa'
    limit = 1000
    drain = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    min_len = 50
    rundebug = False
    lua_src1 = pkgutil.get_data('jobscrapers', 'lua/ec-europa.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/ec-europa-job.lua')
    annotation = False

    def __init__(self, limit=False, drain=False, debug=None, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if self.annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["https://ec.europa.eu"]
        if self.rundebug:
            self.logger.info('!!!Run debug')
            url = 'https://ec.europa.eu/eures/eures-searchengine/page/main?lang=fr#/jv-details/MDg4VkZWUiA5'
            request = SplashRequest(url, callback=self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = url.split('/').pop()
            yield request
        else:
            url = 'https://ec.europa.eu/eures/eures-searchengine/page/main?lang=fr#/search'
            for title in wood_jobs[:]:
                args = {'wait': 0.5, 'lua_source': self.lua_src1, 'timeout': 3600, 'limit': self.limit, 'search': title}
                #args = {'wait': 0.5, 'lua_source': self.lua_src1, 'timeout': 3600, 'limit': self.limit}
                request = SplashRequest(url, callback=self.get_job_list, endpoint='execute', args=args)
                yield request

    def get_job_list(self, response):
        data = json.loads(response.text)
        links = data.values()
        for link in links:
            url = ''.join(['https://ec.europa.eu/eures/eures-searchengine/page/main?lang=fr', link])
            request = SplashRequest(url, callback=self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = link.split('/').pop()
            yield request

    def parse_job(self, response):
        data = json.loads(response.text)
        name = response.meta['name']
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            self.add_element_from_data(l1, data, name, 'position', 'position', html=False)
            self.add_element_from_data(l1, data, name, 'poste', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'poste', 'O', html=False)
            if 'country' in data:
                country = data['country'].replace('\n', '').split(':')[0]
                region = data['country'].replace('\n', '').split(':').pop()
                self.add_element(l1, country, name, 'country', 'country', html=False)
                self.add_element(l1, region, name, 'region', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'age1', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'age2', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'description', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'footer', 'O', html=True)
            self.add_element_from_data(l1, data, name, 'side_title1', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'lang_title', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'lang', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'exp_title', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'exp', 'experience_duration', html=False)
            self.add_element_from_data(l1, data, name, 'permis_title', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'permis', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'side_title2', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'type_title', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'type', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'contract_type_title', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'contract_type', 'position_scheduled', html=False)
            self.add_element_from_data(l1, data, name, 'side_title3', 'O', html=False)
            self.add_element_from_data(l1, data, name, 'company', 'company', html=False)
            self.add_element_from_data(l1, data, name, 'website', 'O', html=False)
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            self.get_element(data, 'position'),
            self.get_element(data, 'poste'),
            '<br>',
            self.get_element(data, 'country'),
            self.get_element(data, 'age1'),
            self.get_element(data, 'age2'),
            '<br>',
            self.get_element(data, 'description'),
            '<br>',
            self.get_element(data, 'footer'),
            '<br>',
            self.get_element(data, 'sidebar_html')
        ])
        l2.add_value('text', html)
        yield l2.load_item()

    def rm_spaces(self, text):
        text = text.replace('\n', ' ').replace('&nbsp;', ' ')
        while not text.find('  ') == -1:
            text = text.replace('  ', ' ')
        return text

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
                           'address', 'figure', 'td', 'th', 'tr', 'img', 'div', 'br', 'strong', 'span', 'section',
                            'li', 'ul', 'ol', 'p',
                            'details-box-label', 'details-box', 'share-jv-overlay', 'jv-details-header'
                           ]:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        text = re.sub("""<!--.*-->""", '', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]

    def add_element_from_data(self, l, data, name, key, annotation, html=False):
        if key in data:
            value = data[key]
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def add_element(self, l, text, name, key, annotation, html=False):
            value = text
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def get_element(self, data, key):
        if key in data:
            return data[key]
        return ''




