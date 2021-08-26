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

class CarriereonlineSpider(scrapy.Spider):

    name = "carriereonline"
    publisher = "carriereonline"
    publisherurl = 'http://www.carriereonline.com'
    url_index = None
    dirname = 'carriereonline'
    limit = False
    drain = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    rundebug = False
    annotation = False

    def __init__(self, limit=False, drain=False, debug=None, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["http://www.carriereonline.com/"]
        if self.rundebug:
            url = 'http://www.carriereonline.com/offre-emploi/menuisier-agencement-h-f/cote-d-or-21/9156890/'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'http://www.carriereonline.com/annonce_liste.php?savekw=1&typeLieu=&selectionFullTextFct=&selectionFullTextGeo=&filters%5BKEYWORDS%5D={title}&filters%5BgeoKEYWORDS%5D='.format(title=title)
                request = scrapy.Request(url, callback=self.get_job_list)
                yield request

    def get_job_list(self, response):
        links = response.css('ul[id="listeId"] h2 a').xpath('@href').extract()
        for link in links:
            url = ''.join(['http://www.carriereonline.com', link])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = '-'.join(link.split('/')[1:])
            yield request
        next_link = ' '.join(response.css('div[class="flor"] a[class="page_fl_suiv"]').xpath('@href')[:1].extract())
        next_url = ''.join(['http://www.carriereonline.com', next_link])
        request = scrapy.Request(next_url, callback=self.get_job_list)
        yield request

    def parse_job(self, response):
        name = response.meta['name']
        position = ' '.join(response.css('div[class="dann2015_intitule"] h1').xpath('text()').extract())
        description = response.css('div[class="det_txt"]').extract()
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            l1.add_value('position', position)
            l1.add_value('position', 'position')
            self.add_order(name, 'position')
            l1.add_value('resume_title', ' '.join(response.css('div[class="d2015_encg_resumeint"] p').xpath('text()').extract()))
            l1.add_value('resume_title', 'O')
            self.add_order(name, 'resume_title')
            published = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Offre")]/strong/text()').extract())
            published_l = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Offre")]/text()').extract())
            if len(published_l) > 0:
                l1.add_value('published_l', published_l)
                l1.add_value('published_l', 'O')
                self.add_order(name, 'published_l')
                l1.add_value('published', published)
                l1.add_value('published', 'O')
                self.add_order(name, 'published')
            contract_type = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Type de contrat")]/strong/text()').extract())
            contract_type_l = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Type de contrat")]/text()').extract())
            if len(contract_type_l) > 0:
                l1.add_value('contract_type_l', contract_type_l)
                l1.add_value('contract_type_l', 'O')
                self.add_order(name, 'contract_type_l')
                l1.add_value('contract_type', contract_type)
                l1.add_value('contract_type', 'contract_type')
                self.add_order(name, 'contract_type')
            contract_duration = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Durée")]/strong/text()').extract())
            contract_duration_l = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Durée")]/text()').extract())
            if len(contract_duration_l) > 0:
                l1.add_value('contract_duration_l', contract_duration_l)
                l1.add_value('contract_duration_l', 'O')
                self.add_order(name, 'contract_duration_l')
                l1.add_value('contract_duration', contract_duration)
                l1.add_value('contract_duration', 'contract_duration')
                self.add_order(name, 'contract_duration')
            ref = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Référence")]/strong/text()').extract())
            ref_l = ' '.join(response.css('div[class="d2015_encg_resumeint"] ul').xpath(u'//li[starts-with(text(),"Référence")]/text()').extract())
            if len(ref_l) > 0:
                l1.add_value('ref_l', ref_l)
                l1.add_value('ref_l', 'O')
                self.add_order(name, 'ref_l')
                l1.add_value('ref', ref)
                l1.add_value('ref', 'O')
                self.add_order(name, 'ref')
            l1.add_value('description', ' '.join(map(lambda i: self.rm_spaces(self.cut_tags(i)), description)))
            l1.add_value('description', 'O')
            self.add_order(name, 'description')
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            position,
            ' '.join(response.css('div[class="d2015_encg_resumeint"] p').extract()),
            ' '.join(response.css('div[class="d2015_encg_resumeint"] ul li').extract()),
            ' '.join(description)
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
                            'li', 'ul', 'ol', 'p'
                           ]:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]




