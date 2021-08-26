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

class KeljobSpider(scrapy.Spider):

    name = "keljob"
    publisher = "Keljob"
    publisherurl = 'https://www.keljob.com/'
    url_index = None
    dirname = 'keljob'
    limit = False
    drain = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    rundebug = False
    annotation = False
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    contract_types = [u'CDI', u'CDD', u'Stage', u'Freelance', u'Autres', u'Alternance', u'Full-time', u'Part-time',
                      u'Internship', u'Freelance', u'Other', u'Apprenticeship', u'Intérim']

    def __init__(self, limit=False, drain=False, debug=None, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        if annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["https://www.keljob.com/"]
        if self.rundebug:
            self.logger.info('!!!Run debug')
            url = 'https://www.keljob.com/offre/reparateur-palettes-h-f-62527197?context=c2VjdGV1cjpBbWV1YmxlbWVudCBpbmR1c3RyaWUgZHUgYm9pcw....MTAwMDAxLDEwMDAwMywxMDAwMDUsMTAwMDA3LDEwMDAwOCwxMDAwMDksMTAwMDExLDEwMDAxNQ....MQ.MjU'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            #url = 'https://www.keljob.com/recherche?q=secteur:Ameublement%20industrie%20du%20bois&sec=100001,100003,100005,100007,100008,100009,100011,100015'
            for title in wood_jobs:
                title = urllib.quote(title.encode('utf-8'))
                url = 'https://www.keljob.com/recherche?q={title}'.format(title=title)
                request = scrapy.Request(url, callback=self.get_job_list)
                yield request

    def get_job_list(self, response):
        links = response.css('h2[class="offre-title"] a').xpath('@href').extract()
        for link in links:
            url = ''.join(['https://www.keljob.com', link])
            request = scrapy.Request(url, callback=self.parse_job)
            parsed = urlparse.urlparse(url)
            name = parsed.path.split('/').pop()
            request.meta['name'] = name
            yield request
        next_link = ' '.join(response.css('div[class="pagination-centered"] ul[class="pagination"] li[class="arrow"] a').xpath('@href').extract()[:1])
        next_url = ''.join(['https://www.keljob.com', next_link])
        request = scrapy.Request(next_url, callback=self.get_job_list)
        yield request

    def parse_job(self, response):
        name = response.meta['name']
        position = ' '.join(response.css('div[class="jobs-detail__header"] h1').xpath('text()').extract())
        description = ' '.join(response.css('div[id="content-container"]').extract())
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            self.add_element(l1, position, name, 'position', 'position', html=False)
            elements = map(lambda i: self.rm_spaces(i.strip()), response.css('div[class="jobs-detail__header"] ul li').xpath('text()').extract())
            number = ' '.join(map(lambda i: i.strip(), response.css('div[class="jobs-detail__header"] ul li span').xpath('text()').extract()))
            if len(elements) == 5:
                self.add_element(l1, elements[0], name, 'other1', 'O', html=False)
                self.add_element(l1, number, name, 'other2', 'O', html=False)
                self.add_element(l1, elements[1], name, 'other3', 'O', html=False)
                if len(filter(lambda i: elements[2].startswith(i), self.contract_types)):
                    self.add_element(l1, elements[2].split(' ')[0], name, 'contract_type', 'contract_type', html=False)
                    self.add_element(l1, ' '.join(elements[2].split(' ')[1:]), name, 'duration', 'duration', html=False)
                else:
                    self.add_element(l1, elements[2], name, 'other4', 'O', html=False)
                if len(filter(lambda i: elements[3].lower().startswith(i), self.cities)) > 0:
                    self.add_element(l1, elements[3].split('(')[0].strip(), name, 'city', 'city', html=False)
                    self.add_element(l1, elements[3].split('(')[1].replace(')', '').strip(), name, 'postal_code', 'postal_code', html=False)
                else:
                    self.add_element(l1, ' '.join(elements[3].split(' ')[1:]), name, 'other5', 'O', html=False)
                self.add_element(l1, elements[4].strip(), name, 'other6', 'O', html=False)
            elif len(elements) == 4:
                self.add_element(l1, elements[0], name, 'other1', 'O', html=False)
                if len(filter(lambda i: elements[1].startswith(i), self.contract_types)):
                    self.add_element(l1, elements[1].split(' ')[0], name, 'contract_type', 'contract_type', html=False)
                    self.add_element(l1, ' '.join(elements[1].split(' ')[1:]), name, 'duration', 'duration', html=False)
                else:
                    self.add_element(l1, elements[1], name, 'other4', 'O', html=False)
                if len(filter(lambda i: elements[2].lower().startswith(i), self.cities)) > 0:
                    self.add_element(l1, elements[2].split('(')[0].strip(), name, 'city', 'city', html=False)
                    self.add_element(l1, elements[2].split('(')[1].replace(')', '').strip(), name, 'postal_code', 'postal_code', html=False)
                else:
                    self.add_element(l1, ' '.join(elements[2].split(' ')[1:]), name, 'other5', 'O', html=False)
                self.add_element(l1, elements[3].strip(), name, 'other6', 'O', html=False)
            self.add_element(l1, description, name, 'description', 'O', html=True)
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            ' '.join(response.css('div[class="jobs-detail__header"]').extract()),
            description
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
                            'li', 'ul', 'ol', 'p', 'hr'
                           ]:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), ' ', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]

    def add_element(self, l, text, name, key, annotation, html=False):
        value = text
        if len(value) == 0:
            return
        if html:
            value = self.rm_spaces(self.cut_tags(value))
        l.add_value(key, value)
        l.add_value(key, annotation)
        self.add_order(name, key)




