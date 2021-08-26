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

class VivastreetSpider(scrapy.Spider):

    name = "vivastreet"
    publisher = "Vivastreet"
    publisherurl = 'https://search.vivastreet.com'
    url_index = None
    dirname = 'vivastreet'
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
        allowed_domains = ["https://search.vivastreet.com"]
        if self.rundebug:
            self.logger.info('!!!Run debug')
            url = 'https://www.vivastreet.com/emploi-agriculture/aix-en-othe-10160/commis-de-bois--h-f-/198099241'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'https://search.vivastreet.com/emploi-agriculture/fr?lb=new&search=1&start_field=1&keywords={title}&cat_1=271&geosearch_text=&searchGeoId=&sp_communities_rideshare_detail='.format(title=title)
                request = scrapy.Request(url, callback=self.get_job_list)
                yield request

    def get_job_list(self, response):
        links = response.css('div[class="clad"] a[class="clad__wrapper"]').xpath('@href').extract()
        for link in links:
            url = link
            request = scrapy.Request(url, callback=self.parse_job)
            parsed = urlparse.urlparse(url)
            name = parsed.path.split('/').pop()
            request.meta['name'] = name
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        description = ' '.join(response.css('div[class="kiwii-padding-ver-xxsmall vs-description-wrapper"]').extract())
        footer = ' '.join(response.css('ul[class="kiwii-description-footer"]').extract())
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            position = ' '.join(response.css('h1[class="kiwii-font-xlarge kiwii-margin-none kiwii-font-weight-semibold"]').xpath('text()').extract())
            self.add_element(l1, position, name, 'position', 'position', html=False)
            other11 = ' '.join(response.css('div[class="kiwii-poster-details"]').extract())
            self.add_element(l1, other11, name, 'other1', 'O', html=True)
            keys = response.css('table[id="details-tbl-specs"] td.kiwii-font-dark').xpath('text()').extract()
            values = response.css('table[id="details-tbl-specs"] td.kiwii-font-dark').xpath('following-sibling::td').extract()
            for i in range(len(keys)):
                if keys[i].startswith(u'Ville/Code postal'):
                    line = values[i]
                    city = line.split('<br>').pop().replace('</div>', '').replace('</td>', '').strip().split('-')[0].strip()
                    postal_code = line.split('<br>').pop().replace('</div>', '').replace('</td>', '').strip().split('-')[1].strip()
                    self.add_element(l1, keys[i], name, 'location', 'O', html=False)
                    self.add_element(l1, '<br> '.join(line.split('<br>')[:-1]), name, 'other'+str(i), 'O', html=True)
                    self.add_element(l1, city, name, 'city', 'city', html=False)
                    self.add_element(l1, postal_code, name, 'postal_code', 'postal_code', html=False)
                elif keys[i].startswith(u'Type de contrat'):
                    self.add_element(l1, keys[i], name, 'contract_type_l', 'O', html=False)
                    self.add_element(l1, values[i], name, 'contract_type', 'contract_type', html=True)
                elif keys[i].startswith(u"Type d'emploi"):
                    self.add_element(l1, keys[i], name, 'position_scheduled_l', 'O', html=False)
                    self.add_element(l1, values[i], name, 'position_scheduled', 'position_scheduled', html=True)
                else:
                    key = 'other' + str(i)
                    self.add_element(l1, keys[i], name, key+'_l', 'O', html=False)
                    self.add_element(l1, values[i], name, key, 'O', html=True)
            self.add_element(l1, description, name, 'description', 'O', html=True)
            self.add_element(l1, footer, name, 'footer', 'O', html=True)
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            ' '.join(response.css('div[id="kiwii-details-carousel-block"]').extract()),
            ' '.join(response.css('table[id="details-tbl-specs"]').extract()),
            description,
            footer
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




