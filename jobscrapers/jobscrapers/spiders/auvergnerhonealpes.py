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

class AuvergnerhonealpesSpider(scrapy.Spider):

    name = "auvergnerhonealpes"
    publisher = "Auvergnerhonealpes"
    publisherurl = 'https://nostalentsnosemplois.auvergnerhonealpes.fr'
    url_index = None
    dirname = 'auvergnerhonealpes'
    limit = False
    drain = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    annotation = False
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    contract_types = [u'CDI', u'CDD', u'Stage', u'Freelance', u'Autres', u'Alternance', u'Full-time', u'Part-time', u'Internship', u'Freelance', u'Other', u'Apprenticeship']

    def __init__(self, limit=False, drain=False, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1 and j[0] == 'FR', map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        if annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["https://nostalentsnosemplois.auvergnerhonealpes.fr/jobsearch/offers"]
        urls = []
        for title in wood_jobs:
            title = urllib.quote(title.encode('utf-8'))
            url = 'https://nostalentsnosemplois.auvergnerhonealpes.fr/jobsearch/offers?what={title}'.format(title=title)
            request = scrapy.Request(url, callback=self.get_job_list)
            request.meta['search_url'] = url
            request.meta['industry'] = title
            yield request

    def get_job_list(self, response):
        links = response.css('ul[class="mj-offers-list"] li a[class="block-link"]').xpath('@href').extract()
        for link in links:
            url = ''.join(['https://nostalentsnosemplois.auvergnerhonealpes.fr', link])
            request = scrapy.Request(url, callback=self.parse_job)
            yield request
        pg_links = response.css('ul[class="pagination-pages"] li a').xpath('@href').extract()
        for pg_link in pg_links:
            pg_url = ''.join(['https://nostalentsnosemplois.auvergnerhonealpes.fr', pg_link])
            request = scrapy.Request(pg_url, callback=self.get_job_list)
            yield request

    def parse_job(self, response):
        parsed = urlparse.urlparse(response.url)
        name = parsed.path.split('/').pop()
        position = ' '.join(response.css('div[class="title"] h1').xpath('text()').extract())
        company = ' '.join(response.css('div[class="logo  "] a p').xpath('text()').extract())
        if len(company) == 0:
            company = ' '.join(response.css('div[class="logo  "] a img').xpath('@alt').extract())
        criteries = response.css('div[class="row matching-criteria well"] ul li span[class="matching-criterion-wrapper"]').extract()
        criteries = map(lambda i: self.rm_spaces(self.cut_tags(i)).strip(), criteries)
        # annotated
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            l1.add_value('position', position)
            l1.add_value('position', 'position')
            self.add_order(name, 'position')
            l1.add_value('company', company)
            l1.add_value('company', 'company')
            self.add_order(name, 'position')
            l1.add_value('published', ' '.join(response.css('span[class="publication-date"]').xpath('text()').extract()))
            l1.add_value('published', 'O')
            self.add_order(name, 'published')
            l1.add_value('criteries_title', ' '.join(response.css('div[class="row matching-criteria well"] h2').xpath('text()').extract()))
            l1.add_value('criteries_title', 'O')
            self.add_order(name, 'criteries_title')
            index = 1
            for critery in criteries:
                if critery.startswith(position):
                    l1.add_value('position2', critery)
                    l1.add_value('position2', 'position')
                    self.add_order(name, 'position2')
                elif len(filter(lambda i: critery.lower().startswith(i), self.cities)) > 0:
                    l1.add_value('city', critery)
                    l1.add_value('city', 'city')
                    self.add_order(name, 'city')
                elif len(filter(lambda i: critery.startswith(i), self.contract_types)) > 0:
                    l1.add_value('contract_type', critery)
                    l1.add_value('contract_type', 'contract_type')
                    self.add_order(name, 'contract_type')
                elif len(filter(lambda i: critery.startswith(i), [u'Temps'])) > 0:
                    l1.add_value('position_scheduled', critery)
                    l1.add_value('position_scheduled', 'position_scheduled')
                    self.add_order(name, 'position_scheduled')
                elif len(filter(lambda i: critery.startswith(i), [u'Expérience requise :'])) > 0:
                    l1.add_value('experience_l', u'Expérience requise :')
                    l1.add_value('experience_l', 'O')
                    self.add_order(name, 'experience_l')
                    l1.add_value('experience', critery.replace(u'Expérience requise :', ''))
                    l1.add_value('experience', 'experience')
                    self.add_order(name, 'experience')
                elif len(filter(lambda i: critery.startswith(i), [u'Permis'])) > 0:
                    l1.add_value('hard_skills', critery)
                    l1.add_value('hard_skills', 'hard_skills')
                    self.add_order(name, 'hard_skills')
                elif len(filter(lambda i: critery.startswith(i), [u"Niveau d'études"])) > 0:
                    l1.add_value('education', critery)
                    l1.add_value('education', 'education')
                    self.add_order(name, 'education')
                else:
                    key = 'other'+str(index)
                    l1.add_value(key, critery)
                    l1.add_value(key, 'O')
                    self.add_order(name, key)
                    index += 1

            description = [
                ' '.join(response.css('section[class="company-description"]').extract()),
                ' '.join(response.css('section[class="job-description"]').extract()),
                ' '.join(response.css('section[class="profile-description"]').extract()),
                ' '.join(response.css('section[class="offer-apply-form"]').xpath('preceding-sibling::section')[-1:].extract())
            ]
            l1.add_value('description', ' '.join(map(lambda i: self.cut_tags(self.rm_spaces(i)), description)))
            l1.add_value('description', 'O')
            yield l1.load_item()
        #plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        criteries = ' '.join(response.css('div[class="row matching-criteria well"] ul li span[class="matching-criterion-wrapper"]').extract())
        description = '\n'.join([
            ' '.join(response.css('section[class="company-description"]').extract()),
            ' '.join(response.css('section[class="job-description"]').extract()),
            ' '.join(response.css('section[class="profile-description"]').extract()),
            ' '.join(response.css('section[class="offer-apply-form"]').xpath('preceding-sibling::section')[-1:].extract())
        ])
        html = '\n'.join([
            position,
            company,
            ' '.join(response.css('span[class="publication-date"]').extract()),
            ' '.join(response.css('div[class="row matching-criteria well"] h2').extract()),
            criteries,
            description
        ])
        l2.add_value('text', html)
        yield l2.load_item()


    def rm_spaces(self, text):
        text = text.replace('\n', ' ')
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
                    text = re.sub("""<%s.*?>""" % (tag,), '', text)
                    text = re.sub("""<\/%s>""" % (tag,), '', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]




