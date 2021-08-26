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
import math
import re
import os
import pkgutil
from jobscrapers.extensions import Geocode
from jobscrapers.items import wood_jobs


class EmploiTerritorialSpider(scrapy.Spider):

    name = "emploi-territorial"
    publisher = "emploi-territorial"
    publisherurl = 'https://www.emploi-territorial.fr/'
    url_index = None
    dirname = 'emploi-territorial'
    limit = False
    drain = False
    orders = {}
    min_len = 50
    cities = []
    #handle_httpstatus_list = [400]
    rundebug = False
    annotation = False
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'

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
        allowed_domains = ["https://www.emploi-territorial.fr/"]
        if self.rundebug:
            url = 'https://www.emploi-territorial.fr/details_offre/2-499902'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = url.split('/').pop()
            yield request
        else:
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'https://www.emploi-territorial.fr/page.php'
                data = {
                        'controller': 'rechercheoffre',
                        'search': title,
                        'secteur': -1,
                        'btn_search': 'Consulter'
                }
                body = urllib.urlencode(data)
                request = scrapy.Request(url, method='POST', body=body, callback=self.get_job_list, dont_filter=True)
                headers = request.headers
                headers['User-Agent'] = self.user_agent
                headers['Origin'] = 'https://www.emploi-territorial.fr'
                headers['Referer'] = 'https://www.emploi-territorial.fr/recherche_emploi_mobilite/1/'
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
                request = request.replace(headers=headers)
                yield request

    def get_job_list(self, response):
        links = response.css('div[class="btn-lien-details-offre intitule-poste"] + a').xpath('@href').extract()
        for link in links:
            url = ''.join(['https://www.emploi-territorial.fr', link])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = url.split('/').pop()
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        description = ' '.join(response.css('div[class="page"]').extract())
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            l1.add_value('description', self.rm_spaces(self.cut_tags(description)))
            l1.add_value('description', 'description')
            self.add_order(name, 'description')
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = description
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
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), ' ', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]




