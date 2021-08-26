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


class InzejobSpider(scrapy.Spider):

    name = "inzejob"
    publisher = "inzejob"
    publisherurl = 'https://emploi.inzejob.com'
    url_index = None
    dirname = 'inzejob'
    limit = False
    drain = False
    orders = {}
    min_len = 50
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
        allowed_domains = ["https://emploi.inzejob.com"]
        if self.rundebug:
            url = 'https://emploi.inzejob.com/offre-emploi.php?id=5575715'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = url.split('=').pop()
            yield request
        else:
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'https://emploi.inzejob.com/recherche.php?query={title}&location=&position=&contract_type='.format(title=title)
                request = scrapy.Request(url, callback=self.get_job_list)
                yield request

    def get_job_list(self, response):
        links = response.css('div[class="jobs-list"] h3 a').xpath('@href').extract()
        for link in links:
            url = '/'.join(['https://emploi.inzejob.com', link])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = link.split('=').pop()
            yield request
        next_link = ' '.join(response.css('ul[class="pagination"] li a[title="Page suivante"]').xpath('@href').extract())
        if len(next_link) > 0:
            next_url = ''.join(['https://emploi.inzejob.com', next_link])
            request = scrapy.Request(next_url, callback=self.get_job_list)
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        description = ' '.join(response.css('div[class="page-container"]').extract()).replace(' '.join(response.css('a[id="apply-for-job-description"]').extract()), '').replace(' '.join(response.css('div[class="well apply-for-job"]').extract()), '')
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




