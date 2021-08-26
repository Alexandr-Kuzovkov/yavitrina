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
from scrapy_splash import SplashRequest

class AgentCoSpider(scrapy.Spider):

    name = "agent-co"
    publisher = "Agent-co"
    publisherurl = 'https://www.agent-co.fr'
    url_index = None
    dirname = 'agent-co'
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
        allowed_domains = ["https://www.glassdoor.fr"]
        if self.rundebug:
            url = 'https://www.agent-co.fr/fr/missions-commerciales/agent-commercial-france-agent-de-vetements-pour-enfant-bebe-distributeur--638841?q=Agent+Forestier&c=73&r=0&pr=0&s=0&i=0&p=1&fav=0&new=0'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'https://www.agent-co.fr/fr/missions-commerciales?q={title}&c=73&r=0&pr=0&s=0&i=0&fav=0&new=0&p=1'.format(title=title)
                request = SplashRequest(url, callback=self.get_job_list, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
                parsed = urlparse.urlparse(url)
                try:
                    p = int(urlparse.parse_qs(parsed.query)['p'][0])
                except Exception:
                    p = 1
                request.meta['p'] = p
                yield request

    def get_job_list(self, response):
        links = response.css('div[id="assignmentsSearchResults"] ul li h3 a').xpath('@href').extract()
        for link in links:
            url = link
            request = scrapy.Request(url, callback=self.parse_job)
            parsed = urlparse.urlparse(url)
            name = parsed.path.split('/').pop()
            request.meta['name'] = name
            yield request
        next_links = response.css('div[class="pages"] a[class="next"]')
        if len(next_links) > 0:
            p = response.meta['p']
            next_url = response.url.replace('&p={p}'.format(p=p), '&p={p}'.format(p=p+1))
            request = SplashRequest(next_url, callback=self.get_job_list, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            request.meta['p'] = p + 1
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        description = ' '.join(response.css('div[id="openedAdContents"]').extract())
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




