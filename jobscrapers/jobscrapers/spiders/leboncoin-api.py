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


class LeboncoinApiSpider(scrapy.Spider):

    name = "leboncoin-api"
    publisher = "Leboncoin"
    publisherurl = 'https://www.leboncoin.fr'
    url_index = None
    dirname = 'leboncoin-api'
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
        allowed_domains = ["https://www.leboncoin.fr"]
        if self.rundebug:
            url = 'https://www.leboncoin.fr/offres_d_emploi/1631931804.htm/'
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            for title in wood_jobs[:]:
                data = self.get_jobs_from_api(title, 1)
                url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
                request = scrapy.Request(url, callback=self.get_job_list, dont_filter=True)
                request.meta['data'] = data
                request.meta['title'] = title
                yield request

    def get_jobs_from_api(self, query, page):
        command = 'jobscrapers/js/leboncoin.js "{title}" {page}'.format(title=query.encode('utf-8'), page=page)
        data = self.run_command(command)
        data = json.loads(data)
        return data

    def run_command(self, command):
        self.logger.info('Command: "%s"' % command)
        output = os.popen(command).read()
        #self.logger.info('Ouput: %s' % output)
        return output

    def get_job_list(self, response):
        data = response.meta['data']
        for job in data['results']:
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request(url, callback=self.parse_job, dont_filter=True)
            request.meta['job'] = job
            yield request
        length = data['len']
        page = data['page']
        if length > 0:
            title = response.meta['title']
            data = self.get_jobs_from_api(title, page + 1)
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request(url, callback=self.get_job_list, dont_filter=True)
            request.meta['data'] = data
            request.meta['title'] = title
            yield request

    def parse_job(self, response):
        job = response.meta['job']
        if job['category'] == u"Offres d'emploi":
            name = str(job['id'])
            description = '<br>'.join([
                    job['title'],
                    'Description',
                    job['description'],
                    u'Localisation',
                    job['location']['city'],
                    job['location']['zipcode']
                ])
            if self.annotation:
                l1 = ItemLoader(item=MonsterItem())
                l1.add_value('name', name)
                l1.add_value('itemtype', 'annotation')
                l1.add_value('description', self.rm_spaces(description))
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





