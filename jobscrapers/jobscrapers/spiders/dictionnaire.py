# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import SpieItem
import time

class DictionnaireSpider(scrapy.Spider):

    name = "dictionnaire"
    publisher = "Dictionnaire"
    publisherurl = 'http://dictionnaire.sensagent.leparisien.fr'
    drain = False
    dirname = 'dictionnaire'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36'
    limit = False

    def __init__(self, limit=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)

    def start_requests(self):
        allowed_domains = ["http://dictionnaire.sensagent.leparisien.fr"]
        urls = ['http://dictionnaire.sensagent.leparisien.fr/liste%20des%20m%C3%A9tiers/fr-fr/']
        for index in range(len(urls)):
            url = urls[index]
            request = scrapy.Request(url, callback=self.get_jobs_list)
            headers = request.headers
            headers['User-Agent'] = self.user_agent
            request = request.replace(headers=headers)
            yield request


    def get_jobs_list(self, response):
        links = response.css('td[valign="top"] ul li a').xpath('@href').extract()
        words = response.css('td[valign="top"] ul li a').xpath('text()').extract()
        count = len(links)
        if self.limit:
            count = self.limit
        for i in range(count):
            url = ''.join(['http://dictionnaire.sensagent.leparisien.fr', links[i]])
            request = scrapy.Request(url, callback=self.parse_page)
            headers = request.headers
            headers['User-Agent'] = self.user_agent
            request = request.replace(headers=headers)
            request.meta['name'] = words[i]
            yield request

    def parse_page(self, response):
        l = ItemLoader(item=SpieItem())
        l.add_value('name', response.meta['name'])
        l.add_value('word', response.meta['name'])
        try:
            definition = response.css('div[class="definitions"] p[class="definition"] span[class="wording"]').xpath('text()').extract()[0]
            definition = definition.replace(',', '').replace(';', '').replace('.', '').replace('"', '').replace("'", '')
            l.add_value('definition', definition)
        except Exception:
            pass
        try:
            l.add_value('synonyms', ','.join(response.css('div[class="divSynonyms"] p[class="synonyms"] a span[class="wording"]').xpath('text()').extract()))
        except Exception:
            pass
        yield l.load_item()



