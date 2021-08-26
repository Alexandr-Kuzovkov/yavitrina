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

class EscoSpider(scrapy.Spider):

    name = "esco"
    publisher = "Esco"
    publisherurl = 'https://ec.europa.eu'
    drain = False
    dirname = 'esco'
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Safari/537.36'
    limit = False

    def __init__(self, limit=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)

    def start_requests(self):
        allowed_domains = ["https://ec.europa.eu"]
        url = 'https://ec.europa.eu/esco/portal/occupation?resetLanguage=true&newLanguage=fr'
        request = scrapy.Request(url, callback=self.get_data)
        headers = request.headers
        headers['User-Agent'] = self.user_agent
        request = request.replace(headers=headers)
        yield request

    def get_data(self, response):
        urls = ['https://ec.europa.eu/esco/portal/portalData']
        for index in range(len(urls)):
            url = urls[index]
            request = scrapy.Request(url, callback=self.get_jobs_list)
            headers = request.headers
            headers['User-Agent'] = self.user_agent
            request = request.replace(headers=headers)
            yield request

    def get_jobs_list(self, response):
        data = json.loads(response.text)
        items = data['occupationForumMapping']
        links = items.keys()
        count = len(links)
        if self.limit:
            count = self.limit
        for i in range(count):
            url = links[i]
            request = scrapy.Request(url, callback=self.change_lang)
            headers = request.headers
            headers['User-Agent'] = self.user_agent
            request = request.replace(headers=headers)
            request.meta['name'] = url.split('/').pop()
            yield request

    def change_lang(self, response):
        url = response.url
        url = url.replace('conceptLanguage=en', 'conceptLanguage=fr')
        request = scrapy.Request(url, callback=self.parse_page)
        request.meta['name'] = response.meta['name']
        yield request

    def parse_page(self, response):
        l = ItemLoader(item=SpieItem())
        l.add_value('name', response.meta['name'])
        try:
            definition = response.css('header[class="header-solid small"] h1').xpath('text()').extract()[0]
            definition = definition.replace(',', '').replace(';', '').replace('.', '').replace('"', '').replace("'", '')
            l.add_value('definition', definition)
        except Exception:
            pass
        try:
            l.add_value('synonyms', ','.join(response.css('div[class="content-container"] h2 + ul')[0].xpath('li/p').xpath('text()').extract()))
        except Exception:
            pass
        yield l.load_item()



