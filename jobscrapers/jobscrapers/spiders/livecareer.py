# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import IndeedItem
from jobscrapers.items import categories
import time
import pkgutil
from transliterate import translit
from scrapy_splash import SplashRequest
from scrapy_splash import SplashFormRequest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from random import randint
import requests
import os

class LivecareerSpider(scrapy.Spider):

    name = "livecareer"
    publisher = "Livecareer"
    publisherurl = 'https://www.livecareer.com'
    dirname = 'livecareer'
    limit = False
    drain = False
    lang = None
    use_selenium = False
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    env_content = pkgutil.get_data('jobscrapers', 'data/.env')
    rundebug = False

    def __init__(self, limit=False, drain=False, dirname=False, lang=None, debug=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if dirname:
            self.dirname = str(dirname)
        self.lang = lang
        if debug:
            self.rundebug = True
        #if use_selenium:
        #    self.use_selenium = True

    def start_requests(self):
        allowed_domains = ["https://www.livecareer.com"]
        if not self.rundebug:
            url = 'https://www.livecareer.com/resume-search/search?jt=*'
            request = scrapy.Request(url, callback=self.get_cv_list)
            request.meta['page'] = 1
            yield request
        else:
            self.logger.info('Debug run!!!')
            self.debug()

    def get_cv_list(self, response):
        links = response.css('ul[class="resume-list list-unstyled"] li a').xpath('@href').extract()
        for link in links:
            url = ''.join(['https://www.livecareer.com', link])
            request = scrapy.Request(url, callback=self.get_cv)
            request.meta['name'] = url.split('/').pop()
            yield request
        page = response.meta['page']
        pagination = response.css('ul[class="pagination"] li a').xpath('@onclick').extract()
        max_page = max(map(lambda i: int(i.replace('asyncNavigateTo(', '').replace(')', '')), pagination))
        if page < max_page:
            page += 1
            url = 'https://www.livecareer.com/resume-search/search?jt=*&pg={page}'.format(page=page)
            request = scrapy.Request(url, callback=self.get_cv_list)
            request.meta['page'] = page
            yield request

    def get_cv(self, response):
        l = ItemLoader(item=IndeedItem())
        l.add_value('name', response.meta['name'])
        html = ' '.join(response.css('div[id="document"]').extract() + response.css('div[class="content-box mt20"]').extract())
        l.add_value('text', html)
        yield l.load_item()

    def debug(self):
        self.logger.info('debug...')




