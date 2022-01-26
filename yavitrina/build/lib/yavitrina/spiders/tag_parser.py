# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
import time
import html2text
import datetime
import logging
import urllib
from yavitrina.items import CategoryItem
from yavitrina.items import CategoryDescriptionItem
from yavitrina.items import TagItem
from yavitrina.items import ProductCardItem
from yavitrina.items import ProductItem
from yavitrina.items import ImageItem
from yavitrina.items import SearchTagItem
from yavitrina.items import CategoryTagItem
from yavitrina.items import SettingItem
from yavitrina.items import SettingValueItem
from yavitrina.scrapestack import ScrapestackRequest
from yavitrina.seleniumrequest import SelenuimRequest
from scrapy_headless import HeadlessRequest
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib import urlencode

###############################################################################
# Parsing tags to fill target_title field (tilte odf page where tag is linked)
###############################################################################

class TagParserSpider(scrapy.Spider):
    name = 'tag_parser'
    #allowed_domains = ['yavitrina.ru', 'i.yavitrina.ru']
    allowed_domains = []
    dirname = 'tag_parser'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    drain = False
    pagination = {}
    base_url = 'https://yavitrina.ru'
    lua_src = pkgutil.get_data('yavitrina', 'lua/html-render.lua')
    lua_src2 = pkgutil.get_data('yavitrina', 'lua/html-render-scrolldown.lua')
    clear_db = False
    db = None
    paginations = {}
    scrapestack_access_key = ''
    product_request_type = 'headless'
    request_type = 'splash'
    #product_request_type = 'selenium'
    use_scrapestack = True
    custom_settings = {
        'SELENIUM_GRID_URL': 'http://selenium-hub:4444/wd/hub',  # Example for local grid with docker-compose
        'SELENIUM_NODES': 1,  # Number of nodes(browsers) you are running on your grid
        'SELENIUM_CAPABILITIES': DesiredCapabilities.CHROME,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_headless.HeadlessDownloadHandler",
            "https": "scrapy_headless.HeadlessDownloadHandler",
        },
        #'LOG_LEVEL': 'DEBUG',
        'LOG_LEVEL': 'INFO',
    }

    def __init__(self, noproxy=False, request_type=False, product_request_type=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if noproxy:
            self.use_scrapestack = False
        if request_type:
            self.request_type = request_type
        if product_request_type:
            self.product_request_type = product_request_type

    def getRequest(self, url, callback, request_type=None, dont_filter=False):
        url_origin = url
        if request_type is None:
            request_type = self.request_type
        if self.use_scrapestack:
            params = {'access_key': self.scrapestack_access_key, 'url': url}
            url = 'http://api.scrapestack.com/scrape?' + urlencode(params)
        if request_type == 'headless':
            request = HeadlessRequest(url, callback=callback, driver_callback=self.process_webdriver)
        elif request_type == 'selenium':
            request = SelenuimRequest(url, callback=callback, dont_filter=dont_filter, options={'minsize': 2028, 'wait': 2})
        elif request_type == 'scrapestack':
            url = url_origin
            request = ScrapestackRequest(url, callback=callback, access_key=self.scrapestack_access_key, dont_filter=dont_filter, options={'render_js': 1})
        elif request_type == 'splash':
            args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
            request = SplashRequest(url, callback=callback, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=dont_filter)
            request.meta['url_origin'] = url_origin
        else:
            request = scrapy.Request(url, callback=callback, dont_filter=dont_filter)
        if self.use_scrapestack:
            request.meta['url_origin'] = url_origin
        return request

    def start_requests(self):
        url = 'http://localhost:6800'
        request = scrapy.Request(url, callback=self.handle_tags)
        yield request

    def handle_tags(self, response):
        table = 'tag'
        total_tags = self.db.get_items_total(table, 'target_title ISNULL')
        self.logger.info('total_tags; {total}'.format(total=total_tags))
        LIMIT = 100
        offsets = range(0, total_tags, LIMIT)
        for offset in offsets:
            self.logger.debug('handle tags %s-%s' % (offset, min(offset+LIMIT, total_tags)))
            tags = self.db.get_items_chunk(table, 'target_title ISNULL', offset, LIMIT)
            tags = filter(lambda i: len(i) > 1, tags)
            for tag in tags:
                #pprint(tag['url'])
                url = ''.join([self.base_url, tag['url']])
                request = self.getRequest(url, callback=self.handle_target_page, request_type='origin')
                request.meta['tag'] = tag
                yield request

    def handle_target_page(self, response):
        h1 = ''.join(response.css('h1[class="p-title"]').xpath('text()').extract())
        tag = response.meta['tag']
        if len(h1) > 0:
            l = ItemLoader(item=TagItem(), response=response)
            l.add_value('id', tag['id'])
            l.add_value('target_title', h1)
            yield l.load_item()

    def process_webdriver(self, driver):
        IMPLICITLY_WAIT = 3
        driver.implicitly_wait(IMPLICITLY_WAIT)
        time.sleep(IMPLICITLY_WAIT)


    def get_request_url(self, response):
        if 'url_origin' in response.meta:
            return response.meta['url_origin']
        elif hasattr(response.request, 'url_origin'):
            return response.request.url_origin
        else:
            return response.request.url















