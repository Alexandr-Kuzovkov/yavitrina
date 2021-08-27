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


class VitrinaSpider(scrapy.Spider):
    name = 'vitrina'
    allowed_domains = ['yavitrina.ru', 'i.yavitrina.ru']
    dirname = 'vitrina'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    drain = False
    pagination = {}
    use_splash = True
    es_exporter = None
    base_url = 'https://yavitrina.ru'
    lua_src = pkgutil.get_data('yavitrina', 'lua/html-render.lua')

    def __init__(self, drain=False, noproxy=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if noproxy:
            self.use_splash = False

    def getRequest(self, url, callback):
        if self.use_splash:
            args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
            request = SplashRequest(url, callback=callback, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
        else:
            request = scrapy.Request(url, callback=callback, dont_filter=True)
        return request

    def start_requests(self):
        request = self.getRequest(self.base_url, self.parse_top_category)
        request.meta['url'] = self.base_url
        yield request

    # parse index page
    def parse_top_category(self, response):
        result_blocks = response.css('div[class="category-list"] div[class="list"] a').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('a').xpath('@href').extract())
            title = ' '.join(block.css('span[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"]').extract())
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category)
            request.meta['parent'] = url
            yield request

    # parse subcategory (ex. Каталог -> Одежда и обувь)
    def parse_sub_category(self, response):
        result_blocks = response.css('div[class="hub-category"] div[class="item hub-item"]').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('@href').extract())
            title = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('parent', parent)
            yield l.load_item()
            #link = ''.join([self.base_url, url])
            #request = self.getRequest(link, self.parse_sub_category2)
            #request.meta['parent'] = url
            #yield request


    # parse subcategory2 (ex. Каталог -> Одежда и обувь -> Обувь)
    def parse_sub_category2(self, response):
        result_blocks = response.css('div[class="hub-category"] div[class="item hub-item"]').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('@href').extract())
            title = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('parent', parent)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category3)
            request.meta['parent'] = url
            yield request


    # parse subcategory3 (ex. Каталог -> Одежда и обувь -> Обувь -> Женская обувь)
    def parse_sub_category3(self, response):
        result_blocks = response.css('div[class="hub-category"] div[class="item hub-item"]').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('@href').extract())
            title = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('parent', parent)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category3)
            request.meta['parent'] = url
            yield request


    # parse subcategory4 (ex. Каталог -> Одежда и обувь -> Обувь -> Женская обувь -> Сандалии)
    def parse_sub_category4(self, response):
        result_blocks = response.css('div[class="hub-category"] div[class="item hub-item"]').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('@href').extract())
            title = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('parent', parent)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category3)
            request.meta['parent'] = url
            yield request











