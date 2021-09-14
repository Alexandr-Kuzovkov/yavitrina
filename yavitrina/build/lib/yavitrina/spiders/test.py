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
import urlparse
import requests
from yavitrina.items import CategoryItem
from yavitrina.items import TagItem
from yavitrina.items import ProductCardItem
from yavitrina.items import ProductItem
from yavitrina.items import ImageItem


class TestSpider(scrapy.Spider):
    name = 'test'
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
        url = 'https://yavitrina.ru/product/674779192'
        request = self.getRequest(url, self.parse_product_page)
        yield request

    def parse_product_page(self, response):
        title = ' '.join(response.css('div[class="product-page"] div[class="p-info"] h1').xpath('text()').extract())
        description = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="desc"] p[class="d-text"]').xpath('text()').extract())
        price = int(' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="info-detail"] span[itemprop="price"]').xpath('@content').extract()))
        shop_link = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="btn-box"] a[class="btn btn-in-shops"]').xpath('@href').extract())
        shop_link2 = ' '.join(response.css('div[class="product_tabs"] section[id="content1"] a').xpath('@href').extract())
        parameters = response.css('div[class="product_tabs"] section[id="content2"] article span').xpath('text()').extract()
        feedbacks = '##@@@!!!'.join(response.css('div[class="product_tabs"] section[id="content3"] article div[class="_1qMiEXz _17VRAZ_"]').extract())
        product_id = response.url.split('/').pop()
        categories = response.css('div[class="b-top"] li[class="breadcrumbs-item"] a').xpath('@href').extract()
        parsed = urlparse.urlparse(shop_link)
        try:
            vid = urlparse.parse_qs(parsed.query)['vid'][0]
            pprint(vid)
            spec_url = 'https://aflt.market.yandex.ru/widget/multi/api/initByType/specifications?themeId=1&specificationGroups=3&vid={vid}&metrikaCounterId=43180609'.format(vid=vid)
            #request = self.getRequest(spec_url, self.handle_spec)
            #yield request
            pprint(spec_url)
            res = requests.get(spec_url, headers={'user-agent': self.settings.get('USER_AGENT'), 'referer': 'https://yavitrina.ru/product/674779192', 'origin': 'https://yavitrina.ru'})
            pprint(res.text)
        except Exception:
            self.logger.info('parse vid error')
        l = ItemLoader(item=ProductItem(), response=response)
        l.add_value('product_id', product_id)
        l.add_value('html', response.text)
        l.add_value('url', response.url)
        l.add_value('title', title)
        l.add_value('description', description)
        l.add_value('price', price)
        l.add_value('shop_link', shop_link)
        l.add_value('shop_link2', shop_link2)
        l.add_value('parameters', parameters)
        l.add_value('feedbacks', feedbacks)
        for category in categories:
            l.add_value('category', category)
        yield l.load_item()
        #save images
        image_urls = response.css('div[class="photos"] img').xpath('@src').extract()
        for link in image_urls:
            pprint(link)
            request = scrapy.Request(link, self.download_image)
            request.meta['product_id'] = product_id
            request.meta['filename'] = '-'.join(link.split('/')[-3:])
            request.meta['autotype'] = True
            yield request


    def download_image(self, response):
        Item = ImageItem()
        if 'product_id' in response.meta:
            Item['product_id'] = response.meta['product_id']
        if 'category_url' in response.meta:
            Item['category_url'] = response.meta['category_url']
        if 'autotype' in response.meta and response.meta['autotype']:
            filename = response.meta['filename']
            ext = response.headers['Content-Type'].split('/').pop()
            Item['filename'] = '.'.join([filename, ext])
        else:
            Item['filename'] = response.meta['filename']
        Item['data'] = response.body
        Item['url'] = response.url
        yield Item

    def handle_spec(self, response):
        pprint('!!!handle_spec')
        pprint(response.text)















