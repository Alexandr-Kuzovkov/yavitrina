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
    dirname = 'test'
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
        #url = 'https://yavitrina.ru/product/674779192'
        #request = self.getRequest(url, self.parse_product_page)
        url = 'https://yavitrina.ru/shampuni'
        #url = 'https://yavitrina.ru/verhnyaya-odezhda-dlya-malyshey'
        request = self.getRequest(url, self.parse_sub_category3)
        yield request

    def parse_product_page(self, response):
        title = ' '.join(response.css('div[class="product-page"] div[class="p-info"] h1').xpath('text()').extract())
        description = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="desc"] p[class="d-text"]').xpath('text()').extract())
        try:
            price = int(' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="info-detail"] span[itemprop="price"]').xpath('@content').extract()))
        except Exception as ex:
            price = None
        shop_link = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="btn-box"] a[class="btn btn-in-shops"]').xpath('@href').extract())
        shop_link2 = ' '.join(response.css('div[class="product_tabs"] section[id="content1"] a').xpath('@href').extract())
        parameters = response.css('div[class="product_tabs"] section[id="content2"] article span').xpath('text()').extract()
        feedbacks = '##@@@!!!'.join(response.css('div[class="product_tabs"] section[id="content3"] article div[class="_1qMiEXz _17VRAZ_"]').extract())
        product_id = response.url.split('/').pop()
        categories = response.css('div[class="b-top"] li[class="breadcrumbs-item"] a').xpath('@href').extract()
        l = ItemLoader(item=ProductItem(), response=response)
        l.add_value('product_id', product_id)
        l.add_value('html', response.text)
        l.add_value('url', response.url)
        l.add_value('title', title)
        l.add_value('description', description)
        l.add_value('price', price)
        l.add_value('shop_link', shop_link)
        if 'ymarket_link' in response.meta and len(response.meta['ymarket_link']) > 0:
            l.add_value('shop_link2', response.meta['ymarket_link'])
        l.add_value('parameters', parameters)
        l.add_value('feedbacks', feedbacks)
        if len(categories) > 0:
            for category in categories:
                l.add_value('category', category)
                break
        elif 'category' in response.meta:
            l.add_value('category', response.meta['category'])
        yield l.load_item()
        #save images
        image_urls = response.css('div[class="photos"] img').xpath('@src').extract()
        for link in image_urls:
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

    # parse subcategory3 (ex. Каталог -> Одежда и обувь -> Обувь -> Сандалии)
    def parse_sub_category3(self, response):
        # save tags
        tags_blocks = response.css('div[class="category-tags"] div a').extract()
        for html in tags_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('a').xpath('@href').extract())
            title = ' '.join(block.css('a').xpath('text()').extract())
            l = ItemLoader(item=TagItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('page', self.get_uri(response.url))
            l.add_value('html', block.text)
            yield l.load_item()
        # save product cards
        card_blocks = response.css('div[class="products-list"] div.p-card').extract()
        self.logger.info('parse_sub_category3: url: {url}; {count} products fetched'.format(url=response.url, count=len(card_blocks)))
        if len(card_blocks) == 0:
            message = ''.join(response.css('div[class="cat-text-null"] b').xpath('text()').extract())
            if len(response.text) < 200:
                self.logger.info('Response text: {text}'.format(text=response.text))
            self.logger.info('message: "{message}"; http_code: {code}'.format(message=message.encode('utf-8'), code=response.status))
        for html in card_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            img = ' '.join(block.css('img[class="gaclkimg"]').xpath('@src').extract())
            url = ' '.join(block.css('a[class="b-info"]').xpath('@href').extract())
            id = url.split('/').pop()
            title = ' '.join(
                block.css('div[class="name"] p[class="datalink clck gaclkname"]').xpath('text()').extract())
            price = int(
                ' '.join(block.css('span[class="price"] strong[itemprop="price"]').xpath('text()').extract()).replace(
                    ' ', '').replace('.', '').encode('ascii', 'ignore'))
            l = ItemLoader(item=ProductCardItem(), response=response)
            l.add_value('img', img)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('price', price)
            l.add_value('product_id', id)
            l.add_value('page', self.get_uri(response.url))
            l.add_value('html', block.text)
            ymarket_link = ''.join(block.css('div[class="price-in-shops"] span').xpath('@data-link').extract())
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_product_page)
            request.meta['parent'] = url
            request.meta['category'] = self.get_uri(response.url)
            request.meta['ymarket_link'] = ymarket_link
            yield request
            if img.startswith('//'):
                img = 'https:{img}'.format(img=img)
            try:
                request_img = scrapy.Request(img, self.download_image)
            except Exception as ex:
                self.logger.warning('Error load image from {url}'.format(url=img))
            request_img.meta['filename'] = '-'.join(img.split('/')[-3:])
            request_img.meta['product_id'] = id
            request_img.meta['autotype'] = True
            yield request_img
        # handle pagination
        self.handle_pagination(response, self.parse_sub_category3, len(card_blocks))

    def get_uri(self, url):
        return url.replace(self.base_url, '').split('?')[0]

    def handle_pagination(self, response, callback, default_count=72):
        self.logger.info('handle pagination from url: {url}'.format(url=response.url))
        try:
            count = int(''.join(
                response.css('div[class="p-title__wrap"] h1[class="p-title"] + span[class="p-title__text"]').xpath(
                    'text()').extract()).encode('ascii', 'ignore').replace(' ', ''))
        except Exception as ex:
            self.logger.warning('Error while get count products')
            self.logger.warning(ex)
            count = default_count
        self.logger.info('handle pagination: count={count}'.format(count=count))
        count_pages = math.ceil(count / 72.0)
        self.logger.info('handle pagination: count_pages={count_pages}'.format(count=count))
        category_uri = self.get_uri(response.url)
        if count_pages > 1:
            if category_uri not in self.pagination:
                page = 2
                self.pagination[category_uri] = page
            else:
                self.pagination[category_uri] += 1
                page = self.pagination[category_uri]
            url = '{base_url}{category_uri}?page={page}&sort=position-asc'.format(base_url=self.base_url,
                                                                                  category_uri=category_uri,
                                                                                  page=page)
            self.logger.info('page {page} for category {category}'.format(page=page, category=category_uri))
            request = self.getRequest(url, callback=callback)
            request.meta['parent'] = response.meta['parent']
            yield request
















