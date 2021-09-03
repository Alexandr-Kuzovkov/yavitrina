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
from yavitrina.items import TagItem
from yavitrina.items import ProductCardItem
from yavitrina.items import ProductItem
from yavitrina.items import ImageItem


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
    clear_db = False

    def __init__(self, drain=False, noproxy=False, cleardb=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if noproxy:
            self.use_splash = False
        if cleardb:
            self.clear_db = True

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
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            if len(img) == 0:
                img = ' '.join(block.css('span[class="icon"]').extract())
                img = img[img.find('xlink:href') + 12:img.find('</use>') - 2]
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('html', response.text)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category)
            request.meta['parent'] = url
            yield request
            last = img.split('/').pop()
            filename = last[:last.find('#')]
            img_link = ''.join([self.base_url, img])
            request = self.getRequest(img_link, self.download_image)
            request.meta['filename'] = filename
            request.meta['category_url'] = url
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
            l.add_value('html', response.text)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category2)
            request.meta['parent'] = url
            yield request
            img_link = ''.join([self.base_url, img])
            request_img = self.getRequest(img_link, self.download_image)
            request_img.meta['filename'] = img_link.split('/').pop()
            request_img.meta['category_url'] = url
            yield request_img


    # parse subcategory2 (ex. Каталог -> Одежда и обувь -> Обувь)
    def parse_sub_category2(self, response):
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
            l.add_value('page', response.url.replace(self.base_url, ''))
            l.add_value('html', block.text)
            yield l.load_item()
        #save categories
        cat_blocks = response.css('div[class="aside"]').xpath(u"//span[text() = 'Категории']/parent::div/following-sibling::div/ul/li").extract()
        for html in cat_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('li a').xpath('@href').extract())
            title = ' '.join(block.css('li a').xpath('text()').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('parent', parent)
            l.add_value('html', block.text)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_sub_category3)
            request.meta['parent'] = url
            yield request
        #save product cards
        card_blocks = response.css('div[class="products-list"] div.p-card').extract()
        for html in card_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            img = ' '.join(block.css('img[class="gaclkimg"]').xpath('@src').extract())
            url = ' '.join(block.css('a[class="b-info"]').xpath('@href').extract())
            id = url.split('/').pop()
            title = ' '.join(block.css('div[class="name"] p[class="datalink clck gaclkname"]').xpath('text()').extract())
            price = int(' '.join(block.css('span[class="price"] strong[itemprop="price"]').xpath('text()').extract()).replace(' ', '').replace('.', '').encode('ascii','ignore'))
            l = ItemLoader(item=ProductCardItem(), response=response)
            l.add_value('img', img)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('price', price)
            l.add_value('product_id', id)
            l.add_value('page', response.url.replace(self.base_url, ''))
            l.add_value('html', block.text)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_product_page)
            request.meta['parent'] = url
            request.meta['category'] = response.url.replace(self.base_url, '')
            yield request
            if img.startswith('//'):
                img = 'https:{img}'.format(img=img)
            request_img = scrapy.Request(img, self.download_image)
            request_img.meta['filename'] = '-'.join(img.split('/')[-3:])
            request_img.meta['product_id'] = id
            request_img.meta['autotype'] = True
            yield request_img


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
            l.add_value('page', response.url.replace(self.base_url, ''))
            l.add_value('html', block.text)
            yield l.load_item()
        # save product cards
        card_blocks = response.css('div[class="products-list"] div.p-card').extract()
        for html in card_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            img = ' '.join(block.css('img[class="gaclkimg"]').xpath('@src').extract())
            url = ' '.join(block.css('a[class="b-info"]').xpath('@href').extract())
            id = url.split('/').pop()
            title = ' '.join(block.css('div[class="name"] p[class="datalink clck gaclkname"]').xpath('text()').extract())
            price = int(' '.join(block.css('span[class="price"] strong[itemprop="price"]').xpath('text()').extract()).replace(' ', '').replace('.', '').encode('ascii', 'ignore'))
            l = ItemLoader(item=ProductCardItem(), response=response)
            l.add_value('img', img)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('price', price)
            l.add_value('product_id', id)
            l.add_value('page', response.url.replace(self.base_url, ''))
            l.add_value('html', block.text)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_product_page)
            request.meta['parent'] = url
            request.meta['category'] = response.url.replace(self.base_url, '')
            yield request
            if img.startswith('//'):
                img = 'https:{img}'.format(img=img)
            request_img = scrapy.Request(img, self.download_image)
            request_img.meta['filename'] = '-'.join(img.split('/')[-3:])
            request_img.meta['product_id'] = id
            request_img.meta['autotype'] = True
            yield request_img

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













