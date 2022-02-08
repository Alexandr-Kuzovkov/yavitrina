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


class VitrinaSpider(scrapy.Spider):
    name = 'category_parser'
    #allowed_domains = ['yavitrina.ru', 'i.yavitrina.ru']
    allowed_domains = []
    dirname = 'vitrina'
    deltafetch_dir = 'deltafetch'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    drain = False
    pagination = {}
    db = None
    base_url = 'https://yavitrina.ru'
    local_url = 'http://localhost:6800'
    lua_src = pkgutil.get_data('yavitrina', 'lua/html-render.lua')
    lua_src2 = pkgutil.get_data('yavitrina', 'lua/html-render-scrolldown.lua')
    clear_db = False
    paginations = {}
    scrapestack_access_key = ''
    product_request_type = 'headless'
    request_type = 'origin'
    #product_request_type = 'selenium'
    use_scrapestack = True
    lostonly = False
    custom_settings = {
        'SELENIUM_GRID_URL': 'http://selenium-hub:4444/wd/hub',  # Example for local grid with docker-compose
        'SELENIUM_NODES': 1,  # Number of nodes(browsers) you are running on your grid
        'SELENIUM_CAPABILITIES': DesiredCapabilities.CHROME,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_headless.HeadlessDownloadHandler",
            "https": "scrapy_headless.HeadlessDownloadHandler",
        },
        'SPIDER_MIDDLEWARES': {
            'scrapy_splash.SplashDeduplicateArgsMiddleware': 100,
            'scrapy.downloadermiddlewares.redirect.RedirectMiddleware': 101,
            'scrapy_deltafetch.DeltaFetch': 102,
        },
        'DELTAFETCH_ENABLED': True,
        'DELTAFETCH_DIR': '/scrapy/yavitrina/files/vitrina/deltafetch',
        'DELTAFETCH_RESET': False,

        # 'LOG_LEVEL': 'DEBUG'
    }
    # reset visited history
    # scrapy crawl category_parser -a deltafetch_reset=1

    def __init__(self, drain=False, noproxy=False, request_type=False, product_request_type=False, lostonly=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if noproxy:
            self.use_scrapestack = False
        if lostonly:
            self.lostonly = True
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
        if self.lostonly:
            self.logger.info('!!!Parse lost only categories...')
            request = self.getRequest(self.local_url, self.parse_lost_category)
            yield request
        else:
            request = self.getRequest(self.base_url, self.parse_top_category)
            request.meta['url'] = self.base_url
            yield request

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
            description = ' '.join(response.css('div[class="seo-text"] div[class="text"]').extract())
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('html', response.text)
            if len(description) > 0:
                l.add_value('description', description)
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
        requested_url = self.get_request_url(response)
        result_blocks = response.css('div[class="hub-category"] div[class="item hub-item"]').extract()
        for html in result_blocks:
            body = html.encode('utf-8')
            block = response.replace(body=body)
            url = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('@href').extract())
            title = ' '.join(block.css('div[class="holder"] a[class="name"]').xpath('text()').extract())
            img = ' '.join(block.css('span[class="icon"] img').xpath('@src').extract())
            description = ' '.join(response.css('div[class="seo-text"] div[class="text"]').extract())
            parent = response.meta['parent']
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', url)
            l.add_value('title', title)
            l.add_value('img', img)
            l.add_value('parent', parent)
            l.add_value('html', response.text)
            if len(description) > 0:
                l.add_value('description', description)
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_card_page)
            request.meta['parent'] = url
            yield request
            img_link = ''.join([self.base_url, img])
            request_img = self.getRequest(img_link, self.download_image)
            request_img.meta['filename'] = img_link.split('/').pop()
            request_img.meta['category_url'] = url
            yield request_img

    # parse card page (ex. Каталог -> Одежда и обувь -> Обувь)
    def parse_card_page(self, response):
        requested_url = self.get_request_url(response)
        page_uri = self.get_uri(requested_url)
        # checking is it card page
        card_blocks = response.css('div[class="products-list"] div.p-card').extract()
        if len(card_blocks) == 0:
            self.logger.info('url: {url} is not card page'.format(url=requested_url))
        else:
            # tags walk
            tags_blocks = response.css('div[class="category-tags"] div a').extract()
            for html in tags_blocks:
                body = html.encode('utf-8')
                block = response.replace(body=body)
                url = ' '.join(block.css('a').xpath('@href').extract())
                link = ''.join([self.base_url, url])
                request = self.getRequest(link, self.parse_card_page)
                yield request
            # side categories walk
            cat_blocks = response.css('div[class="aside"]').xpath(u"//span[text() = 'Категории']/parent::div/following-sibling::div/ul/li").extract()
            for html in cat_blocks:
                body = html.encode('utf-8')
                block = response.replace(body=body)
                url = ' '.join(block.css('li a').xpath('@href').extract())
                link = ''.join([self.base_url, url])
                request = self.getRequest(link, self.parse_card_page)
                yield request
            # save current page category
            html = ''.join(response.css('h1[class="p-title"]').extract())
            title = ''.join(response.css('h1[class="p-title"]').xpath('text()').extract())
            parent = ''.join(response.css('div[class="breadcrumbs"] a[class="breadcrumbs-link"]').xpath('@href').extract()[-1:])
            if len(parent) < 2:
                parent = None
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', page_uri)
            l.add_value('title', title)
            l.add_value('parent', parent)
            l.add_value('html', html)
            yield l.load_item()
            request = scrapy.Request(self.local_url, callback=self.parse_lost_category)
            yield request

    def parse_card_page_category_only(self, response):
        requested_url = self.get_request_url(response)
        page_uri = self.get_uri(requested_url)
        # save current page category
        html = ''.join(response.css('h1[class="p-title"]').extract())
        title = ''.join(response.css('h1[class="p-title"]').xpath('text()').extract())
        parent = ''.join(response.css('div[class="breadcrumbs"] a[class="breadcrumbs-link"]').xpath('@href').extract()[-1:])
        if len(parent) < 2:
            parent = None
        if len(title) > 0:
            l = ItemLoader(item=CategoryItem(), response=response)
            l.add_value('url', page_uri)
            l.add_value('title', title)
            l.add_value('parent', parent)
            l.add_value('html', html)
            yield l.load_item()

    def parse_lost_category(self, response):
        sql = """SELECT count(*) AS total FROM
                (SELECT c.title, c.url, c.parent_id, c.parent_url AS parent, pc.title AS parent_tilte,
                 pc.url AS parent_url, pc.id AS parent_category_id
                FROM category c LEFT JOIN category pc
                 ON c.parent_url=pc.url
                WHERE c.parent_id isnull) t1 WHERE parent_category_id isnull"""
        total = self.db._getone(sql)
        self.logger.info('total items %s' % total)
        LIMIT = 100
        offsets = range(0, total, LIMIT)
        for offset in offsets:
            self.logger.info('handle items %s-%s' % (offset, min(offset + LIMIT, total)))
            sql = """SELECT parent FROM
                    (SELECT c.title, c.url, c.parent_id, c.parent_url AS parent, pc.title AS parent_tilte,
                     pc.url AS parent_url, pc.id AS parent_category_id
                    FROM category c LEFT JOIN category pc
                     ON c.parent_url=pc.url
                    WHERE c.parent_id isnull) t1 WHERE parent_category_id isnull
                    ORDER BY parent OFFSET {offset} LIMIT {limit}""".format(offset=offset, limit=LIMIT)
            items = self.db._getraw(sql, ['parent'])
            items = filter(lambda i: i['parent'] is not None and len(i['parent']) > 1, items)
            # pprint(len(items))
            for item in items:
                url = ''.join([self.base_url, item['parent']])
                # pprint(url)
                request = self.getRequest(url, callback=self.parse_card_page_category_only, request_type='origin')
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
        
    def get_uri(self, url):
        return url.replace(self.base_url, '').split('?')[0]















