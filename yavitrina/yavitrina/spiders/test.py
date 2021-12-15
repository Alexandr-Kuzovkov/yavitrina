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
from yavitrina.items import SettingItem
from yavitrina.items import SettingValueItem
from yavitrina.scrapestack import ScrapestackRequest
from yavitrina.seleniumrequest import SelenuimRequest
from scrapy_headless import HeadlessRequest
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib import urlencode



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
    scrapestack_access_key = ''
    db = None

    custom_settings = {
        #'LOG_LEVEL': 'DEBUG',
        'LOG_LEVEL': 'INFO',
        'SELENIUM_GRID_URL': 'http://selenium-hub:4444/wd/hub',  # Example for local grid with docker-compose
        'SELENIUM_NODES': 1,  # Number of nodes(browsers) you are running on your grid
        'SELENIUM_CAPABILITIES': DesiredCapabilities.CHROME,
        'DOWNLOAD_HANDLERS': {
            "http": "scrapy_headless.HeadlessDownloadHandler",
            "https": "scrapy_headless.HeadlessDownloadHandler",
        }
    }

    def __init__(self, drain=False, noproxy=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if noproxy:
            self.use_splash = False

    def getRequest(self, url, callback, request_type='splash', dont_filter=False, use_scrapestack=False):
        url_origin = url
        if use_scrapestack:
            params = {'access_key': self.scrapestack_access_key, 'url': url}
            url = 'http://api.scrapestack.com/scrape?' + urlencode(params)
        if request_type == 'headless':
            request = HeadlessRequest(url, callback=callback, driver_callback=self.process_webdriver)
        elif request_type == 'selenium':
            request = SelenuimRequest(url, callback=callback, dont_filter=dont_filter, options={'minsize': 2, 'wait': 2})
        elif request_type == 'scrapestack':
            url = url_origin
            request = ScrapestackRequest(url, callback=callback, access_key=self.scrapestack_access_key, dont_filter=dont_filter, options={'render_js': 1})
        elif request_type == 'splash':
            args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
            request = SplashRequest(url, callback=callback, endpoint='execute', args=args, meta={"handle_httpstatus_all": True}, dont_filter=dont_filter)
            request.meta['url_origin'] = url_origin
        else:
            request = scrapy.Request(url, callback=callback, dont_filter=dont_filter)
        if use_scrapestack:
            request.meta['url_origin'] = url_origin
        return request

    def process_webdriver(self, driver):
        IMPLICITLY_WAIT = 3
        driver.implicitly_wait(IMPLICITLY_WAIT)
        time.sleep(IMPLICITLY_WAIT)
        #print(driver.page_source)

    def start_requests(self):
        #url = 'https://yavitrina.ru/product/674779192'
        url = 'https://yavitrina.ru/product/677731028'
        #url = 'https://yavitrina.ru/product/382715171'
        #url = 'https://yavitrina.ru/product/822105004'
        #url = 'https://yavitrina.ru/product/14008662'
        #url = 'https://yavitrina.ru/zhenskaya-obuv'
        #url = 'https://hub.kuzovkov12.ru:8001/googleapi'
        #DOCKER_HOST_IP = os.popen("ip ro | grep default | cut -d' ' -f 3").read().strip()
        #url = 'http://{DOCKER_HOST_IP}:8002/googleapi'.format(DOCKER_HOST_IP=DOCKER_HOST_IP)
        #request = self.getRequest(url, self.parse_product_page, request_type='scrapestack')
        #request = self.getRequest(url, self.parse_product_page, request_type='selenium', use_scrapestack=True)
        #request = self.getRequest(url, self.test_saving_filters, request_type='origin', use_scrapestack=False)
        #request = self.getRequest(url, self.parse_product_page, request_type='splash', use_scrapestack=True)
        #request = self.getRequest(url, self.parse_product_page, request_type='headless', use_scrapestack=True)

        #request = self.getRequest(url, self.parse_product_page)
        #url = 'https://yavitrina.ru/shampuni'
        #url = 'https://yavitrina.ru/verhnyaya-odezhda-dlya-malyshey'
        #request = self.getRequest(url, self.parse_sub_category3)
        #request.meta['parent'] = url
        #yield request

        #update feedbacks
        sql = "SELECT count(*) as total FROM product WHERE jsonb_array_length(feedbacks) > 0"
        total = self.db._getone(sql)
        pprint('total: {total}'.format(total=total))
        LIMIT = 100
        offsets = range(0, total, LIMIT)
        for offset in offsets:
            sql = "SELECT title, url, product_id, feedbacks FROM product WHERE jsonb_array_length(feedbacks) > 0  ORDER BY id OFFSET {offset} LIMIT {limit}".format(offset=offset, limit=LIMIT)
            products = self.db._getraw(sql, ['title', 'url', 'product_id', 'feedbacks'])
            for product in products:
                url = product['url']
                request = self.getRequest(url, self.parse_product_page, request_type='headless', use_scrapestack=True)
                request.meta['parent'] = url
                request.meta['category'] = product['category']
                yield request


    def parse_product_page(self, response):
        #pprint(response.text)
        requested_url = self.get_request_url(response)
        title = ' '.join(response.css('div[class="product-page"] div[class="p-info"] h1').xpath('text()').extract())
        description = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="desc"] p[class="d-text"]').xpath('text()').extract())
        try:
            price = int(' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="info-detail"] span[itemprop="price"]').xpath('@content').extract()))
        except Exception as ex:
            price = None
        shop_link = ' '.join(response.css('div[class="product-page"] div[class="p-info"] div[class="btn-box"] a[class="btn btn-in-shops"]').xpath('@href').extract())
        shop_link2 = ' '.join(response.css('div[class="product_tabs"] section[id="content1"] a').xpath('@href').extract())
        categories = response.css('div[class="b-top"] li[class="breadcrumbs-item"] a').xpath('@href').extract()
        l = ItemLoader(item=ProductItem(), response=response)
        product_id = requested_url.split('/').pop()
        l.add_value('product_id', product_id)
        l.add_value('html', response.text)
        l.add_value('url', requested_url)
        l.add_value('title', title)
        l.add_value('description', description)
        l.add_value('price', price)
        l.add_value('shop_link', shop_link)
        if 'ymarket_link' in response.meta and len(response.meta['ymarket_link']) > 0:
            l.add_value('shop_link2', response.meta['ymarket_link'])
        if 'rate' in response.meta and len(response.meta['rate']) > 0:
            l.add_value('rate', response.meta['rate'])
        if 'colors' in response.meta and len(response.meta['colors']) > 0:
            l.add_value('colors', response.meta['colors'])
        parameters = self.parse_parameters(response)
        l.add_value('parameters', parameters)
        feedbacks = self.parse_feedbacks(response)
        l.add_value('feedbacks', feedbacks)
        if len(categories) > 0:
            for category in categories:
                l.add_value('category', category)
                break
        elif 'category' in response.meta:
            l.add_value('category', response.meta['category'])
        related_products = ','.join(map(lambda i: i.split('/').pop(), response.css('div[class="related-products"] div[class="b-info-wrap"] a').xpath('@href').extract()))
        if len(related_products) > 0:
            l.add_value('related_products', related_products)
        yield l.load_item()
        #save images
        # image_urls = response.css('div[class="photos"] img').xpath('@src').extract()
        # for link in image_urls:
        #     if link.startswith('//') or (not link.startswith('https:')):
        #         link = 'https:{img}'.format(img=link)
        #     request = scrapy.Request(link, self.download_image)
        #     request.meta['product_id'] = product_id
        #     request.meta['filename'] = '-'.join(link.split('/')[-3:])
        #     request.meta['autotype'] = True
        #     yield request


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
        Item['data'] = 'image data' #response.body
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
            #l.add_value('html', block.text)
            l.add_value('html', 'tag html here')
            yield l.load_item()
        # save categories
        # cat_blocks = response.css('div[class="aside"]').xpath(u"//span[text() = 'Категории']/parent::div/following-sibling::div/ul/li").extract()
        # for html in cat_blocks:
        #     body = html.encode('utf-8')
        #     block = response.replace(body=body)
        #     url = ' '.join(block.css('li a').xpath('@href').extract())
        #     title = ' '.join(block.css('li a').xpath('text()').extract())
        #     parent = response.meta['parent']
        #     l = ItemLoader(item=CategoryItem(), response=response)
        #     l.add_value('url', url)
        #     l.add_value('title', title)
        #     l.add_value('parent', parent)
        #     l.add_value('html', block.text)
        #     yield l.load_item()
        #     link = ''.join([self.base_url, url])
        #     request = self.getRequest(link, self.parse_sub_category3)
        #     request.meta['parent'] = url
        #     yield request
        # save product cards
        card_blocks = response.css('div[class="products-list"] div.p-card').extract()
        self.logger.info('parse_sub_category3: url: {url}; {count} products fetched'.format(url=response.url, count=len(card_blocks)))
        if len(card_blocks) == 0:
            message = ''.join(response.css('div[class="cat-text-null"] b').xpath('text()').extract())
            if len(response.text) < 200:
                self.logger.info('Response text: {text}'.format(text=response.text))
            self.logger.info('message: "{message}"; http_code: {code}'.format(message=message.encode('utf-8'), code=response.status))
        else:
            # handle pagination
            pagination_request = self.handle_pagination(response, self.parse_sub_category3, len(card_blocks))
            yield pagination_request
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
            #l.add_value('html', block.text)
            l.add_value('html', 'block html here')
            ymarket_link = ''.join(block.css('div[class="price-in-shops"] span').xpath('@data-link').extract())
            yield l.load_item()
            link = ''.join([self.base_url, url])
            request = self.getRequest(link, self.parse_product_page, True)
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

    def get_uri(self, url):
        return url.replace(self.base_url, '').split('?')[0]

    def get_request_url(self, response):
        if 'url_origin' in response.meta:
            return response.meta['url_origin']
        elif hasattr(response.request, 'url_origin'):
            return response.request.url_origin
        else:
            return response.request.url

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
        self.logger.info('handle pagination: count_pages={count_pages}'.format(count_pages=count_pages))
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
            return request

    def parse_parameters(self, response):
        parameters_html = ' '.join(response.css('div[class="product_tabs"] section[id="content2"] div[id="marketSpecs"]').extract())
        body = parameters_html
        block = response.replace(body=body.encode('utf-8'))
        params = block.xpath(u'//article/header/following-sibling::div[1]').xpath(u'//div[@data-tid]/span/text()').extract()
        data = {}
        for i in range(0, len(params), 2):
            data[params[i]] = params[i+1]
        return json.dumps(data)

    def parse_feedbacks(self, response):
        feedbacks_html = ' '.join(response.css('div[class="product_tabs"] section[id="content3"] div[id="marketReviews"]').extract())
        body = feedbacks_html.encode('utf-8')
        block = response.replace(body=body)
        fb_blocks = block.xpath(u'//div[text() = "Отзывы"]/following-sibling::div[1]/div').extract()
        data = []
        for fb_block in fb_blocks:
            item = {}
            fb_response = response.replace(body=fb_block.encode('utf-8'))
            item['name'] = ' '.join(fb_response.xpath('//img/following-sibling::div[1]/div[1]/span').xpath('text()').extract())
            item['eval'] = ' '.join(fb_response.xpath('//img/following-sibling::div[1]/div[2]/div/div').xpath('text()').extract())
            item['opinion'] = ' '.join(fb_response.xpath('//img/following-sibling::div[1]/div[2]/span[1]').xpath('text()').extract())
            item['experience'] = ' '.join(fb_response.xpath('//img/following-sibling::div[1]/div[2]/span[2]').xpath('text()').extract())
            item['plus'] = ' '.join(fb_response.xpath(u"//span[text() = 'Достоинства']/following-sibling::p[1]").xpath('text()').extract())
            item['minus'] = ' '.join(fb_response.xpath(u"//span[text() = 'Недостатки']/following-sibling::p[1]").xpath('text()').extract())
            item['comment'] = ' '.join(fb_response.xpath(u"//span[text() = 'Комментарий']/following-sibling::p[1]").xpath('text()').extract())
            item['date'] = ' '.join(fb_response.xpath('//div[3]').xpath('text()').extract())
            data.append(item)
        pprint(data)
        return json.dumps(data)

    def parse_filters(self, response):
        self.logger.info('!!!PARSE FILTERS')
        filters = {'url': None, 'settings': []}
        url = ' '.join(response.css('form[id="filter-form"]').xpath('@action').extract())
        filters['url'] = url
        self.logger.debug('url={url}'.format(url=url))
        filter_blocks = response.css('form[id="filter-form"] div[class="box active"]').extract()
        for body in filter_blocks:
            block = response.replace(body=body.encode('utf-8'))
            setting = u' '.join(filter(lambda i: len(i) > 0, map(lambda i: i.strip(), block.css('div[class="heading"]').xpath('text()').extract())))
            if len(setting) == 0:
                continue
            self.logger.debug(u'setting={setting}'.format(setting=setting))
            setting_values = filter(lambda i: len(i)>0, map(lambda i: i.strip(), block.css('div[class="box-inner"] div[class="ya-checkbox"] label[class="ya-check-label"]').xpath('text()').extract()))
            self.logger.debug('setting_values={setting_values}'.format(setting_values=setting_values))
            filters['settings'].append({setting: setting_values})
        self.logger.debug(u'filters={filters}'.format(filters=filters))
        return filters

    def test_saving_filters(self, response):
        # save filters
        filters = self.parse_filters(response)
        if filters['url'] is not None and len(filters['settings']) > 0:
            for setting in filters['settings']:
                if type(setting) is dict:
                    for setting_name, setting_values in setting.items():
                        l = ItemLoader(item=SettingItem(), response=response)
                        l.add_value('url', filters['url'])
                        l.add_value('name', setting_name)
                        yield l.load_item()
                        for setting_value in setting_values:
                            l = ItemLoader(item=SettingValueItem(), response=response)
                            l.add_value('settings_name', setting_name)
                            l.add_value('value', setting_value)
                            l.add_value('url', filters['url'])
                            yield l.load_item()



















