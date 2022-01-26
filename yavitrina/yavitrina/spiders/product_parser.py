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

############################################################################
# Parsing product pages if products is absent but product_card is present
############################################################################

class ProductParserSpider(scrapy.Spider):
    name = 'product_parser'
    #allowed_domains = ['yavitrina.ru', 'i.yavitrina.ru']
    allowed_domains = []
    dirname = 'product_parser'
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

    def __init__(self, drain=False, noproxy=False, cleardb=False, request_type=False, product_request_type=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if noproxy:
            self.use_scrapestack = False
        if cleardb:
            self.clear_db = True
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
        request = scrapy.Request(url, callback=self.handle_product_cards)
        yield request

    def handle_product_cards(self, response):
        total_cards = self.db._getone("SELECT count(*) AS total FROM product_card pc LEFT JOIN  product p ON pc.product_id = p.product_id WHERE p.title ISNULL")
        self.logger.info('total_cards; {total}'.format(total=total_cards))
        LIMIT = 100
        offsets = range(0, total_cards, LIMIT)
        for offset in offsets:
            self.logger.debug('handle tags %s-%s' % (offset, min(offset+LIMIT, total_cards)))
            cards = self.db._getraw("SELECT pc.url, pc.product_id, pc.html, pc.page AS total FROM product_card pc LEFT JOIN  product p ON pc.product_id = p.product_id WHERE p.title ISNULL ORDER BY pc.id OFFSET {offset} LIMIT {limit} ".format(offset=offset, limit=LIMIT), ['url', 'product_id', 'html', 'page'])
            for card in cards:
                #pprint(card)
                url = ''.join([self.base_url, card['url']])
                # body = card['html'].encode('utf-8')
                body = card['html']
                block = response.replace(body=body)
                rate = ' '.join(block.css('div[class="reviews-info"] span[class="point"]').xpath('text()').extract())
                colors = ','.join(map(lambda i: i.replace(u'border:1px solid #b6b6b6; background-color: ', ''), block.css('div.color-list span').xpath('@style').extract()))
                ymarket_link = ''.join(block.css('div[class="price-in-shops"] span').xpath('@data-link').extract())
                pprint(rate)
                pprint(colors)
                pprint(ymarket_link)
                request = self.getRequest(url, self.parse_product_page, dont_filter=True, request_type=self.product_request_type)
                request.meta['parent'] = card['page']
                request.meta['category'] = card['page']
                request.meta['ymarket_link'] = ymarket_link
                request.meta['rate'] = rate
                request.meta['colors'] = colors
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

    def parse_product_page(self, response):
        # pprint(response.text)
        requested_url = self.get_request_url(response)
        title = ' '.join(response.css('div[class="product-page"] div[class="p-info"] h1').xpath('text()').extract())
        description = ' '.join(
            response.css('div[class="product-page"] div[class="p-info"] div[class="desc"] p[class="d-text"]').xpath(
                'text()').extract())
        try:
            price = int(' '.join(response.css(
                'div[class="product-page"] div[class="p-info"] div[class="info-detail"] span[itemprop="price"]').xpath(
                '@content').extract()))
        except Exception as ex:
            price = None
        shop_link = ' '.join(response.css(
            'div[class="product-page"] div[class="p-info"] div[class="btn-box"] a[class="btn btn-in-shops"]').xpath(
            '@href').extract())
        shop_link2 = ' '.join(
            response.css('div[class="product_tabs"] section[id="content1"] a').xpath('@href').extract())
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
        related_products = ','.join(map(lambda i: i.split('/').pop(),
                                        response.css('div[class="related-products"] div[class="b-info-wrap"] a').xpath(
                                            '@href').extract()))
        if len(related_products) > 0:
            l.add_value('related_products', related_products)
        yield l.load_item()
        # save images
        image_urls = response.css('div[class="photos"] img').xpath('@src').extract()
        for link in image_urls:
            if link.startswith('//') or (not link.startswith('https:')):
                link = 'https:{img}'.format(img=link)
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

    def get_uri(self, url):
        return url.replace(self.base_url, '').split('?')[0]


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
            item['image'] = ' '.join(fb_response.xpath('//img').xpath('@src').extract())
            data.append(item)
        return json.dumps(data)














