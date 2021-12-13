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
from yavitrina.scrapestack import ScrapestackRequest
from yavitrina.seleniumrequest import SelenuimRequest
from scrapy_headless import HeadlessRequest
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib import urlencode
from yavitrina.items import SettingItem
from yavitrina.items import SettingValueItem
from yavitrina.items import ExSettingItem
from yavitrina.items import ExSettingValueItem
from yavitrina.items import ExProductItem
from yavitrina.items import ExProductColorItem
from yavitrina.items import ExSearchProductItem
from yavitrina.items import ExProductImageItem
from yavitrina.items import ExProductPriceItem
from yavitrina.items import ExReviewItem
from yavitrina.items import ExCategoryItem
from yavitrina.items import ExCategorySearchItem
from yavitrina.items import ExProductCategoryItem
from yavitrina.items import ExTagItem
from yavitrina.items import ExProductSettingsItem
from yavitrina.items import ExNewCategoryItem
from yavitrina.items import ExCategoryHasSettingsItem


class ExporterSpider(scrapy.Spider):
    name = 'exporter'
    dirname = 'exporter'
    handle_httpstatus_list = [400, 404]
    logger = logging.getLogger()
    drain = False
    custom_settings = {
        'LOG_LEVEL': 'DEBUG'
    }
    clear_db = False
    db_import = None
    db_export = None

    def __init__(self, drain=False, cleardb=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if cleardb:
            self.clear_db = True

    def start_requests(self):
        url = 'http://localhost:6800'
        request = scrapy.Request(url, callback=self.export_product)
        yield request


    def starting_export(self, response):
        self.export_product(response)

    def export_product(self, response):
        latest_product_time = self.db_export.get_latest_time('product')
        if latest_product_time is not None:
            condition = {'created_at >=': str(latest_product_time)}
        else:
            condition = {'created_at >=': '1970-01-01'}
        table = 'product'
        total_products = self.db_import.get_items_total(table, condition)
        # pprint(total_products)
        LIMIT = 100
        offsets = range(0, total_products, LIMIT)
        #offsets = offsets[0:5]
        #pprint(offsets)
        for offset in offsets:
            products = self.db_import.get_items_chunk(table, condition, offset, LIMIT)
            for product in products:
                ex_product = ItemLoader(item=ExProductItem(), response=response)
                rating = None
                if product['rate'] is not None:
                    rating = float(product['rate'].strip().replace('(', '').replace(')', '').replace('/', '.'))
                ex_product.add_value('title', product['title'])
                ex_product.add_value('description', product['description'])
                ex_product.add_value('price', product['price'])
                ex_product.add_value('url', product['shop_link'])
                ex_product.add_value('url_review', product['shop_link2'])
                ex_product.add_value('rating', rating)
                ex_product.add_value('product_id', product['product_id'])
                ex_product.add_value('rate', product['rate'])
                ex_product.add_value('created_at', self.datetime2str(product['created_at']))
                yield ex_product.load_item()




    def str2datetime(self, str):
        d = map(lambda i: int(i), str.split(' ')[0].split('-'))
        t = map(lambda i: int(i), str.split(' ')[1].split('.')[0].split(':'))
        res = datetime.datetime(d[0], d[1], d[2], t[0], t[1], t[2], 0)
        return res

    def datetime2str(self, dt):
        return str(dt).split('+').pop(0)

    def helper(self):
        tables = self.db_export._get_tables_list()
        pprint(tables)
        for table in tables:
            fld_lst = self.db_export._get_fld_list(table)
            print('{table}: ({columns})'.format(table=table, columns=','.join(fld_lst)))















