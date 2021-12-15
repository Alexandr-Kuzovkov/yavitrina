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
        request = scrapy.Request(url, callback=self.starting_export)
        yield request


    def starting_export(self, response):
        self.helper()
        data = self.export_product()
        #self.export_product_colors(data['product_colors'])
        #self.export_search_product(data['related_products'])
        #self.export_product_image(data['product_colors'].keys())
        self.export_product_price(data['product_prices'])

    def export_product(self):
        self.logger.info('export product...')
        latest_time = self.db_export.get_latest_time('product')
        if latest_time is not None:
            condition = {'created_at >=': str(latest_time)}
        else:
            condition = {'created_at >=': '1970-01-01'}
        table = 'product'
        total_products = self.db_import.get_items_total(table, condition)
        # pprint(total_products)
        LIMIT = 100
        product_colors = {}
        related_products = {}
        product_prices = {}
        offsets = range(0, total_products, LIMIT)
        for offset in offsets:
            buffer = []
            products = self.db_import.get_items_chunk(table, condition, offset, LIMIT)
            for product in products:
                #ex_product = ItemLoader(item=ExProductItem(), response=response)
                row = {}
                rating = None
                if product['rate'] is not None:
                    rating = float(product['rate'].strip().replace('(', '').replace(')', '').replace('/', '.'))
                row['title'] = product['title']
                row['description'] = product['description']
                row['price'] = product['price']
                row['url'] = product['shop_link']
                row['url_review'] = product['shop_link2']
                row['rating'] = rating
                row['product_id'] = product['product_id']
                row['rate'] = product['rate']
                row['created_at'] = self.datetime2str(product['created_at'])
                buffer.append(row)
                if product['colors'] is not None:
                    product_colors[product['product_id']] = product['colors']
                if product['related_products'] is not None:
                    related_products[product['product_id']] = product['related_products']
                if product['feedbacks'] is not None:
                    product_prices[product['product_id']] = {'price': product['price'], 'name': product['title'], 'rating': rating, 'count_review': len(product['feedbacks'])}
            product_ids = map(lambda i: i['product_id'], buffer)
            sql = "SELECT product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, product_ids))
            exist_items = self.db_export._getraw(sql, ['product_id'], None)
            exist_product_ids = map(lambda i: i['product_id'], exist_items)
            filtered_buffer = filter(lambda i: i['product_id'] not in exist_product_ids, buffer)
            self.db_export._insert(table, filtered_buffer)
        self.logger.info('done')
        return {
            'product_colors': product_colors,
            'related_products': related_products,
            'product_prices': product_prices
        }

    def export_product_colors(self, product_colors):
        #pprint(product_colors)
        self.logger.info('export product_color')
        LIMIT = 100
        product_ids = product_colors.keys()
        offsets = range(0, len(product_ids), LIMIT)
        self.db_export.dbopen()
        for offset in offsets:
            part_ids = product_ids[offset:offset+LIMIT]
            sql = "SELECT id, product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_ids))
            products = self.db_export._getraw(sql, ['id', 'product_id'], None)
            self.db_export.dbopen()
            try:
                for product in products:
                    colors = product_colors[product['product_id']].split(',')
                    for color in colors:
                        sql = "INSERT INTO product_color (hex, product_id) VALUES (%s,%s)"
                        self.db_export.cur.execute(sql, [color, product['id']])
                self.db_export.conn.commit()
            except Exception as ex:
                self.db_export.conn.rollback()
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')

    def export_search_product(self, related_products):
        #pprint(related_products)
        self.logger.info('export search_product')
        LIMIT = 100
        product_ids = related_products.keys()
        offsets = range(0, len(product_ids), LIMIT)
        self.db_export.dbopen()
        for offset in offsets:
            part_ids = product_ids[offset:offset+LIMIT]
            sql = "SELECT id, product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_ids))
            products = self.db_export._getraw(sql, ['id', 'product_id'], None)
            self.db_export.dbopen()
            try:
                for product in products:
                    related_product_ids = related_products[product['product_id']].split(',')
                    for related_product_id in related_product_ids:
                        child_id = self.db_export._getone("SELECT id FROM product WHERE product_id='%s'" % related_product_id)
                        if child_id is not None:
                            sql = "INSERT INTO search_product (product_id, child_id) VALUES (%s,%s)"
                            self.db_export.cur.execute(sql, [product['id'], child_id])
                self.db_export.conn.commit()
            except Exception as ex:
                self.db_export.conn.rollback()
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')


    def export_product_image(self, product_ids):
        self.logger.info('export product_image')
        LIMIT = 100
        offsets = range(0, len(product_ids), LIMIT)
        self.db_export.dbopen()
        for offset in offsets:
            part_ids = product_ids[offset:offset + LIMIT]
            sql = "SELECT id, product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_ids))
            products = self.db_export._getraw(sql, ['id', 'product_id'], None)
            self.db_export.dbopen()
            try:
                for product in products:
                    images = self.db_import._getraw("SELECT path, filename, url FROM image WHERE product_id=%s", ['path', 'filename', 'url'], [product['product_id']])
                    image_type = 'main'
                    for i in range(0, len(images)):
                        if len(images) > 1:
                            sizes = map(lambda j: j['url'].split('/').pop(), images)
                            index_max = sizes.index(max(sizes))
                            if i == index_max:
                                image_type = 'main'
                            else:
                                image_type = 'child'
                        sql = "INSERT INTO product_image (path, type, product_id) VALUES (%s,%s,%s)"
                        self.db_export.cur.execute(sql, [images[i]['path'], image_type, product['id']])
                self.db_export.conn.commit()
            except Exception as ex:
                self.db_export.conn.rollback()
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')

    def export_product_price(self, product_prices):
        self.logger.info('export product_price')
        product_ids = product_prices.keys()
        LIMIT = 100
        offsets = range(0, len(product_ids), LIMIT)
        self.db_export.dbopen()
        for offset in offsets:
            part_ids = product_ids[offset:offset + LIMIT]
            sql = "SELECT id, product_id, url FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_ids))
            products = self.db_export._getraw(sql, ['id', 'product_id', 'url'], None)
            self.db_export.dbopen()
            try:
                for product in products:
                    sql = "INSERT INTO product_price (url, name, price, product_id, count_review, rating) VALUES (%s,%s,%s,%s,%s,%s)"
                    self.db_export.cur.execute(sql, [
                        product['url'],
                        product_prices[product['product_id']]['name'],
                        product_prices[product['product_id']]['price'],
                        product['id'],
                        product_prices[product['product_id']]['count_review'],
                        product_prices[product['product_id']]['rating']
                    ])
                self.db_export.conn.commit()
            except Exception as ex:
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')


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
        # condition = {'created_at >=': '1970-01-01'}
        # table = 'product'
        # products = self.db_import.get_items_chunk(table, condition, 0, 100)
        # pprint(map(lambda i: len(i['feedbacks']), products))















