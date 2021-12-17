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
        # data = self.export_product()
        # self.export_product_colors(data['product_colors'])
        # self.export_search_product(data['related_products'])
        # self.export_product_image(data['product_colors'].keys())
        # self.export_product_price(data['product_prices'])
        # self.export_review(data['feedbacks'])
        category_urls = self.export_category()
        self.link_categories(category_urls)
        self.export_settings()
        self.export_settings_value()
        self.export_category_search()
        self.export_category_has_settings()

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
        feedbacks = {}
        offsets = range(0, total_products, LIMIT)
        for offset in offsets:
            buffer = []
            products = self.db_import.get_items_chunk(table, condition, offset, LIMIT)
            for product in products:
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
                    if len(product['feedbacks']) > 0:
                        feedbacks[product['product_id']] = product['feedbacks']
            # product_ids = map(lambda i: i['product_id'], buffer)
            # sql = "SELECT product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, product_ids))
            # exist_items = self.db_export._getraw(sql, ['product_id'], None)
            # exist_product_ids = map(lambda i: i['product_id'], exist_items)
            # filtered_buffer = filter(lambda i: i['product_id'] not in exist_product_ids, buffer)
            self.db_export._insert(table, buffer, ignore=True)
        self.logger.info('done')
        return {
            'product_colors': product_colors,
            'related_products': related_products,
            'product_prices': product_prices,
            'feedbacks': feedbacks
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

    def export_review(self, feedbacks):
        self.logger.info('export review')
        product_ids = feedbacks.keys()
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
                    sql = "INSERT INTO review (name, dignity, flaw, grade, product_id, date, image, comment, use_experince, city) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    date = None
                    city = None
                    for feedback in feedbacks[product['product_id']]:
                        if feedback['date'] is not None:
                            parts = feedback['date'].split(',')
                            date = ''.join(parts[-1:]).strip()
                            date = self.get_parsed_date(date)
                            if len(parts) > 1:
                                city = ''.join(parts[0:1]).strip()
                        self.db_export.cur.execute(sql, [
                            feedback['name'],
                            feedback['plus'],
                            feedback['minus'],
                            feedback['eval'],
                            product['id'],
                            date,
                            feedback['image'],
                            feedback['comment'],
                            feedback['experience'],
                            city
                        ])
                self.db_export.conn.commit()
            except Exception as ex:
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')

    def export_category(self):
        self.logger.info('export category...')
        latest_time = self.db_export.get_latest_time('category')
        if latest_time is not None:
            condition = {'created_at >=': str(latest_time)}
        else:
            condition = {'created_at >=': '1970-01-01'}
        table = 'category'
        total_categories = self.db_import.get_items_total(table, condition)
        # pprint(total_categories)
        LIMIT = 100
        category_urls = {}
        offsets = range(0, total_categories, LIMIT)
        for offset in offsets:
            buffer = []
            categories = self.db_import.get_items_chunk(table, condition, offset, LIMIT)
            for category in categories:
                row = {}
                row['name'] = category['title']
                row['description'] = category['description']
                row['url'] = category['url']
                row['image_path'] = category['img']
                row['created_at'] = self.datetime2str(category['created_at'])
                buffer.append(row)
                category_urls[category['url']] = {'parent_url': category['parent_url']}
            self.db_export._insert(table, buffer, ignore=True)
        self.logger.info('done')
        return category_urls

    def link_categories(self, category_urls):
        self.logger.info('link categories...')
        urls = category_urls.keys()
        LIMIT = 100
        offsets = range(0, len(urls), LIMIT)
        self.db_export.dbopen()
        for offset in offsets:
            part_urls = urls[offset:offset + LIMIT]
            sql = "SELECT id, url, name FROM category WHERE url IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_urls))
            categories = self.db_export._getraw(sql, ['id', 'url', 'name'], None)
            self.db_export.dbopen()
            try:
                for category in categories:
                    parent_url = category_urls[category['url']]['parent_url']
                    if parent_url is not None:
                        parent_id = self.db_export._getone("SELECT id FROM category WHERE url='{parent_url}'".format(parent_url=parent_url))
                        if parent_id is not None:
                            sql = "UPDATE category SET parent_id={parent_id} WHERE id={id}".format(parent_id=parent_id, id=category['id'])
                            self.db_export.cur.execute(sql)
                self.db_export.conn.commit()
            except Exception as ex:
                self.logger.error(ex)
        self.db_export.dbclose()
        self.logger.info('done')

    def export_settings(self):
        self.logger.info('export settings...')
        table = 'settings'
        total_settings = self.db_import.get_items_total('settings')
        # pprint(total_settings)
        LIMIT = 100
        settings_urls = {}
        offsets = range(0, total_settings, LIMIT)
        for offset in offsets:
            buffer = []
            settings = self.db_import._getraw("SELECT name,url FROM settings ORDER BY id OFFSET {offset} LIMIT {limit}".format(offset=offset, limit=LIMIT), ['name', 'url'])
            for setting in settings:
                row = {}
                row['name'] = setting['name']
                row['url'] = setting['url']
                buffer.append(row)
            self.db_export._insert(table, buffer, ignore=True)
        self.logger.info('done')

    def export_category_search(self):
        self.logger.info('export category_search...')
        latest_time = self.db_export.get_latest_time('category_search')
        if latest_time is not None:
            condition = {'created_at >=': str(latest_time)}
        else:
            condition = {'created_at >=': '1970-01-01'}
        table = 'search_tag'
        total_search_tag = self.db_import.get_items_total(table, condition)
        LIMIT = 100
        offsets = range(0, total_search_tag, LIMIT)
        for offset in offsets:
            buffer = []
            search_tags = self.db_import.get_items_chunk(table, condition, offset, LIMIT)
            for search_tag in search_tags:
                row = {}
                category_id = self.db_export._getone("SELECT id FROM category WHERE url='{url}'".format(url=search_tag['page']))
                child_id = self.db_export._getone("SELECT id FROM category WHERE url='{url}'".format(url=search_tag['url']))
                if (category_id is not None and child_id is not None):
                    row['child_id'] = child_id
                    row['category_id'] = category_id
                    row['created_at'] = self.datetime2str(search_tag['created_at'])
                    buffer.append(row)
            self.db_export._insert('category_search', buffer, ignore=True)
        self.logger.info('done')

    def export_settings_value(self):
        self.logger.info('export settings_value...')
        table = 'settings_value'
        LIMIT = 500
        settings_map = {}
        #collecting settings
        total_settings = self.db_export.get_items_total('settings')
        offsets = range(0, total_settings, LIMIT)
        for offset in offsets:
            settings_part = self.db_export.get_items_chunk('settings', condition=None, offset=offset, limit=LIMIT)
            for settings_item in settings_part:
                if settings_item['url'] in settings_map:
                    settings_map[settings_item['url']].append(settings_item)
                else:
                    settings_map[settings_item['url']] = [settings_item]
        total_settings_value = self.db_import.get_items_total('settings_value')
        # pprint(total_settings_value)
        offsets = range(0, total_settings_value, LIMIT)
        for offset in offsets:
            buffer = []
            settings_values = self.db_import.get_items_chunk(table, condition=None, offset=offset, limit=LIMIT)
            for setting_value in settings_values:
                res = filter(lambda i: i['url'] == setting_value['url'] and i['name'] == setting_value['settings_name'].decode('utf-8'), settings_map[setting_value['url']])
                if len(res) > 0:
                    for item in res:
                        row = {}
                        row['settings_id'] = item['id']
                        row['value'] = setting_value['value']
                        buffer.append(row)
            self.db_export._insert('settings_value', buffer, ignore=True)
        self.logger.info('done')

    def export_category_has_settings(self):
        self.logger.info('export category_has_settings...')
        LIMIT = 500
        settings_map = {}
        # collecting settings
        total_settings = self.db_export.get_items_total('settings')
        offsets = range(0, total_settings, LIMIT)
        for offset in offsets:
            settings_part = self.db_export.get_items_chunk('settings', condition=None, offset=offset, limit=LIMIT)
            for settings_item in settings_part:
                if settings_item['url'] in settings_map:
                    settings_map[settings_item['url']].append(settings_item)
                else:
                    settings_map[settings_item['url']] = [settings_item]
        total_categories = self.db_export.get_items_total('category')
        # pprint(total_categories)
        offsets = range(0, total_categories, LIMIT)
        for offset in offsets:
            buffer = []
            categories = self.db_export.get_items_chunk('category', condition=None, offset=offset, limit=LIMIT)
            for category in categories:
                if category['url'] not in settings_map:
                    continue
                res = filter(lambda i: i['url'] == category['url'], settings_map[category['url']])
                if len(res) > 0:
                    for item in res:
                        row = {}
                        row['settings_id'] = item['id']
                        row['category_id'] = category['id']
                        buffer.append(row)
            self.db_export._insert('category_has_settings', buffer, ignore=True)
        self.logger.info('done')


    def str2datetime(self, str):
        d = map(lambda i: int(i), str.split(' ')[0].split('-'))
        t = map(lambda i: int(i), str.split(' ')[1].split('.')[0].split(':'))
        res = datetime.datetime(d[0], d[1], d[2], t[0], t[1], t[2], 0)
        return res

    def datetime2str(self, dt):
        return str(dt).split('+').pop(0)

    def get_parsed_date(self, str):
        month_map = {
            u'января': 1,
            u'февраля': 2,
            u'марта': 3,
            u'апреля': 4,
            u'мая': 5,
            u'июня': 6,
            u'июля': 7,
            u'августа': 8,
            u'сентября': 9,
            u'октября': 10,
            u'ноября': 11,
            u'декабря': 12
        }
        parts = filter(lambda i: len(i) > 0, map(lambda i: i.strip(), str.strip().split(' ')))
        day = int(parts[0])
        month = month_map[parts[1]]
        if len(parts) < 3:
            year = datetime.datetime.now().year
        else:
            year = int(parts[2])
        dt = datetime.datetime(year, month, day, 0, 0, 0, 0)
        date = self.datetime2str(dt)
        return date

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















