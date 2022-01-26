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


class ExporterProductTagSpider(scrapy.Spider):
    name = 'exporter_product_tag'
    dirname = 'exporter_product_tag'
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
        self.export_product_tag()

    def export_product_tag(self):
        self.logger.info('export product_tag...')
        LIMIT = 1000
        offset = 0
        while True:
            buffer = []
            self.logger.info('fetching items %s - %s' % (offset, offset+LIMIT))
            sql = '''SELECT product_id, tag.url
                    FROM (SELECT * FROM product_card WHERE product_id IN (SELECT product_id FROM product)) AS pc
                    INNER JOIN tag ON string_to_array(pc.page, ',') && string_to_array(tag.page, ',')
                    WHERE pc.product_id IN (SELECT product_id FROM product)
                    ORDER BY pc.id OFFSET {offset} LIMIT {limit}'''.format(offset=offset, limit=LIMIT)
            start_time = time.time()
            items = self.db_import._getraw(sql, ['product_id', 'url'])
            end_time = time.time()
            count = len(items)
            self.log('... %s items fetched (%s ms)' % (count, round((end_time - start_time), 3)))
            if count == 0:
                break
            part_ids = map(lambda i: i['product_id'], items)
            products = self.db_export._getraw("SELECT id, product_id FROM product WHERE product_id IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_ids)), ['id', 'product_id'])
            product_map = {}
            for product in products:
                product_map[product['product_id']] = product['id']
            part_urls = map(lambda i: i['url'], items)
            tags = self.db_export._getraw("SELECT id, url FROM tag WHERE url IN (%s)" % ','.join(map(lambda i: "'%s'" % i, part_urls)), ['id', 'url'])
            tag_map = {}
            for tag in tags:
                tag_map[tag['url']] = tag['id']
            for item in items:
                row = {}
                if item['product_id'] in product_map and item['url'] in tag_map:
                    row['product_id'] = product_map[item['product_id']]
                    row['tag_id'] = tag_map[item['url']]
                    buffer.append(row)
            self.db_export._insert('product_tag', buffer, ignore=True)
            offset += LIMIT
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















