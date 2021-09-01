# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
from scrapy.exporters import JsonItemExporter
import tempfile
from scrapy.utils.serialize import ScrapyJSONEncoder
import time
from scrapy.utils.python import is_listlike, to_bytes, to_unicode
import os
import json
from os import path
from configparser import ConfigParser
import logging
logger = logging.getLogger()
from pprint import pprint
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from yavitrina.items import CategoryItem
from yavitrina.items import TagItem
from yavitrina.items import ProductCardItem
from yavitrina.items import ProductItem
from yavitrina.items import ImageItem
from scrapy.exceptions import CloseSpider
from yavitrina.config import load_config
from yavitrina.extensions import PgSQLStore

def create_folder(directory):
    logger.debug('Create directory. "%s"' % directory)
    try:
        if not os.path.exists(directory):
            logger.debug('Creating... directory. "%s"' % directory)
            os.makedirs(directory)
    except OSError, ex:
        logging.error('Error: Creating directory. "%s"' % directory)
        raise ex


def clear_folder(directory):
    if os.path.exists(directory):
        files = os.listdir(directory)
        for fname in files:
            if fname in ['.', '..']:
                continue
            fullname = os.path.sep.join([directory, fname])
            if not os.path.isdir(fullname):
                os.remove(fullname)


def get_current_date():
    current_date = time.strftime('%Y-%m-%d', time.gmtime(int(time.time())))
    return current_date



class YavitrinaPipeline(object):

    files = {}
    feed_name = ''
    config = None

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        CONFIG_FILE = '/home/root/vitrina.config.ini'
        try:
            self.config = load_config(CONFIG_FILE)
        except Exception as ex:
            logging.error(ex.message)
            raise CloseSpider(ex.message)

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        return pipeline

    def spider_opened(self, spider):
        #self.exporter = YavitrinaFileExporter(spider)
        self.exporter = YavitrinaPgSqlExporter(spider, self.config)
        self.exporter.start_exporting()


    def spider_closed(self, spider):
        self.exporter.finish_exporting()

        stats = self.stats.get_stats()
        #if 'log_count/ERROR' in stats:
        #    fp = self.files.pop(spider)
        #    fp.close()
        #else:

    def process_item(self, item, spider):
        if not spider.drain:
            self.exporter.export_item(item)
        return item


class YavitrinaFileExporter(object):

    spider = None
    dirname = None
    sub_folders = {'categories': None, 'images': None, 'products': None, 'tags': None, 'product_card': None, 'images_files': None}

    def __init__(self, spider, **kwargs):
        super(self.__class__, self).__init__()
        self.spider = spider

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', 'files')
        create_folder(files_dir)
        if hasattr(self.spider, 'dirname') and self.spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)
        for folder in self.sub_folders.keys():
            item_folder = os.path.sep.join([self.dirname, folder])
            create_folder(item_folder)
            self.sub_folders[folder] = item_folder

    def finish_exporting(self):
        pass

    def export_item(self, item):
        if isinstance(item, CategoryItem):
            logging.info('saving category item')
            self.save_category_item(item)
        elif isinstance(item, TagItem):
            logging.info('saving tag item')
            self.save_tag_item(item)
        elif isinstance(item, ProductCardItem):
            logging.info('saving product card item')
            self.save_product_card_item(item)
        elif isinstance(item, ProductItem):
            logging.info('saving product item')
            self.save_product_item(item)
        elif isinstance(item, ImageItem):
            logging.info('saving image item')
            #pprint(item)
            self.save_image_item(item)


    def save_category_item(self, item):
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = u','.join(val)
            else:
                data[key] = val
        item_folder = self.sub_folders['categories']
        filename = os.path.sep.join([item_folder, '.'.join([data['url'].replace('/', ''), 'json'])])
        with open(filename, 'wb') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def save_tag_item(self, item):
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = u','.join(val)
            else:
                data[key] = val
        item_folder = self.sub_folders['tags']
        filename = os.path.sep.join([item_folder, '.'.join([data['url'].replace('/', ''), 'json'])])
        with open(filename, 'wb') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def save_product_card_item(self, item):
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = u','.join(map(lambda i: unicode(i), val))
            else:
                data[key] = val
        item_folder = self.sub_folders['product_card']
        filename = os.path.sep.join([item_folder, '.'.join([data['product_id'], 'json'])])
        with open(filename, 'wb') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def save_product_item(self, item):
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = u','.join(map(lambda i: unicode(i), val))
            else:
                data[key] = val
        item_folder = self.sub_folders['products']
        filename = os.path.sep.join([item_folder, '.'.join([data['product_id'], 'json'])])
        with open(filename, 'wb') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))

    def save_image_item(self, item):
        filename = os.path.sep.join([self.sub_folders['images_files'], item['filename']])
        if not (os.path.exists(filename) and os.path.isfile(filename)):
            with open(filename, 'wb') as f1:
                f1.write(item['data'])
        data = {}
        for key, val in item.items():
            if key == 'data':
                continue
            if type(val) is list:
                data[key] = ','.join(map(lambda i: str(i), val))
            else:
                data[key] = val
        data['path'] = filename
        item_folder = self.sub_folders['images']
        if 'product_id' in data:
            filename = os.path.sep.join([item_folder, '.'.join([data['product_id'], 'json'])])
        elif 'category_url' in data:
            filename = os.path.sep.join([item_folder, '.'.join([data['category_url'], 'json'])])
        with open(filename, 'wb') as f2:
            f2.write(json.dumps(data, sort_keys=True, indent=4))


class YavitrinaPgSqlExporter(object):

    spider = None
    dirname = None
    sub_folders = {'images_files': None}
    config = None
    db = None

    def __init__(self, spider, config, **kwargs):
        super(self.__class__, self).__init__()
        self.spider = spider
        self.config = config

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', 'files')
        create_folder(files_dir)
        if hasattr(self.spider, 'dirname') and self.spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)
        for folder in self.sub_folders.keys():
            item_folder = os.path.sep.join([self.dirname, folder])
            create_folder(item_folder)
            self.sub_folders[folder] = item_folder
        db_conf = {
            'dbname':self.config['DATABASE']['DB_NAME'],
            'dbuser':self.config['DATABASE']['DB_USER'],
            'dbhost':self.config['DATABASE']['DB_HOST'],
            'dbport':self.config['DATABASE']['DB_PORT'],
            'dbpass':self.config['DATABASE']['DB_PASS']
        }
        self.db = PgSQLStore(db_conf)
        if self.spider.clear_db:
            self.db.clear_db()

    def finish_exporting(self):
        pass

    def export_item(self, item):
        if isinstance(item, CategoryItem):
            logging.info('saving category item')
            self.save_category_item(item)
        elif isinstance(item, TagItem):
            logging.info('saving tag item')
            self.save_tag_item(item)
        elif isinstance(item, ProductCardItem):
            logging.info('saving product card item')
            self.save_product_card_item(item)
        elif isinstance(item, ProductItem):
            logging.info('saving product item')
            self.save_product_item(item)
        elif isinstance(item, ImageItem):
            logging.info('saving image item')
            #pprint(item)
            self.save_image_item(item)


    def save_category_item(self, item):
        data = {}
        mapping = {'parent': 'parent_url'}
        for key, val in item.items():
            if key in mapping:
                key = mapping[key]
            if type(val) is list:
                data[key] = u','.join(val)
            else:
                data[key] = val
        self.db.save_category_item(data)

    def save_tag_item(self, item):
        pass

    def save_product_card_item(self, item):
        pass

    def save_product_item(self, item):
        pass

    def save_image_item(self, item):
        filename = os.path.sep.join([self.sub_folders['images_files'], item['filename']])
        if not (os.path.exists(filename) and os.path.isfile(filename)):
            with open(filename, 'wb') as f1:
                f1.write(item['data'])
        data = {}
        for key, val in item.items():
            if key == 'data':
                continue
            if type(val) is list:
                data[key] = ','.join(map(lambda i: str(i), val))
            else:
                data[key] = val
        data['path'] = filename
        self.db.save_image_item(data)








