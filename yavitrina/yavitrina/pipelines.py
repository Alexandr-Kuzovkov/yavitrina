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

    def __init__(self):
        self.files = {}
        self.feed_name = ''

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        return pipeline

    def spider_opened(self, spider):
        self.exporter = YavitrinaFileExporter(spider)
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


class MyJsonItemExporter(JsonItemExporter):

    def __init__(self, file, **kwargs):
        super(self.__class__, self).__init__(file, **kwargs)

    def _beautify_newline(self):
        if self.indent is not None:
            self.file.write(b'\n')

    def start_exporting(self):
        self.file.write(b"[")
        self._beautify_newline()

    def finish_exporting(self):
        self._beautify_newline()
        self.file.write(b"]")

    def export_item(self, item):
        if self.first_item:
            self.first_item = False
        else:
            self.file.write(b',')
            self._beautify_newline()
        itemdict = dict(self._get_serialized_fields(item))
        data = self.encoder.encode(itemdict)
        self.file.write(to_bytes(data, self.encoding))

class YavitrinaFileExporter(object):

    spider = None
    dirname = None

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

    def finish_exporting(self):
        pass

    def export_item(self, item):
        if isinstance(item, CategoryItem):
            print("!!!is of type CategoryItem")
            self.save_category_item(item)


    def save_category_item(self, item):
        category_folder = os.path.sep.join([self.dirname, 'categories'])
        create_folder(category_folder)
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = ' '.join(val)
            else:
                data[key] = val
        filename = os.path.sep.join([category_folder, '.'.join([data['url'].replace('/', ''), 'json'])])
        with open(filename, 'wb') as f:
            f.write(json.dumps(data, sort_keys=True, indent=4))







