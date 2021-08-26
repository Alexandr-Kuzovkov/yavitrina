# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


import time
from scrapy import signals
from scrapy.exporters import XmlItemExporter, PprintItemExporter
import tempfile
import six
from datetime import datetime
import os
import logging
from pprint import pprint
from feedgenerator.azurestorage import AzureStorage


class FeedgeneratorPipeline(object):
    def process_item(self, item, spider):
        return item



class MyXmlItemExporter(XmlItemExporter):

    publisher = ''
    publisherurl = ''
    spider = None

    def __init__(self, file, **kwargs):
        self.spider = kwargs.pop('spider')
        super(self.__class__, self).__init__(file, **kwargs)

    def start_exporting(self):
        super(self.__class__, self).start_exporting()
        lastBuildDate = self.dateFormat()
        try:
            self._export_xml_field('publisher', self.publisher)
            self._export_xml_field('publisherurl', self.publisherurl)
            self._export_xml_field('lastBuildDate', lastBuildDate)
        except TypeError, ex:
            self._export_xml_field('publisher', self.publisher, 0)
            self._export_xml_field('publisherurl', self.publisherurl, 0)
            self._export_xml_field('lastBuildDate', lastBuildDate, 0)

    def _xg_characters(self, serialized_value):
        if not isinstance(serialized_value, six.text_type):
            serialized_value = serialized_value.decode(self.encoding)
        return self.xg.ignorableWhitespace(serialized_value)

    def export_item(self, item):
        self.xg.startElement(self.item_element, {})
        fields = []
        for name, value in self._get_serialized_fields(item, default_value=''):
            if value is None:
                continue
            fields.append({'name': name, 'value': value})
        fields = self.sortFields(fields)
        for field in fields:
            try:
                self._export_xml_field(field['name'], field['value'])
            except TypeError, ex:
                self._export_xml_field(field['name'], field['value'], 0)
        self.xg.endElement(self.item_element)

    def setPublisher(self, publisher):
        self.publisher = publisher

    def setPublisherUrl(self, publishedurl):
        self.publisherurl = publishedurl

    def serialize_field(self, field, name, value):
        if value is None:
            return super(self.__class__, self).serialize_field(field, name, value)
        if type(value) is list:
            value = ','.join(map(self.convertValue, value))
        else:
            value = self.convertValue(value)
        value = ''.join(['<![CDATA[', value, ']]>'])
        return super(self.__class__, self).serialize_field(field, name, value)

    def dateFormat(self, value=None, date_format=None):
        if date_format is None:
            date_format = '%Y-%m-%dT%H:%M:%S'
        if value is not None and type(value) is datetime:
            return value.strftime(date_format)
        else:
            return datetime.now().strftime(date_format)

    def convertValue(self, value):
        if type(value) is datetime:
            return self.dateFormat(value, self.spider.date_format)
        else:
            try:
                return str(value)
            except Exception, ex:
                return str(value.encode('utf-8'))

    def sortFields(self, fields):
        return sorted(fields, key=self.fieldsOrder)

    def fieldsOrder(self, field):
        order = {
            'title': 1,
            'date': 2,
            'referencenumber': 3,
            'url': 4,
            'company': 5,
            'city': 6,
            'state': 7,
            'country': 8,
            'location': 8,
            'postalcode': 9,
            'description': 10,
            'salary': 11,
            'education': 12,
            'jobtype': 13,
            'category': 14,
            'experience': 15,
            'cpc': 16
        }
        if field['name'] in order:
            return order[field['name']]
        return 1


class XmlExportPipeline(object):

    item_count = 0

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        self.azurestorage = None

    @classmethod
    def from_crawler(self, crawler):
        self.logger = logging.getLogger('XmlExportPipeline')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('111:%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.warning('XmlExportPipeline.from_crawler')

        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        if crawler.spider.name not in ['jobintree', 'jobintree2']:
            return pipeline

    def spider_opened(self, spider):
        if spider.name in ['general3', 'general4']:
            return
        self.logger.warning('XmlExportPipeline.spider_opened')
        if type(spider.feed) is not list:
            self.feed_name = spider.settings.get('FEED_DIR', '') + '/%s' % spider.feed
            if (hasattr(spider, 'toAzure') and spider.toAzure is not None) or  spider.settings.get('AZURE_ENABLE', False):
                self.azurestorage = AzureStorage(spider.settings.get('AZURE_ACCOUNT_NAME', ''), spider.settings.get('AZURE_ACCOUNT_KEY', ''))
                self.azurestorage.enable(True)
            fp = tempfile.TemporaryFile()
            self.files[spider] = fp
            item_element = spider.item
            root_element = spider.root
            self.exporter = MyXmlItemExporter(fp, item_element=item_element, root_element=root_element, spider=spider)
            self.exporter.setPublisher(spider.publisher)
            self.exporter.setPublisherUrl(spider.publisherurl)
            self.exporter.start_exporting()
        else:
            self.feed_name = {}
            self.exporter = {}
            for feed in spider.feed:
                self.feed_name[feed] = spider.settings.get('FEED_DIR_INDEED', '') + '/%s' % feed
                fp = tempfile.TemporaryFile()
                self.files[feed] = fp
                item_element = spider.item
                root_element = spider.root
                self.exporter[feed] = MyXmlItemExporter(fp, item_element=item_element, root_element=root_element, spider=spider)
                self.exporter[feed].setPublisher(spider.publisher)
                self.exporter[feed].setPublisherUrl(spider.publisherurl)
                self.exporter[feed].start_exporting()

    def spider_closed(self, spider):
        self.logger.warning('XmlExportPipeline.spider_closed')
        if spider.name in ['general3', 'general4']:
            return
        if type(spider.feed) is not list:
            self.exporter.finish_exporting()
            if hasattr(spider, 'geocode'):
                spider.geocode.saveCache()
            spider.store.saveHistory()
            stats = self.stats.get_stats()
            if 'log_count/ERROR' in stats:
                fp = self.files.pop(spider)
                fp.close()
            else:
                try:
                    feed_file = open(self.feed_name, 'w+b')
                    full_path_to_file = os.path.abspath(self.feed_name)
                except IOError, ex:
                    feed_file = open('%s' % spider.feed, 'w+b')
                    full_path_to_file = os.path.abspath(spider.feed)
                fp = self.files.pop(spider)
                fp.seek(0)
                feed_file.write(fp.read())
                fp.close()
                feed_file.close()
                if self.azurestorage is not None:
                    azure_url = self.azurestorage.push_file(full_path_to_file)
                    self.logger.info(azure_url)
                if spider.board is not None:
                    date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
                    spider.store.changeFeedUpdated(spider.board['id'], date)
        else:
            for exporter in self.exporter.values():
                exporter.finish_exporting()
            if hasattr(spider, 'geocode'):
                spider.geocode.saveCache()
            stats = self.stats.get_stats()
            if 'log_count/ERROR' in stats:
                fp = self.files.pop(spider)
                fp.close()
            else:
                try:
                    path = spider.settings.get('FEED_DIR_INDEED', '')
                    files = os.listdir(path)
                    for fl in files:
                        os.remove('/'.join([path, fl]))
                except Exception, ex:
                    pass
                for feed in spider.feed:
                    try:
                        feed_file = open(self.feed_name[feed], 'w+b')
                    except IOError, ex:
                        feed_file = open('%s' % feed, 'w+b')
                    fp = self.files[feed]
                    fp.seek(0)
                    feed_file.write(fp.read())
                    fp.close()
                    feed_file.close()

    def process_item(self, item, spider):
        self.logger.warning('XmlExportPipeline.process_item')
        if type(spider.feed) is not list:
            if spider.board_feed_settings is None:
                self.exporter.export_item(item)
                return item
            elif 'required_fields' not in spider.board_feed_settings or spider.board_feed_settings['required_fields'] is None:
                self.exporter.export_item(item)
                return item
            else:
                required_fields = map(lambda i: i.strip(), spider.board_feed_settings['required_fields'].split(','))
                #f = open('item.txt', 'w')
                #f.write(str(required_fields))
                #.close()
                for field in required_fields:
                    if field not in item:
                        return item
                self.exporter.export_item(item)
                return item
        else:
            try:
                feed = spider.company2feedname(item['company'][0])
                self.exporter[feed].export_item(item)
            except KeyError, ex:
                pass
            else:
                return item




class JobInTreeExportPipeline(object):

    logger = None
    item_count = 0

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        self.azurestorage = None
        self.item_count = 0

    @classmethod
    def from_crawler(self, crawler):

        self.logger = logging.getLogger('JobInTreeExportPipeline')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('222:%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)


        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        if crawler.spider.name in ['jobintree', 'jobintree2']:
            return pipeline

    def spider_opened(self, spider):
        self.feed_name = spider.settings.get('FEED_DIR', '') + '/%s' % spider.feed
        fp = tempfile.TemporaryFile()
        self.files[spider] = fp
        self.exporter = JobInTreeExporter(fp, spider=spider)
        if (hasattr(spider, 'toAzure') and spider.toAzure is not None) or spider.settings.get('AZURE_ENABLE', False):
            self.azurestorage = AzureStorage(spider.settings.get('AZURE_ACCOUNT_NAME', ''),spider.settings.get('AZURE_ACCOUNT_KEY', ''))
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        if hasattr(spider, 'geocode'):
            spider.geocode.saveCache()
        spider.store.saveHistory()
        stats = self.stats.get_stats()
        if 'log_count/ERROR' in stats:
            fp = self.files.pop(spider)
            fp.close()
        else:
            try:
                feed_file = open(self.feed_name, 'w+b')
                full_path_to_file = os.path.abspath(self.feed_name)
            except IOError, ex:
                feed_file = open('%s' % spider.feed, 'w+b')
                full_path_to_file = os.path.abspath('%s' % spider.feed)
            fp = self.files.pop(spider)
            fp.seek(0)
            feed_file.write(fp.read())
            fp.close()
            feed_file.close()
            if self.azurestorage is not None:
                azure_url = self.azurestorage.push_file(full_path_to_file)
                self.logger.info(azure_url)
            if spider.board is not None:
                date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
                spider.store.changeFeedUpdated(spider.board['id'], date)

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        self.item_count += 1
        return item


class JobInTreeExporter(PprintItemExporter):

    encode = 'cp1252'

    def __init__(self, file, **kwargs):
        self.spider = kwargs.pop('spider')
        super(self.__class__, self).__init__(file, **kwargs)

    def export_item(self, item):
        self.file.write('\n')
        fields = []
        itemdict = dict(self._get_serialized_fields(item, include_empty=True))
        for name, value in itemdict.items():
            if value is None:
                #value = ''
                continue     #for remove tags which have not value
            fields.append({'name': name, 'value': value})
        fields = self.sortFields(fields)
        for field in fields:
            line = '='.join([field['name'], field['value']])
            self.file.write(self.encodeString(line))
            self.file.write(self.encodeString('\n'))

    def start_exporting(self):
        self.file.write(self.encodeString('JOBINTREE\n'))
        self.file.write(self.encodeString('VERSION=1.0\n'))

    def serialize_field(self, field, name, value):
        if value is None:
            return super(self.__class__, self).serialize_field(field, name, value)
        if type(value) is list:
            value = ','.join(map(self.convertValue, value))
        else:
            value = self.convertValue(value)
        return super(self.__class__, self).serialize_field(field, name, value)

    def convertValue(self, value):
        if type(value) is datetime:
            return self.dateFormat(value, self.spider.date_format)
        else:
            try:
                res = str(value)
            except Exception, ex:
                res = unicode(value)
            return res

    def dateFormat(self, value=None, date_format=None):
        if date_format is None:
            date_format = '%Y-%m-%dT%H:%M:%S'
        if value is not None and type(value) is datetime:
            value = value.strftime(date_format)
        else:
            value = datetime.now().strftime(date_format)
        return self.encodeString(value)

    def encodeString(self, string):
        try:
            return string.decode('utf-8').encode(self.encode)
        except UnicodeEncodeError, ex:
            return string.encode(self.encode, errors='replace')

    def sortFields(self, fields):
        return sorted(fields, key=self.fieldsOrder)

    def fieldsOrder(self, field):
        order = {
            'ANNOUNCER': 1,
            'RECRUITER': 2,
            'MAXCV': 3,
            'CONTRACT': 4,
            'JOBSTATUS': 5,
            'EXPERIENCE': 6,
            'PAY': 7,
            'AVAILABILITY': 8,
            'CONTACT': 9,
            'COUNTRY': 10,
            'REGION': 11,
            'DEPARTMENT': 12,
            'CITY': 13,
            'SECTOR': 14,
            'FUNCTION': 15,
            'REFERENCE': 16,
            'TITLE': 17,
            'LINK': 18,
            'DESCJOB': 19,
            'DESCCOMPANY': 20,
            'DESCPROFIL': 21,
            'DESCINFO': 22,

        }
        if field['name'] in order:
            return order[field['name']]
        return 100



