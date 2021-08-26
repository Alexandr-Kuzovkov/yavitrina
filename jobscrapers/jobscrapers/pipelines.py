# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import time
from scrapy import signals
from scrapy.exporters import XmlItemExporter
import tempfile
import math
import six
import os
from jobscrapers.extensions import PgSQLStoreScrapers
from pprint import pprint
import uuid
import unicodedata, re, json
from scrapy.exporters import PprintItemExporter
import logging
import base64
from bs4 import BeautifulSoup
import html2text
from langdetect import detect

FILE_EXPORT_SPIDERS = [
    'hays', 'pole-emploi', 'synergie', 'sandyou', 'spie', 'leboncoin', 'germanpersonal',
    'reannotate', 'indeed', 'monster', 'adh', 'job4', 'chaussea', 'leboncoin-raw', 'spie-raw',
    'bouyguestelecom', 'jobintree', 'indeed-fr', 'dictionnaire', 'esco', 'public-employer',
    'employmentcrossing', 'emploilibre', 'livecareer', 'indeed-jobs', 'pole-emploi-api', 'onisep',
    'auvergnerhonealpes', 'carriereonline', 'public-employer2', 'leboncoin2', 'ec-europa', 'keljob',
    'vivastreet', 'pole-emploi-wood', 'leboncoin3', 'glassdoor', 'agent-co', 'emploi-territorial',
    'vitijob', 'inzejob', 'envirojob', 'jobaviz', 'apec', 'ouestfrance-emploi', 'leboncoin-api'
]


def create_folder(directory):
    try:
        if not os.path.exists(directory):
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


class JobscrapersPipeline(object):
    def process_item(self, item, spider):
        return item

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass



class MyXmlItemExporter(XmlItemExporter):

    publisher = ''
    publisherurl = ''

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


    def setPublisher(self, publisher):
        self.publisher = publisher

    def setPublisherUrl(self, publishedurl):
        self.publisherurl = publishedurl

    def serialize_field(self, field, name, value):
        if type(value) is list:
            value = value[0]
        if name == 'date':
            value = time.strftime('%Y-%m-%d', time.localtime(int(value)))
        if name == 'utime':
            value = self.dateFormat(value)
        value = ''.join(['<![CDATA[', value, ']]>'])
        return super(self.__class__, self).serialize_field(field, name, value)

    def dateFormat(self, value=None):
        if value is None:
            date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time())))
        else:
            date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(value)))
        if time.timezone < 0:
            date += '+%02d:%02d' % (int(math.modf(abs(time.timezone)/3600.0)[1]), int(math.modf(abs(time.timezone)/3600.0)[0]*60))
        else:
            date += '-%02d:%02d' % (int(math.modf(abs(time.timezone)/3600.0)[1]), int(math.modf(abs(time.timezone)/3600.0)[0]*60))
        return date




class XmlExportPipeline(object):

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        self.store = PgSQLStoreScrapers()
        self.employerMetadata = {}

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        if crawler.spider.name not in FILE_EXPORT_SPIDERS:
            return pipeline

    def spider_opened(self, spider):
        if hasattr(spider, 'company_name') and spider.company_name is not None:
            self.feed_name = spider.settings.get('FEED_DIR', '') + '/%s_jobs.xml' % spider.company_name.replace(' ', '_').lower()
        else:
            self.feed_name = spider.settings.get('FEED_DIR', '') + '/%s_jobs.xml' % spider.name
        self.employerMetadata['spider'] = spider.name
        if hasattr(spider, 'employer_id') and spider.employer_id is not None:
            self.employerMetadata['employer_id'] = spider.employer_id
        fp = tempfile.TemporaryFile()
        self.files[spider] = fp
        self.exporter = MyXmlItemExporter(fp, item_element='job', root_element='source')
        self.exporter.setPublisher(spider.publisher)
        self.exporter.setPublisherUrl(spider.publisherurl)
        self.exporter.start_exporting()


    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        if hasattr(spider, 'geocode'):
            spider.geocode.saveCache()
        stats = self.stats.get_stats()
        if 'log_count/ERROR' in stats:
            fp = self.files.pop(spider)
            fp.close()
        else:
            try:
                feed_file = open(self.feed_name, 'w+b')
            except IOError, ex:
                if hasattr(spider, 'company_name') and spider.company_name is not None:
                    feed_file = open('%s_jobs.xml' % spider.company_name.replace(' ', '_').lower(), 'w+b')
                else:
                    feed_file = open('%s_jobs.xml' % spider.name, 'w+b')
            fp = self.files.pop(spider)
            fp.seek(0)
            feed_file.write(fp.read())
            fp.close()
            feed_file.close()
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            employer = self.store.getEmployerByMetadata(self.employerMetadata)
            self.store.changeFeedUpdated(employer, date)
            self.store.updateFeedUrl(employer, spider.settings.get('FEED_BASE_URL', '') + self.feed_name.split('/').pop())


    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item



class MyFileItemExporter(object):

    spider = None
    dirname = ''

    def __init__(self, spider):
        self.spider = spider
        self.h = html2text.HTML2Text()
        self.h.ignore_emphasis = True
        self.h.ignore_links = True

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', '')
        create_folder(files_dir)
        if hasattr(self.spider, 'dirname') and self.spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)

    def export_item(self, item):
        if hasattr(self.spider, 'lang') and self.spider.lang is not None:
            text = self.h.handle(item['text'][0]).encode('utf-8').strip()
            detected_lang = detect(text.decode('utf-8'))
            #self.spider.logger.info('DETECTED LANG: %s' % detected_lang)
            if detected_lang != self.spider.lang:
                return None
        folder = self.dirname
        png_folder = None
        if 'subfolder' in item:
            folder = os.path.sep.join([folder, item['subfolder'][0]])
            create_folder(folder)
        if 'industry' in item:
            folder = os.path.sep.join([folder, item['industry'][0]])
            create_folder(folder)
        if 'name' in item:
            if 'body' in item:
                name = item['name'][0]
            else:
                name = '.'.join([item['name'][0], 'txt'])
        else:
            name = '.'.join([str(uuid.uuid1()), 'txt'])
        if not hasattr(self.spider, 'drain') or not self.spider.drain:
            if 'body' in item:
                with open(os.path.sep.join([folder.decode('utf-8'), name]), 'wb') as f0:
                    f0.write(item['body'][0])
                return item['name'][0]
            if 'itemtype' in item and item['itemtype'][0] == 'plaintext':
                with open(os.path.sep.join([folder.decode('utf-8'), name]), 'w') as f01:
                    plaintext = self.h.handle(item['text'][0]).encode('utf-8').strip()
                    if len(plaintext) > 0:
                        f01.write(plaintext)
                return item['name'][0]
            if 'png' in item:
                png_folder = os.path.sep.join([folder, 'png'])
                create_folder(png_folder)
                with open(os.path.sep.join([png_folder.decode('utf-8'), '.'.join([name[:-4], 'png'])]), 'wb') as f1:
                    f1.write(base64.b64decode(item['png'][0]))
            if not hasattr(self.spider, 'min_len') or len(item['text'][0]) > self.spider.min_len:
                with open(os.path.sep.join([folder.decode('utf-8'), name]), 'w') as f2:
                    f2.write(self.prepare_item(item))
                if os.path.getsize(os.path.sep.join([folder.decode('utf-8'), name])) == 0:
                    os.remove(os.path.sep.join([folder.decode('utf-8'), name]))

    def prepare_item(self, item):
        if self.spider.name in [
            'indeed', 'leboncoin-raw', 'spie-raw', 'bouyguestelecom', 'jobintree', 'indeed-fr', 'livecareer',
            'indeed-jobs', 'public-employer2', 'pole-emploi-api', 'ec-europa', 'auvergnerhonealpes', 'carriereonline',
            'keljob', 'vivastreet', 'pole-emploi-wood', 'leboncoin3', 'glassdoor', 'agent-co', 'emploi-territorial',
            'vitijob', 'inzejob', 'envirojob', 'jobaviz', 'apec', 'ouestfrance-emploi', 'leboncoin-api'
        ]:
            #soup = BeautifulSoup(item['text'][0])
            #text = soup.get_text()
            text = self.h.handle(item['text'][0]).encode('utf-8').strip()
            return text
        elif self.spider.name in ['dictionnaire']:
            rows = []
            if 'synonyms' in item and len(item['synonyms'][0]) > 0:
                rows.append('%s => %s' % (item['synonyms'][0].encode('utf-8'), item['word'][0].encode('utf-8')))
            if 'definition' in item and len(item['definition'][0]) > 0:
                rows.append('%s => %s' % (item['definition'][0].encode('utf-8'), item['word'][0].encode('utf-8')))
            return '\n'.join(rows)
        elif self.spider.name in ['esco']:
            if 'definition' in item and len(item['definition'][0]) > 0:
                if 'synonyms' in item and len(item['synonyms'][0]) > 0:
                    return '%s => %s' % (item['definition'][0].encode('utf-8'), item['synonyms'][0].encode('utf-8'))
                else:
                    return '%s => %s' % (item['definition'][0].encode('utf-8'), item['definition'][0].encode('utf-8'))
        else:
            return '\n'.join(map(lambda i: i[0].encode('utf-8'), [item['title'], item['subtitle'], item['description']]))

    def finish_exporting(self):
        if hasattr(self.spider, 'industries'):
            pprint(self.spider.industries)
        if hasattr(self.spider, 'industries2'):
            pprint(self.spider.industries2)
        pass


class FileExportPipeline(object):

    annotation_list = [
        'auvergnerhonealpes', 'carriereonline', 'leboncoin2', 'pole-emploi-api', 'ec-europa', 'keljob',
        'vivastreet', 'pole-emploi-wood', 'leboncoin3', 'glassdoor', 'agent-co', 'emploi-territorial',
        'vitijob', 'inzejob', 'envirojob', 'jobaviz', 'apec', 'ouestfrance-emploi', 'leboncoin-api'
    ]

    def __init__(self):
        self.files = {}
        self.logger = logging.getLogger('FileExportPipeline')
        fh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        if crawler.spider.name in FILE_EXPORT_SPIDERS:
            return pipeline

    def spider_opened(self, spider):
        if spider.name in ['hays', 'pole-emploi', 'indeed', 'leboncoin-raw', 'spie-raw', 'bouyguestelecom', 'jobintree',
                           'indeed-fr', 'dictionnaire', 'esco', 'employmentcrossing', 'emploilibre', 'livecareer', 'indeed-jobs',
                           'onisep', 'public-employer2'
                           ]:
            self.exporter = MyFileItemExporter(spider)
        elif spider.name in self.annotation_list:
            self.exporter = MyFileItemExporter(spider)
            self.exporter_ann = MyFileAnnotationItemExporter(spider)
            self.exporter_ann.start_exporting()
        elif spider.name in ['germanpersonal']:
            self.exporter = GermanPersonalXmlExporter(spider)
        elif spider.name in ['reannotate']:
           self.exporter = ReannotateExporter(spider)
        else:
            self.exporter = MyFileAnnotationItemExporter(spider)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        #spider.logger.info(str(set(spider.urls)))
        if hasattr(spider, 'categories'):
            self.logger.info(str(spider.categories))
        if hasattr(spider, 'category_codes'):
            self.logger.info(str(spider.category_codes))

    def process_item(self, item, spider):
        if spider.name in self.annotation_list and 'itemtype' in item and item['itemtype'][0] == 'annotation':
            self.exporter_ann.export_item(item)
        else:
            self.exporter.export_item(item)
        return item


class MyFileAnnotationItemExporter(PprintItemExporter):

    spider = None
    dirname = ''
    dirname_src = ''
    all_chars = (unichr(i) for i in xrange(0x110000))
    control_chars = ''.join(c for c in all_chars if unicodedata.category(c) == 'Cc')
    # or equivalently and much more efficiently
    #control_chars = ''.join(map(unichr, range(0, 32) + range(127, 160)))
    control_char_re = re.compile('[%s]' % re.escape(control_chars))
    repchars = ['(', ')', '[', ']' '=', '#', '&', '%', ' ', '\t', '\n', u'•', u'']
    punctuations = [',', '.', ':', ';', '!', '?', '/', '...']
    fields_to_export = None
    order = [
        'name',
        'title',
        'city',
        'postal_code',
        'ref',
        'job_type_l',
        'job_type',
        'salary_l',
        'salary',
        'category_l',
        'category',
        'city_l',
        'city2',
        'postal_code2',
        'experience_l',
        'experience',
        'education_l',
        'education',
        'start_time_l',
        'start_time',
        'end_time_l',
        'end_time',
        'experience2_l',
        'experience2',
        'desc1',
        'desc2',
        'desc3',
        'desc4'
    ]

    def __init__(self, spider):
        self.spider = spider
        if hasattr(self.spider, 'order') and len(self.spider.order) > 0:
            self.order = self.spider.order

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', '')
        create_folder(files_dir)
        if hasattr(self.spider, 'dirname') and self.spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)
        clear_folder(self.dirname)
        if hasattr(self.spider, 'reannotate_only') and not self.spider.reannotate_only:
            self.dirname_src = os.path.sep.join([self.dirname, 'src'])
            create_folder(self.dirname_src)
            #we will not clear 'src' directory
            #clear_folder(self.dirname_src)

    def export_item(self, item):
        if hasattr(self.spider, 'min_item_len') and len(item) < self.spider.min_item_len:
            self.spider.logger.warning('!!!!!!!!!!! Item name "{name}" does not contains enough data'.format(name=item['name'][0]))
            return item
        folder = self.dirname
        src_folder = os.path.sep.join([folder, 'src'])
        if 'subfolder' in item:
            folder = os.path.sep.join([folder, item['subfolder'][0]])
            create_folder(folder)
        if 'industry' in item:
            folder = os.path.sep.join([folder, item['industry'][0]])
            create_folder(folder)
            src_folder = os.path.sep.join([folder, 'src', item['industry'][0]])
        create_folder(src_folder)
        if 'name' in item:
            name = '.'.join([item['name'][0], 'txt'])
        else:
            name = '.'.join([uuid.uuid1(), 'txt'])
        if 'png' in item:
            png_folder = os.path.sep.join([folder, 'png'])
            create_folder(png_folder)
            with open(os.path.sep.join([png_folder.decode('utf-8'), '.'.join([name[:-4], 'png'])]), 'wb') as f1:
                f1.write(base64.b64decode(item['png'][0]))
        if hasattr(self.spider, 'drain') and not self.spider.drain:
            with open(os.path.sep.join([folder.decode('utf-8'), name]), 'w') as f:
                f.write(self.prepare_item(item))
            if hasattr(self.spider, 'reannotate_only') and not self.spider.reannotate_only:
                with open(os.path.sep.join([src_folder, name]), 'w') as f2:
                    f2.write(self.serialize_item(item))


    def serialize_item(self, item):
        return json.dumps(dict(item))

    def prepare_item(self, item):
        item_tokens = []
        fields = []
        itemdict = dict(self._get_serialized_fields(item, include_empty=False))
        for name, value in itemdict.items():
            if value is None or len(value) == 0 or not value[0] or len(value[0]) == 0:
                # value = ''
                continue  # for remove tags which have not value
            fields.append({'name': name, 'value': value})
        fields = self.sortFields(fields, self.spider, item['name'][0])
        for field in fields:
            item_key_tokens = self.tokenize_item_field(field['value'], field['name'])
            if item_key_tokens:
                item_tokens += item_key_tokens
        item_tokens = self.post_processing_item_tokens(item_tokens)
        return '\n'.join(map(lambda i: '\t'.join([i[0], i[1]]), item_tokens)).encode('utf-8')

    def tokenize_item_field(self, value, key):
        if type(value) is list and len(value) > 1:
            punctuations = self.punctuations[:]
            if key not in ['city', 'city2'] and '-' not in punctuations:
                punctuations.append('-')
            for c in punctuations:
                if c in value[0]:
                    value[0] = self.replace(value[0], [c], ' '+c+' ')
            tokens = map(lambda k: self.add_annotation(k, value[1], key), filter(lambda j: len(j) > 0, map(lambda s: self.remove_control_chars(self.replace(s, self.repchars)), map(lambda i: i.strip(), value[0].strip().split(' ')))))
            tokens = self.post_processing_item_key_tokens(tokens, key)
            return tokens
        return None

    def replace(self, text, chars, chrep=''):
        for c in chars:
            text = text.replace(c, chrep)
        return text

    def add_annotation(self, token, annotation, key):
        punctuations = self.punctuations[:]
        if key not in ['city', 'city2'] and '-' not in punctuations:
            punctuations.append('-')
        if token in punctuations:
            annotation = 'O'
        else:
            annotation = self.change_item_key_annotation(key, token, annotation)
        return [token, annotation]

    def remove_control_chars(self, s):
        return self.control_char_re.sub('', s)

    def finish_exporting(self):
        pass

    def sortFields(self, fields, spider, name):
        if hasattr(spider, 'orders') and len(spider.orders) > 0:
            self.order = spider.orders[name]
        return sorted(fields, key=self.fieldsOrder)

    def fieldsOrder(self, field):
        if field['name'] in self.order:
            return self.order.index(field['name'])
        return len(self.order)

    def change_item_key_annotation(self, item_key, token, annotation):
        if self.spider.name in ['spie', 'chaussea']:
            if item_key == 'city':
                if token.lower() in self.spider.countries:
                    annotation = 'country'
                elif token.lower() in self.spider.cities:
                    annotation = 'city'
                elif token.isdigit() and len(token) >= 5:
                    annotation = 'postal_code'
                else:
                    annotation = 'O'
            if item_key == 'experience_duration':
                if token.isdigit():
                    annotation = 'experience_duration'
                elif token.lower() in [u'month', u'year', u'week', u'day', u'quarter', u'months', u'years', u'weeks', u'days', u'quarters', u'mois', u'année', u'année', u'années', u'ans', u'mois', u'semaine', u'jour', u'décénnie']:
                    annotation = 'experience_duration'
                else:
                    annotation = 'O'
        elif self.spider.name in ['leboncoin', 'leboncoin2']:
            if item_key == 'location':
                if token.lower() in self.spider.cities:
                    annotation = 'city'
                elif token.isdigit() and len(token) >= 5:
                    annotation = 'postal_code'
                else:
                    annotation = 'O'
            if item_key == 'experience_duration':
                if token.isdigit():
                    annotation = 'experience_duration'
                elif token.lower() in [u'month', u'year', u'week', u'day', u'quarter', u'months', u'years', u'weeks', u'days', u'quarters', u'mois', u'année', u'année', u'années', u'ans', u'mois', u'semaine', u'jour', u'décénnie']:
                    annotation = 'experience_duration'
                else:
                    annotation = 'O'
        elif self.spider.name in ['adh']:
            if item_key in ['location', 'location2']:
                if token.lower() in self.spider.cities:
                    annotation = 'city'
        elif self.spider.name == 'job4':
            if item_key == 'salary':
                if token.isdigit() or token in [u'K€']:
                    annotation = 'salary'
                else:
                    annotation = 'O'
        return annotation

    def post_processing_item_key_tokens(self, tokens, item_key):
        if self.spider.name in ['spie', 'chaussea']:
            if item_key == 'city':
                postal_code_present = False
                for token in tokens:
                    if token[1] == 'postal_code':
                        postal_code_present = True
                annotations = []
                for index in range(len(tokens)):
                    if tokens[index][1] == 'city' and 'postal_code' not in annotations and postal_code_present:
                        tokens[index][1] = 'O'
                    annotations.append(tokens[index][1])
        return tokens

    def post_processing_item_tokens(self, tokens):
        if self.spider.name in ['leboncoin', 'leboncoin2', 'spie', 'synergie', 'monster', 'adh', 'chaussea']:
            d1 = {} #many words
            d2 = {} #one word
            city = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'city', tokens))
            company = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'company', tokens))
            contrat_type = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'contrat_type', tokens))
            d1['company'] = company
            d1['city'] = city
            d1['contrat_type'] = contrat_type
            if self.spider.name in ['synergie', 'leboncoin', 'leboncoin2']:
                position = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'position', tokens))
                postal_code = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'postal_code', tokens))
                education = map(lambda j: j[0].lower(), filter(lambda i: i[1] == 'education', tokens))
                d1['position'] = position
                d2['postal_code'] = postal_code
                d1['education'] = education

            a_tokens_ind = [] #tokens indices with annotations
            for i in range(len(tokens)):
                if tokens[i][1] != 'O':
                    a_tokens_ind.append(i)
            na_tokens_ind = []  # tokens indices without annotations
            for i in range(len(tokens)):
                if tokens[i][1] == 'O' and tokens[i][0] not in self.punctuations:
                    na_tokens_ind.append(i)
            #company may have several words
            for annotation, words in d1.items():
                if len(words) == 0:
                    continue
                for index in range(len(na_tokens_ind) - len(words)):
                    match = True
                    for i in range(index, len(words) + index):
                        if words[i-index] != tokens[na_tokens_ind[i]][0].lower():
                            match = False
                    if match:
                        for i in range(index, len(words) + index):
                            tokens[na_tokens_ind[i]][1] = annotation
            #city, contrat type have one word
            for annotation, words in d2.items():
                if len(words) == 0:
                    continue
                for index in range(len(tokens)):
                    if tokens[index][0].lower() in words:
                        tokens[index][1] = annotation

        return tokens


class GermanPersonalXmlExporter(PprintItemExporter):
    spider = None
    dirname = ''
    fh_feed = None
    fh_report = None
    categories_full = {u'Recht': 9, u'Ingenieurswesen': 10, u'Umwelt/Verkehrspolitik/Energie': 7, u'Handwerk': 465,
     u'Controlling/Finanz- und Rechnungswesen': 109, u'Projektmanagement': 26,
     u'Medienproduktion/Medientechnik (Film, Funk, Fernsehen, Verlag)': 4, u'Kundendienst/Service/CallCenter': 331,
     u'Banking und Finanzdienstleistung': 31, u'Montage/Inbetriebnahme': 291,
     u'Hilfskraft/Aushilfe/Hilfst\xe4tigkeit': 229, u'Forschung u. Entwicklung (F&E)': 14,
     u'Organisation/Projekte/Beratung': 26, u'Land-/Forstwirtschaft': 12,
     u'Qualit\xe4tsmanagement (Produkt, Prozess, Kontrolle etc.)': 68, u'Personalwesen/HR': 147,
     u'Sozialwesen/Pflege': 115, u'Sonstige gewerbliche Berufe': 203, u'Vertrieb u. Verkauf': 54,
     u'Informationstechnologie (IT)': 30, u'Sonstige T\xe4tigkeitsbereiche': 141, u'Hotel/Gastronomie/Kantine': 54,
     u'Sonstige kaufm\xe4nnische Berufe': 112, u'Produktion/Produktionsplanung': 609,
     u'Marketing/Kommunikation/Werbung': 13, u'Medizin/Pharma/Pflege': 102, u'Erziehung/Bildung/Therapie': 8,
     u'Elektrik/Elektronik/Elektrotechnik': 309, u'Top Management/Gesch\xe4ftsf\xfchrung': 1, u'Technische Berufe': 173,
     u'Logistik u. Materialwirtschaft (Einkauf, Lager, Transport v. G\xfcter u. Personen)': 1142,
     u'Verwaltung/Dienstleistung': 78, u'Sekretariat/Assistenz/Office Management': 225}
    categories_path = {}
    categories_curr = {}

    def __init__(self, spider):
        self.spider = spider
        sum_count = 0
        for count in self.categories_full.values():
            sum_count += count
        for category_name, count in self.categories_full.items():
            self.categories_path[category_name] = int(math.ceil(float(count) * spider.limit/sum_count))

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', '')
        create_folder(files_dir)
        if hasattr(self.spider, 'dirname') and self.spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)
        folder = self.dirname
        name = '.'.join([self.spider.name, 'xml'])
        name2 = '.'.join([self.spider.name, 'report', 'txt'])
        self.fh_feed = open(os.path.sep.join([folder.decode('utf-8'), name]), 'w')
        self.fh_report = open(os.path.sep.join([folder.decode('utf-8'), name2]), 'w')
        self.fh_feed.write('<Advertices>')

    def export_item(self, item):
        if hasattr(self.spider, 'drain') and not self.spider.drain:
            if item['category'] in self.categories_curr:
                self.categories_curr[item['category']] += 1
            else:
                self.categories_curr[item['category']] = 1
            if hasattr(self.spider, 'drain') and not self.spider.drain:
                if hasattr(self.spider, 'full') and not self.spider.full:
                    if self.categories_curr[item['category']] > self.categories_path[item['category']]:
                        return item
                self.fh_feed.write(self.prepare_item(item))
                self.fh_report.writelines([item['original_url'], '\n', item['title'].encode('utf-8'), '\n', item['external_id'], '\n', '-' * 60, '\n'])


    def prepare_item(self, item):
        return ''.join(map(lambda i: i[0].encode('utf-8'), [['<PositionOpening>'], item['PositionRecordInfo'], item['PositionPostings'], item['PositionProfile'], ['</PositionOpening>']]))

    def finish_exporting(self):
        self.fh_feed.write('</Advertices>')
        self.fh_feed.close()
        self.fh_report.write(str(self.categories_path).encode('utf-8'))
        self.fh_report.close()


class ReannotateExporter(object):

    spider = None
    dirname = ''

    def __init__(self, spider):
        self.spider = spider

    def start_exporting(self):
        files_dir = self.spider.settings.get('FILES_DIR', '')
        create_folder(files_dir)
        if hasattr(self.spider, 'spiderdirname') and self.spider.spiderdirname is not None:
            self.dirname = os.path.sep.join([files_dir, self.spider.spiderdirname + '-reannotate'])
        else:
            self.dirname = os.path.sep.join([files_dir, self.spider.name])
        create_folder(self.dirname)
        clear_folder(self.dirname)

    def export_item(self, item):
        dest_dir = self.dirname
        files_dir = self.spider.settings.get('FILES_DIR', '')
        source_dir = os.path.sep.join([files_dir, self.spider.spiderdirname])
        list_of_files = os.listdir(source_dir)
        exporter = MyFileAnnotationItemExporter(self.spider)
        exporter.spider.name = item['spidername']
        for name in list_of_files:
            with open(os.path.sep.join([source_dir, name]), 'r') as fi:
                tokens = filter(lambda t: len(t) == 2,  map(lambda line: line.split('\t'), fi.read().split('\n')))
                #pprint(tokens)
                new_tokens = exporter.post_processing_item_tokens(tokens)
                with open(os.path.sep.join([dest_dir, name]), 'w') as fo:
                    fo.write('\n'.join(map(lambda i: '\t'.join([i[0], i[1]]), new_tokens)))

    def finish_exporting(self):
        pass
