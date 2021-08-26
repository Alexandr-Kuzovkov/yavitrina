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
from os import path
from configparser import ConfigParser
import logging
logger = logging.getLogger()
from fibois.extensions import Elastic
from pprint import pprint
from elasticsearch.helpers import bulk
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from fibois.notifier import Notifier


PAGE_RESULTS_HTML_INDEX = None
JOB_DETAILS_HTML_INDEX = None
REQUEST_TIMEOUT = 60

PAGE_RESULTS_HTML_INDEX_EUROPA = 'scrapping-page-results-html_ec.europa.eu'
JOB_DETAILS_HTML_INDEX_EUROPA = 'scrapping-job-details-html_ec.europa.eu'
SCRAPING_INDEX_EUROPA = 'scraped-jobs_ec.europa.eu'

required_scraped_fields = {
    'common': ['content', 'title', 'url', 'location'],
    'onf.fr': ['content', 'title', 'url', 'location'],
}

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


class FiboisJsonPipeline(object):
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
        files_dir = spider.settings.get('FILES_DIR', 'files')
        create_folder(files_dir)
        if hasattr(spider, 'dirname') and spider.dirname is not None:
            self.dirname = os.path.sep.join([files_dir, spider.dirname])
        else:
            self.dirname = os.path.sep.join([files_dir, spider.name])
        create_folder(self.dirname)

        if hasattr(spider, 'jobboard') and spider.jobboard is not None:
            self.feed_name = os.path.sep.join([self.dirname, spider.jobboard + '.json'])
        else:
            self.feed_name = os.path.sep.join([self.dirname, spider.name + '.json'])
        logger.debug('feed_name: {feed_name}'.format(feed_name=self.feed_name))
        fp = tempfile.TemporaryFile()
        self.files[spider] = fp
        self.exporter = MyJsonItemExporter(fp)
        self.exporter.start_exporting()

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        if hasattr(spider, 'geocode'):
            spider.geocode.saveCache()
        stats = self.stats.get_stats()
        #if 'log_count/ERROR' in stats:
        #    fp = self.files.pop(spider)
        #    fp.close()
        #else:
        try:
            feed_file = open(self.feed_name, 'w+b')
        except IOError, ex:
            if hasattr(spider, 'jobboard') and spider.jobboard is not None:
                feed_file = open('%s_jobs.json' % spider.jobboard.replace(' ', '_').lower(), 'w+b')
            else:
                feed_file = open('%s_jobs.json' % spider.name, 'w+b')
        fp = self.files.pop(spider)
        fp.seek(0)
        feed_file.write(fp.read())
        fp.close()
        feed_file.close()


    def process_item(self, item, spider):
        if not spider.drain:
            self.exporter.export_item(item)
        return item

class FiboisElassticPipeline(object):

    notifier = None

    def __init__(self):
        self.logger = logging.getLogger('FiboisElasticPipeline')
        fh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.es = None

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        return pipeline

    def spider_opened(self, spider):
        global PAGE_RESULTS_HTML_INDEX
        global JOB_DETAILS_HTML_INDEX
        CONFIG_FILE = spider.settings.get('INI_FILE', '/home/root/config.ini')
        try:
            if path.isfile(CONFIG_FILE):
                config = ConfigParser()
                with open(CONFIG_FILE) as config_file:
                    config.read_file(config_file)
                    SCRAPING_ES_URL = config['ELASTICSEARCH']['SCRAPING_URL']
                    SCRAPING_ES_INDEX = config['ELASTICSEARCH']['SCRAPING_INDEX']
                    SCRAPESTACK_ACCESS_KEY = config['SCRAPESTACK']['ACCESS_KEY']
                    PAGE_RESULTS_HTML_INDEX = config['ELASTICSEARCH']['PAGE_RESULTS_HTML_INDEX']
                    JOB_DETAILS_HTML_INDEX = config['ELASTICSEARCH']['JOB_DETAILS_HTML_INDEX']
                    if spider.name in ['ec-europa-api']:
                        SCRAPING_ES_INDEX = 'scraped-jobs_ec.europa.eu'
                        PAGE_RESULTS_HTML_INDEX = 'scrapping-page-results-html_ec.europa.eu'
                        JOB_DETAILS_HTML_INDEX = 'scrapping-job-details-html_ec.europa.eu'
                    SMTP_PASSWORD = config['SMTP']['SMTP_PASSWORD']
                    SLACK_WEB_HOOK_POINT = config['SLACK']['SLACK_WEB_HOOK_POINT']
                    spider.scrapestack_access_key = SCRAPESTACK_ACCESS_KEY
                    self.es = Elastic(SCRAPING_ES_URL).getEs()
                    if spider.name in ['expired']:
                        self.exporter = ElasticSearchMantainer(self.es, SCRAPING_ES_INDEX, spider)
                    elif spider.name in ['duplicates']:
                        self.exporter = ElasticSearchDummy(self.es, SCRAPING_ES_INDEX, spider)
                    else:
                        self.exporter = ElasticSearchExporter(self.es, SCRAPING_ES_INDEX, spider)
                    spider.es_exporter = self.exporter
                    options = {
                        'SMTP_PASSWORD': SMTP_PASSWORD,
                        'SMTP_SERVER': spider.settings.get('SMTP_SERVER', 'smtp.gmail.com'),
                        'NOTIFY_EMAILS': spider.settings.get('NOTIFY_EMAILS', []),
                        'NOTIFY_EMAILS_DEV': spider.settings.get('NOTIFY_EMAILS_DEV', []),
                        'EMAIL_FROM': spider.settings.get('EMAIL_FROM', 'contact@myxtramile.com'),
                        'SMTP_PORT': spider.settings.get('SMTP_PORT', 587),
                        'SLACK_CHANNEL': spider.settings.get('SLACK_CHANNEL', '#incident'),
                        'SLACK_WEB_HOOK_POINT': SLACK_WEB_HOOK_POINT,
                    }
                    self.notifier = Notifier(options, self.logger)
                    self.exporter.start_exporting()
            else:
                self.logger.error('Config file {file} not found'.format(file=CONFIG_FILE))
        except Exception as ex:
            self.logger.error('Error {error}'.format(error=ex))


    def spider_closed(self, spider):
        if spider.name in ['expired', 'duplicates']:
            self.exporter.finish_exporting()
        else:
            self.exporter.finish_exporting(self.notifier)

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

class ElasticSearchExporter(object):

    es = None
    index = None
    inserted = 0
    updated = 0
    found = 0
    notcontent = 0
    jobs = []
    page_results = []
    job_details = []
    JOBS_SIZE = 100
    PAGE_RESULTS_SIZE = 10
    JOB_DETAILS_SIZE = 100
    spider = None
    errors = []
    errors_dev = []
    missed_fields = {}
    missed_fields_dev = {}

    def __init__(self, es, index, spider, **kwargs):
        self.es = es
        self.index = index
        self.spider = spider
        self.logger = logging.getLogger('ElasticSearchExporter')
        fh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def start_exporting(self):
        self.logger.info('Start exporting to index "{index}"'.format(index=self.index))

    def finish_exporting(self, notifier):
        self.flush_buffers()
        self.logger.info('Exporting done: inserted {inserted} items; updated {updated} items; found {found}; notcontent {notcontent}'.format(inserted=self.inserted, updated=self.updated, found=self.found, notcontent=self.notcontent))
        if len(self.errors) > 0:
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            message_lines = []
            message_lines.append('{date}<br> <h2>Spider <b>{name}</b></h2> Requred fields not parsed'.format(date=date, name=self.spider.name))
            for line in self.errors:
                message_lines.append(line)
            for field, count in self.missed_fields.items():
                message_lines.append('"{field}": {count}'.format(field=field, count=count))
            message = "<br>\n".join(message_lines)
            self.logger.error(message)
            subject = 'Spider {name} Requred fields not parsed'.format(name=self.spider.name)
            notifier.email(subject, message, 'tpl/email.html')
        if (len(self.errors_dev) > 0) or (len(self.errors) > 0):
            all_errors = self.errors + self.errors_dev
            all_missed_fields = dict(self.missed_fields.items() + self.missed_fields_dev.items())
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            message_lines = []
            message_lines.append('{date}<br> <h2>Spider <b>{name}</b></h2> Requred fields not parsed'.format(date=date, name=self.spider.name))
            for line in all_errors:
                message_lines.append(line)
            for field, count in all_missed_fields.items():
                message_lines.append('"{field}": {count}'.format(field=field, count=count))
            message = "<br>\n".join(message_lines)
            self.logger.error(message)
            subject = 'Spider {name} Requred fields not parsed (dev)'.format(name=self.spider.name)
            notifier.email(subject, message, 'tpl/email.html', True)

    def export_item(self, item):
        data = {}
        jobboard = ''.join(item['jobboard'])
        if jobboard in required_scraped_fields:
            required_fields = required_scraped_fields[jobboard]
        else:
            required_fields = required_scraped_fields['common']
        for key, val in item.items():
            if key in ['html', 'html_content', 'header']:
                continue
            if type(val) is list:
                data[key] = ' '.join(val)
            else:
                data[key] = val
            if key in required_fields:
                if (data[key] is None or len(data[key]) == 0):
                    self.notcontent += 1
                    if type(item['url_origin']) is list:
                        url = ' '.join(item['url_origin'])
                    else:
                        url = item['url_origin']
                    err_message = 'missed field "{field}" was found when export item; (url={url})'.format(field=key, url=url)
                    if key not in ['content']:
                        if key not in self.missed_fields:
                            self.missed_fields[key] = 1
                            self.errors.append(err_message)
                        else:
                            self.missed_fields[key] += 1
                    else:
                        if key not in self.missed_fields_dev:
                            self.missed_fields_dev[key] = 1
                            self.errors_dev.append(err_message)
                        else:
                            self.missed_fields_dev[key] += 1
                    return False
        data['url_status'] = 'valid'
        res = self.insert_data(self.index, data)
        return item

    def insert_data(self, index, data):
        if len(self.jobs) < self.JOBS_SIZE - 1:
            self.jobs.append(data)
            return True
        else:
            try:
                self.jobs.append(data)
                if data['jobboard'] in ['ec.europa.eu-api']:
                    self.insert_data_bulk2(index)
                else:
                    self.insert_data_bulk(index)
                self.jobs = []
                return True
            except Exception as ex:
                self.logger.error(ex)
                return False
            finally:
                self.jobs = []

    def insert_page_results_html(self, data):
        body = {}
        for fld in ['job_board', 'job_board_url', 'page_url', 'offers', 'created_at']:
            if fld == 'offers':
                if 'offers' not in body:
                    body['offers'] = []
                if type(data[fld]) is list:
                    for offer in data[fld]:
                        item = {'html': offer['html'], 'title': offer['title'], 'url': offer['url']}
                        body['offers'].append(item)
                else:
                    raise Exception('"offers" must be a list!')
            elif fld == 'created_at':
                body[fld] = get_current_date()
            else:
                body[fld] = data[fld]
        if len(self.page_results) < self.PAGE_RESULTS_SIZE - 1:
            self.page_results.append(body)
            self.logger.info('page_results_html inserted for page url "{page_url}"'.format(page_url=body['page_url']))
            return True
        else:
            try:
                self.page_results.append(body)
                self.logger.info('page_results_html inserted for page url "{page_url}"'.format(page_url=body['page_url']))
                self.insert_page_results_html_bulk()
                self.page_results = []
                return True
            except Exception as ex:
                self.logger.error(ex)
                return False
            finally:
                self.page_results = []

    def insert_job_details_html(self, data):
        body = {}
        for fld in ['html', 'title', 'url', 'created_at']:
            if fld == 'created_at':
                body[fld] = get_current_date()
            else:
                body[fld] = data[fld]
        if len(self.job_details) < self.JOB_DETAILS_SIZE - 1:
            self.job_details.append(body)
            self.logger.info('job_details_html inserted for url "{url}"'.format(url=body['url']))
            return True
        else:
            try:
                self.job_details.append(body)
                self.logger.info('job_details_html inserted for url "{url}"'.format(url=body['url']))
                self.insert_job_details_html_bulk()
                self.job_details = []
                return True
            except Exception as ex:
                self.logger.error(ex)
                return False
            finally:
                self.job_details = []

    def insert_data_bulk(self, index):
        self.logger.info('batch size: {total}'.format(total=len(self.jobs)))
        should_items = list(map(lambda job: {
            "bool": {
                "must": [
                    {
                        "term": {
                            "search_term.keyword": job['search_term']
                        }
                    },
                    {
                        "term": {
                            "url.keyword": job['url']
                        }
                    }
                ]

            }
        }, self.jobs))
        search_body = {
            "query": {
                "bool": {
                    "should": should_items
                }
            }
        }
        res = self.es.indices.refresh(index=index)
        self.logger.info(str(res))
        res = self.es.search(index=index, body=search_body, track_scores=True, request_timeout=REQUEST_TIMEOUT, size=1000)
        existing_jobs = list(map(lambda i: i['_source'], res['hits']['hits']))
        self.logger.info('existing jobs: {total}'.format(total=len(existing_jobs)))
        self.found += len(existing_jobs)
        pages = []
        for i in range(0, len(self.jobs)):
            exist = False
            for existing_job in existing_jobs:
                if existing_job['url'] == self.jobs[i]['url'] and existing_job['search_term'] == self.jobs[i]['search_term']:
                    exist = True
                    break
            if not exist:
                item = {
                    '_op_type': 'index',
                    '_index': index,
                    #'_type': 'job',
                    '_source': self.jobs[i]
                }
                pages.append(item)
        self.logger.info('inserting: {total}'.format(total=len(pages)))
        res = bulk(self.es, pages, request_timeout=REQUEST_TIMEOUT)
        if res and len(res) > 0 and type(res[0]) is int:
            self.inserted += res[0]
        self.logger.info(str(res))
        res = self.es.indices.refresh(index=index)
        self.logger.info(str(res))

    #for ec-europa-api
    def insert_data_bulk2(self, index):
        self.logger.info('insert_data_bulk2')
        self.logger.info('batch size: {total}'.format(total=len(self.jobs)))
        should_items = list(map(lambda job: {
            "bool": {
                "must": [
                    {
                        "term": {
                            "url.keyword": job['url']
                        }
                    }
                ]
            }
        }, self.jobs))
        search_body = {
            "query": {
                "bool": {
                    "should": should_items
                }
            }
        }
        res = self.es.indices.refresh(index=index)
        self.logger.info(str(res))
        res = self.es.search(index=index, body=search_body, track_scores=True, request_timeout=REQUEST_TIMEOUT, size=1000)
        existing_jobs = list(map(lambda i: i['_source'], res['hits']['hits']))
        self.logger.info('existing jobs: {total}'.format(total=len(existing_jobs)))
        self.found += len(existing_jobs)
        pages = []
        for i in range(0, len(self.jobs)):
            exist = False
            for existing_job in existing_jobs:
                if existing_job['url'] == self.jobs[i]['url']:
                    exist = True
                    break
            if not exist:
                item = {
                    '_op_type': 'index',
                    '_index': index,
                    #'_type': 'job', #comment for ES 7.x
                    '_source': self.jobs[i]
                }
                pages.append(item)
        self.logger.info('inserting: {total}'.format(total=len(pages)))
        res = bulk(self.es, pages, request_timeout=REQUEST_TIMEOUT)
        if res and len(res) > 0 and type(res[0]) is int:
            self.inserted += res[0]
        self.logger.info(str(res))
        res = self.es.indices.refresh(index=index)
        self.logger.info(str(res))

    def insert_page_results_html_bulk(self):
        self.logger.info('page_results batch size: {total}'.format(total=len(self.page_results)))
        pages = []
        for i in range(0, len(self.page_results)):
            item = {
                '_op_type': 'index',
                '_index': PAGE_RESULTS_HTML_INDEX,
                #'_type': 'job', #comment for ES 7.x
                '_source': self.page_results[i]
            }
            pages.append(item)
        res = bulk(self.es, pages, request_timeout=REQUEST_TIMEOUT)
        self.logger.info(str(res))
        res = self.es.indices.refresh(index=PAGE_RESULTS_HTML_INDEX)
        self.logger.info(str(res))

    def insert_job_details_html_bulk(self):
        self.logger.info('jobs_details batch size: {total}'.format(total=len(self.job_details)))
        should_items = list(map(lambda job: {
            "bool": {
                "must": [
                    {
                        "term": {
                            "url.keyword": job['url']
                        }
                    }
                ]
            }
        }, self.job_details))
        search_body = {
            "query": {
                "bool": {
                    "should": should_items
                }
            }
        }
        res = self.es.indices.refresh(index=JOB_DETAILS_HTML_INDEX)
        self.logger.info(str(res))
        res = self.es.search(index=JOB_DETAILS_HTML_INDEX, body=search_body, track_scores=True, request_timeout=REQUEST_TIMEOUT, size=1000)
        existing_jobs = list(map(lambda i: i['_source'], res['hits']['hits']))
        self.logger.info('existing jobs details: {total}'.format(total=len(existing_jobs)))
        pages = []
        for i in range(0, len(self.job_details)):
            exist = False
            for existing_job in existing_jobs:
                if existing_job['url'] == self.job_details[i]['url']:
                    exist = True
                    break
            if not exist:
                item = {
                    '_op_type': 'index',
                    '_index': JOB_DETAILS_HTML_INDEX,
                    #'_type': 'job', #comment for ES 7.x
                    '_source': self.job_details[i]
                }
                pages.append(item)
        self.logger.info('inserting jobs details: {total}'.format(total=len(pages)))
        res = bulk(self.es, pages, request_timeout=REQUEST_TIMEOUT)
        self.logger.info(str(res))
        res = self.es.indices.refresh(index=JOB_DETAILS_HTML_INDEX)
        self.logger.info(str(res))

    def flush_buffers(self):
        if len(self.jobs) > 0:
            self.insert_data_bulk(self.index)
            self.jobs = []
        if len(self.page_results) > 0:
            self.insert_page_results_html_bulk()
            self.page_results = []
        if len(self.job_details) > 0:
            self.insert_job_details_html_bulk()
            self.job_details = []


class ElasticSearchMantainer(object):

    es = None
    index = None
    spider = None
    updated = 0
    found = 0
    jobs = []
    JOBS_SIZE = 1000

    def __init__(self, es, index, spider, **kwargs):
        self.es = es
        self.index = index
        self.spider = spider
        self.logger = logging.getLogger('ElasticSearchMantainer')
        fh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def start_exporting(self):
        self.logger.info('Start checking expired jobs on index "{index}"'.format(index=self.index))

    def finish_exporting(self):
        self.flush_buffers()
        self.logger.info('Updating done: updated {updated} items; found {found}'.format(updated=self.updated, found=self.found))
        self.logger.info('Requests stat: {request_stat}'.format(request_stat=str(self.spider.request_stat)))
        self.logger.info('Responses stat: {response_stat}'.format(response_stat=str(self.spider.response_stat)))

    def export_item(self, item):
        data = {}
        for key, val in item.items():
            if type(val) is list:
                data[key] = ' '.join(val)
            else:
                data[key] = val
        res = self.update_data(self.index, data)
        return item

    def update_data(self, index, data):
        if len(self.jobs) < self.JOBS_SIZE - 1:
            self.jobs.append(data)
            return True
        else:
            try:
                self.jobs.append(data)
                self.update_data_bulk(index)
                self.jobs = []
                return True
            except Exception as ex:
                self.logger.error(ex)
                return False
            finally:
                self.jobs = []

    def update_data_bulk(self, index):
        self.logger.info('mantainer: batch size: {total}'.format(total=len(self.jobs)))
        pages = []
        for i in range(0, len(self.jobs)):
            item = {
                '_op_type': 'update',
                '_index': index,
                #'_type': 'job', #comment for ES 7.x
                '_id': self.jobs[i]['_id'],
                'doc': {'url_status': self.jobs[i]['url_status']}
            }
            pages.append(item)
        self.logger.info('updating: {total}'.format(total=len(pages)))
        res = bulk(self.es, pages, request_timeout=REQUEST_TIMEOUT)
        if res and len(res) > 0 and type(res[0]) is int:
            self.updated += res[0]
        self.logger.info(str(res))
        res = self.es.indices.refresh(index=index)
        self.logger.info(str(res))

    def flush_buffers(self):
        if len(self.jobs) > 0:
            self.update_data_bulk(self.index)
            self.jobs = []

class ElasticSearchDummy(object):

    es = None
    index = None
    spider = None
    updated = 0
    found = 0
    jobs = []
    JOBS_SIZE = 1000

    def __init__(self, es, index, spider, **kwargs):
        self.es = es
        self.index = index
        self.index = 'scraping-sandbox2'
        self.spider = spider
        self.logger = logging.getLogger('ElasticSearchDummy')
        fh = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def start_exporting(self):
        self.logger.info('Start dummy exporter "{index}"'.format(index=self.index))

    def finish_exporting(self):
        pass

    def export_item(self, item):
        return item


