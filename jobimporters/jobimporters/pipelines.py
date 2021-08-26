# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy import signals
import logging
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import urllib2
import urllib
import json
import math
import requests

class JobimportersPipeline(object):
    def process_item(self, item, spider):
        return item


class Notifier:
    spider = None
    logger = None
    emails = None
    message_lines = []

    def __init__(self, spider, logger):
        self.spider = spider
        self.logger = logger

    def slack(self, message):
        slack_channel = self.spider.settings.get('SLACK_CHANNEL', '#incident')
        slack_web_hook_point = self.spider.settings.get('SLACK_WEB_HOOK_POINT', '')
        if self.spider.env == 'dev':
            slack_channel = '-'.join([slack_channel, 'dev'])
        data = urllib.urlencode({'payload': json.dumps(
            {'channel': slack_channel, 'text': message, 'username': 'Xtramile-chat-bot', 'icon_emoji': ':ghost:'})})
        req = urllib2.Request(url=slack_web_hook_point)
        req.add_data(data)
        try:
            response = urllib2.urlopen(req)
        except Exception, ex:
            self.logger.error('Send slack notify fail!')
        else:
            self.logger.info('Send slack notify successfully')

    def email(self, fromaddr, toaddr, subject, message):
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        if type(toaddr) is list:
            msg['To'] = ','.join(toaddr)
        else:
            msg['To'] = toaddr
        msg['Subject'] = subject
        body = message
        try:
            msg.attach(MIMEText(body, 'plain'))
        except Exception, ex:
            body = body.encode('utf-8')
            msg.attach(MIMEText(body, 'plain'))
        try:
            server = smtplib.SMTP(self.spider.settings.get('SMTP_SERVER', 'smtp.gmail.com'), self.spider.settings.get('SMTP_PORT', 587))
            server.starttls()
            server.login(fromaddr, self.spider.settings.get('SMTP_PASSWORD', ''))
            text = msg.as_string()
            server.sendmail(fromaddr, toaddr, text)
            server.quit()
        except Exception, ex:
            self.logger.error('ERROR: sending email fail: %s' % ex)
            return False
        else:
            self.logger.info('Sending email sucessfully')
            return True



class DbItemExporter:
    store = None
    temporaryTable = 'temp'
    spider = None
    item_per_once = 1000
    current_item = 0

    def __init__(self, spider):
        self.store = spider.store
        self.spider = spider
        if hasattr(spider, 'split') and spider.split:
            self.temporaryTable = '_'.join([spider.name, str(spider.employer_id), str(spider.worker_number), self.temporaryTable])
        else:
            self.temporaryTable = '_'.join([spider.name, str(spider.employer_id), self.temporaryTable])

    def start_exporting(self):
        self.store.dbopen()
        self.store.createTemporaryTable(self.temporaryTable)

    def export_item(self, item):
        #f = open('item.txt', 'w')
        #f.write(str(item))
        #f.close()
        self.store.employer_id = self.spider.employer_id
        try:
            self.store.insertItemToTemporaryTable(item, self.temporaryTable)
        except Exception, ex:
            self.store.logger.warning('This item cause of error: %s' % (str(item)))
        self.current_item += 1
        if self.current_item >= self.item_per_once:
            try:
                self.store.mergeTables(self.temporaryTable)
                self.store.cleanTemporaryTable(self.temporaryTable)
                self.store.conn.commit()
            except Exception, ex:
                self.store.cleanTemporaryTable(self.temporaryTable)
                self.store.conn.rollback()
            self.current_item = 0
        #find expired jobs
        if item['external_unique_id'] in self.store.expired_jobs_cache:
            self.store.expired_jobs_cache[item['external_unique_id']] = -self.store.expired_jobs_cache[item['external_unique_id']]

    def finish_exporting(self):
        self.store.mergeTables(self.temporaryTable)
        self.store.dropTemporaryTable(self.temporaryTable)
        self.store.set_expired(self.temporaryTable)
        self.store.recalculateJobGroupBudgets(100, self.store.employer_id)
        #raise Exception('My error!')


class JobExportPipeline(object):

    notifier = None

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        self.logger = logging.getLogger('JobExportPipeline')
        try:
            fh = logging.FileHandler('logs/warning/warnings.log', 'a', 'utf8')
        except Exception, ex:
            fh = logging.FileHandler('warnings.log', 'a', 'utf8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        return pipeline

    def spider_opened(self, spider):
        self.exporter = DbItemExporter(spider)
        self.exporter.start_exporting()
        self.notifier = Notifier(spider, self.logger)

    def spider_closed(self, spider):
        if hasattr(spider, 'geocode'):
            spider.geocode.saveCache()
        if hasattr(spider, 'categories'):
            self.logger.info(str(spider.categories))
        if hasattr(spider, 'category_codes'):
            self.logger.info(str(spider.category_codes))
            #self.logger.info(str(spider.mapCategories))
        self.exporter.finish_exporting()
        stats = self.stats.get_stats()
        if 'log_count/ERROR' not in stats:
            spider.store.conn.commit()
            spider.store.dbclose()
        else:
            spider.store.conn.rollback()
            spider.store.dbclose()
            fromaddr = spider.settings.get('EMAIL_FROM', 'contact@myxtramile.com')
            toaddr = spider.settings.get('NOTIFY_EMAILS', [])
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            message = 'We have errors when jobimport, spider: %s' % spider.name
            self.notifier.email(fromaddr, toaddr, 'JobLeads jobimport notify', message)
        spider.store.updateJobGroupsFields()
        if len(self.notifier.message_lines) > 0:
            fromaddr = spider.settings.get('EMAIL_FROM', 'contact@myxtramile.com')
            toaddr = spider.settings.get('NOTIFY_EMAILS', [])
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            if hasattr(spider, 'unknownCategory') and spider.unknownCategory is True:
                msg = '%s: "%s" Unknown Category was found!' % (date, spider.name)
            else:
                msg = '%s: "%s" Unknown Job Group was found!' % (date, spider.name)
            self.notifier.message_lines.append(msg)
            self.notifier.message_lines = self.notifier.message_lines + spider.message_lines
            message = "\n".join(self.notifier.message_lines)
            self.notifier.email(fromaddr, toaddr, 'JobLeads jobimport notify', message)
        #clear job-landing page
        company_slug = spider.store.getCompanySlug(spider.employer_id)
        url = 'https://{company_slug}.jobs.xtramile.io?clear=company'.format(company_slug=company_slug)
        requests.get(url)

    def process_item(self, item, spider):
        if hasattr(spider, 'drain') and spider.drain is True:
            return item
        if 'country' not in item or item['country'] is not None:
            if item['job_group_id'] is not None:
                self.exporter.export_item(item)
            else:
                msg = 'Job external_unique_id="%s" category="%i" and country="%s" not imported because job Group not found' % (item['external_unique_id'], item['attributes']['category'], item['country'])
                self.notifier.message_lines.append(msg)
        else:
            self.logger.warning('The item %s with url: %s was not imported because "country" is NULL' % (item['title'], item['url']))
        return item
