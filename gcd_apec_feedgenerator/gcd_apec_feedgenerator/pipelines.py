# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

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
from pprint import pprint


class Notifier:
    spider = None
    logger = None
    emails = None
    message_lines = []

    def __init__(self, spider, logger):
        self.spider = spider
        self.logger = logger

    def slack(self, message):
        slack_channel = self.spider.settings.get('SLACK_CHANNEL', '#inicdent')
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

    def email(self, fromaddr, toaddr, subject, message, email_type='plain'):
        msg = MIMEMultipart()
        msg['From'] = fromaddr
        if type(toaddr) is list:
            msg['To'] = ','.join(toaddr)
        else:
            msg['To'] = toaddr
        msg['Subject'] = subject
        body = message
        try:
            msg.attach(MIMEText(body, email_type))
        except Exception, ex:
            body = body.encode('utf-8')
            msg.attach(MIMEText(body, email_type))
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


class GcdApecExporterPipeline(object):

    notifier = None
    job_service = None
    job_attributes = {}

    def __init__(self):
        self.files = {}
        self.feed_name = ''
        self.logger = logging.getLogger('GcdApecExporter')
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
        self.notifier = Notifier(spider, self.logger)
        if hasattr(spider, 'job_service'):
            self.job_service = spider.job_service

    def spider_closed(self, spider):
        if hasattr(spider, 'geocode'):
            spider.geocode.saveCache()
        stats = self.stats.get_stats()
        if 'log_count/ERROR' not in stats:
            pass
        else:
            pass
        if hasattr(spider, 'job_service'):
            self.job_service.execute_batch_create()
            self.job_service.execute_batch_update()
            self.job_service.execute_batch_delete()
            #pprint('created_jobs:')
            #pprint(self.job_service.created_jobs)
            #pprint('deleted_jobs:')
            #pprint(self.job_service.deleted_jobs)
            spider.store.save_gcd_job_name_batch(self.job_service.created_jobs, self.job_attributes)
            spider.store.rm_gcd_job_name_batch(self.job_service.deleted_jobs, self.job_attributes)
        if hasattr(spider, 'new_published') and len(spider.new_published) > 0:
            pprint(spider.new_published)
            self.send_notify_email(spider)

    def process_item(self, item, spider):
        self.job_attributes[item['id']] = item['attributes']
        return item

    #senf email to Elodie if new jons was published
    def send_notify_email(self, spider):
        fromaddr = spider.settings.get('EMAIL_FROM', 'contact@myxtramile.com')
        toaddr = spider.settings.get('ELODIE_EMAIL', [])
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        jobs = ' '.join(map(lambda i: '<li> %s:     <a href="%s">%s</a></li>' % (i['id'], i['url'], i['url']), spider.new_published))
        message = spider.email_template.replace('##jobs##', jobs).replace('##date##', date)
        self.notifier.email(fromaddr, toaddr, 'New jobs was posting to APEC', message, 'html')