# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy import signals
import logging
from pprint import pprint
import urllib
import urllib2
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


class Notifier:
    spider = None
    logger = None
    emails = None

    def __init__(self, spider, logger):
        self.spider = spider
        self.logger = logger

    def slack(self, message):
        slack_channel = self.spider.settings.get('SLACK_CHANNEL', '#incident')
        slack_web_hook_point = self.spider.settings.get('SLACK_WEB_HOOK_POINT', '')
        if hasattr(self.spider, 'env') and self.spider.env == 'dev':
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


class MonitorPipeline(object):
    slack_channel = None
    notifier = None

    def __init__(self):
        self.logger = logging.getLogger('MonitorPipeline')
        try:
            pprint('log_file=%s' % self.log_file)
            fh = logging.FileHandler(self.log_file, 'a', 'utf8')
        except Exception, ex:
            fh = logging.FileHandler('check-job-lp.log', 'a', 'utf8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.setLevel(1)

    @classmethod
    def from_crawler(self, crawler):
        pipeline = self()
        crawler.signals.connect(pipeline.spider_opened, signals.spider_opened)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        self.stats = crawler.stats
        return pipeline

    def spider_opened(self, spider):
        self.log_file = spider.settings.get('CHECK_JOBLP_LOG', '/home/ubuntu/logs/check-job-lp.log')
        self.notifier = Notifier(spider, self.logger)

    def spider_closed(self, spider):
        stats = self.stats.get_stats()
        fromaddr = spider.settings.get('EMAIL_FROM', 'contact@myxtramile.com')
        toaddr = spider.settings.get('NOTIFY_EMAILS', [])
        if len(spider.errors) > 0:
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            message_lines = []
            message_lines.append('%s - %s' % (spider.subject, date))
            if type(spider.errors) is dict:
                for url, error_list in spider.errors.items():
                    message_lines.append('%s has\'t or has incorrect items:' % url)
                    for line in error_list:
                        message_lines.append(line)
            else:
                for line in spider.errors:
                    message_lines.append(line)
            message = "\n".join(message_lines)
            self.logger.error(message)
            self.notifier.slack(message)
            self.notifier.email(fromaddr, toaddr, spider.subject, message)
        else:
            self.logger.info('%s: test been successfully' % spider.subject)

    def process_item(self, item, spider):
        return item

