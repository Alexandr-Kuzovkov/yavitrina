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
from fibois.items import JobItem
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.keywords import last_posted
from fibois.scrapestack import ScrapestackRequest
from fibois.notifier import Notifier
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class TestSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = []
    dirname = 'test'


    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True

    def start_requests(self):
        self.logger.info('TEST...')
        request = scrapy.Request('http://localhost:6800', callback=self.test_notifier, dont_filter=True)
        yield request

    def test_notifier(self, response):
        self.logger.info('Test notifier...')
        params = {}
        options = {
            'SMTP_PASSWORD': 'user1user1',
            'SMTP_SERVER': params.get('SMTP_SERVER', 'smtp.yandex.ru'),
            'NOTIFY_EMAILS': params.get('NOTIFY_EMAILS', ['test-monitor@kuzovkov12.ru', 'test2-monitor@kuzovkov12.ru']),
            'EMAIL_FROM': params.get('EMAIL_FROM', 'user1@kuzovkov12.ru'),
            'SMTP_PORT': params.get('SMTP_PORT', 465),
            'SLACK_CHANNEL': params.get('SLACK_CHANNEL', '#incident'),
            'SLACK_WEB_HOOK_POINT': '',
        }
        pprint(options)
        subject = 'Test Notifier'
        message = 'Notifier test email message'
        notifier = Notifier(options, self.logger)
        notifier.email(subject, message, 'tpl/email.html')
        self.logger.info('done...')

    def send_mail(self, response):
        fromaddr = 'user1@kuzovkov12.ru'
        toaddrs = 'test-monitor@kuzovkov12.ru'
        message = MIMEMultipart("alternative")
        message["Subject"] = 'subject'
        message["From"] = fromaddr

        message['To'] = toaddrs

        msg = 'test email message'
        part1 = MIMEText(msg, "plain")
        message.attach(part1)
        html = '''
        <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <title>Title</title>
            </head>
            <body>
             <p>Hi,<br>
                   How are you?<br>
                   <a href="http://www.realpython.com">Real Python</a>
                   has many great tutorials.
                </p>
             <p>Notifier test email message</p>
            </body>
            </html>
        '''
        part2 = MIMEText(html, "html")
        message.attach(part2)


        print "Message length is " + str(len(msg))
        server = smtplib.SMTP_SSL('smtp.yandex.ru', 465)
        server.login(fromaddr, 'user1user1')
        #server.set_debuglevel(1)
        server.sendmail(fromaddr, toaddrs, message.as_string())
        server.quit()











