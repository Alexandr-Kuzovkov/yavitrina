
# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import pkgutil
from monitor.extensions import PgSQLStoreMonitor
from pprint import pprint
import json
from pprint import pprint
import requests
import time
import urllib3
#urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
urllib3.disable_warnings()
import certifi


class PixelAliveSpider(scrapy.Spider):
    name = 'pixelalive'
    status = None
    subject = 'Simple pixel monitor'
    errors = []
    company_slug = 'demo-company'
    port = None
    url = None
    MAX_RESPONSE_TIME = 1.0
    LEN_HTML = 2047
    QUEUE_LIMIT = 30

    def __init__(self, port=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        #self.store = PgSQLStoreMonitor(env)
        self.port = port

    def start_requests(self):
        data = {}
        url = 'https://pixel.xtramile.io/p/0426f504-4454-11e7-9daa-2c56dc4b235a/c6df7.js'
        self.logger.info('runtime: %s' % time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time()))))
        data['url'] = url
        data['time_start'] = time.time()
        try:
            result = requests.get(url, verify=certifi.where())
        except Exception:
            self.errors.append('Request to url "{url}" failed'.format(url=url))
        else:
            data['time_end'] =time.time()
            data['code'] = result.status_code
            data['text'] = result.text
            data['len'] = len(result.text)

        url = 'http://pixel.xtramile.io/sidekiq/queues'
        request = scrapy.Request(url, callback=self.check)
        headers = request.headers
        headers['Authorization'] = 'Basic eHRyYW1pbGVfZGV2OjY3UFhpWWQ3dUo='
        request = request.replace(headers=headers)
        request.meta['data'] = data
        yield request

    def check(self, response):
        data = response.meta['data']
        code = data['code']
        response_time = data['time_end'] - data['time_start']
        html = data['text']
        #pprint(html)
        url = data['url']
        self.logger.info('URL: %s' % url)
        self.logger.info('code: %i' % code)
        self.logger.info('resp_time: %f' % response_time)
        #self.logger.info('html: %s' % html)
        self.logger.info('len(html): %i' % len(html))
        if code != 200:
            self.errors.append('Response code is not 200!')
        if response_time > self.MAX_RESPONSE_TIME:
            self.errors.append('Response time is %f s, more than %f s!' % (response_time, self.MAX_RESPONSE_TIME))
        if not self.html_is_ok(html):
            self.errors.append('Html code is not valid!')
        rows = response.css('table.queues tr')
        for row in rows:
            queue_name = row.css('td a')[0].xpath('text()').extract()[0]
            queue_value = row.css('td')[1].xpath('text()').extract()[0].strip()
            self.logger.info('queue_name="%s"; queue_value="%s"' % (queue_name, queue_value))
            if queue_value.isdigit() and int(queue_value) > self.QUEUE_LIMIT:
                self.errors.append('Queue "%s" size is "%i", it more than "%i"' % (queue_name, int(queue_value), self.QUEUE_LIMIT))

    def html_is_ok(self, html):
        if len(html) == self.LEN_HTML:
            return True
        return False








