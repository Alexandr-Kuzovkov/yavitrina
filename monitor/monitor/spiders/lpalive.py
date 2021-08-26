
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


class LpAliveSpider(scrapy.Spider):
    name = 'lpalive'
    status = None
    subject = 'Simple langing page monitor'
    errors = []
    company_slug = 'demo-company'
    port = None
    url = None
    MAX_RESPONSE_TIME = 1.0
    HTML_SNIPPETS = [
        '<html lang="en" prefix="og: http://ogp.me/ns#"><head>',
        '<link rel="stylesheet" href="themes/default/css/icons.css',
        '<script async="" src="https://www.googletagmanager.com/gtm.js?id=GTM-WP8WHPG'
    ]

    HTML_SNIPPETS_RBC = [
        '<title>РБК — новости, акции, курсы валют, доллар, евро</title>',
        '<meta name="title" content="РБК — новости, акции, курсы валют, доллар, евро" />'
    ]

    def __init__(self, port=None, env='prod', *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if env not in ['dev', 'prod']:
            raise Exception('argument "env" must be "dev" or "prod"')
            exit(1)
        self.env = env
        #self.store = PgSQLStoreMonitor(env)
        self.port = port

    def start_requests(self):
        if self.env == 'prod':
            url = 'https://%s.jobs.xtramile.io' % self.company_slug
        elif self.env == 'dev':
            url = 'https://%s.jobs.xtramile.tech' % self.company_slug
        else:
            url = 'https://%s.jobs.xtramile.io' % self.company_slug
        data = {}
        self.url = url
        self.logger.info('runtime: %s' % time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time()))))
        data['url'] = url
        data['time_start'] = time.time()
        result = requests.get(url, verify=certifi.where())
        data['time_end'] =time.time()
        data['code'] = result.status_code
        data['text'] = result.text
        local_url = 'http://127.0.0.1'
        if self.port is not None:
            local_url = ':'.join([local_url, self.port])
        request = scrapy.Request(local_url, callback=self.check)
        request.meta['data'] = data
        yield request
        #https://www.rbc.ru/
        data = {}
        url = 'https://www.rbc.ru'
        data['url'] = url
        data['time_start'] = time.time()
        result = requests.get(url, verify=certifi.where())
        data['time_end'] = time.time()
        data['code'] = result.status_code
        data['text'] = result.text
        local_url = 'http://127.0.0.1/?v=rbc'
        if self.port is not None:
            local_url = 'http://127.0.0.1:{port}/?v=rbc'.format(port=self.port)
        request = scrapy.Request(local_url, callback=self.check_rbc)
        request.meta['data'] = data
        yield request
        # https://demo-company.jobs.xtramile.io/themes/default/dist/css/main.min.css
        data = {}
        url = 'https://demo-company.jobs.xtramile.io/themes/default/dist/css/main.min.css'
        data['url'] = url
        data['time_start'] = time.time()
        result = requests.get(url, verify=certifi.where())
        data['time_end'] = time.time()
        data['code'] = result.status_code
        data['text'] = result.text
        local_url = 'http://127.0.0.1/?v=static'
        if self.port is not None:
            local_url = 'http://127.0.0.1:{port}/?v=static'.format(port=self.port)
        request = scrapy.Request(local_url, callback=self.check_static)
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
        if code != 200:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response code is not 200!')
        if response_time > self.MAX_RESPONSE_TIME:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response time is %f s, more than %f s!' % (response_time, self.MAX_RESPONSE_TIME))
        if not self.html_is_ok(html):
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Html code is not valid!')

    def html_is_ok(self, html):
        for item in self.HTML_SNIPPETS:
            if item not in html:
                self.logger.warning('Item: %s is not in html!' % item)
                return False
        return True

    def html_rbc_is_ok(self, html):
        for item in self.HTML_SNIPPETS_RBC:
            if item not in html:
                self.logger.warning('Item: %s is not in html!' % item)
                return False
        return True

    def static_is_ok(self, html):
        if html.startswith('@font-face{src:url(../../fonts/Raleway-Regular.ttf)'):
            return True
        return False


    def check_rbc(self, response):
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
        if code != 200:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response code is not 200!')
        if response_time > self.MAX_RESPONSE_TIME:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response time is %f s, more than %f s!' % (response_time, self.MAX_RESPONSE_TIME))
        if not self.html_rbc_is_ok(html):
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('RBC: Html code is not valid!')

    def check_static(self, response):
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
        if code != 200:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response code is not 200!')
        if response_time > self.MAX_RESPONSE_TIME:
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Response time is %f s, more than %f s!' % (response_time, self.MAX_RESPONSE_TIME))
        if not self.static_is_ok(html):
            self.errors.append('%s: a problem occurred during the response' % url)
            self.errors.append('Static: Html code is not valid!')







