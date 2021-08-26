# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import pkgutil
from monitor.extensions import PgSQLStoreMonitor
from pprint import pprint
import json

class JobApplyFormSpider(scrapy.Spider):
    name = 'jobapplyform'
    allowed_domains = ['xtramile.tech', 'xtramile.io']
    lua_src = pkgutil.get_data('monitor', 'lua/job_apply.lua')
    subject = 'Job apply form monitor'
    errors = []
    employer_id = 31
    url = None

    def __init__(self, env='dev', *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if env not in ['dev', 'prod']:
            raise Exception('argument "env" must be "dev" or "prod"')
            exit(1)
        self.env = env
        self.store = PgSQLStoreMonitor(env)

    def start_requests(self):
        job = self.store.getDemoJob('programmer-1')
        self.url = job['url']
        request = SplashRequest(job['url'], callback=self.check_apply_form, endpoint='execute', args={'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600})
        request.meta['job'] = job
        yield request

    def check_apply_form(self, response):
        self.logger.info('checking page from "%s"' % response.url)
        try:
            data = json.loads(response.text)
        except Exception, ex:
            self.logger.error('checking apply form "%s" fail' % response.url)
            self.errors.append('checking apply form "%s" fail' % response.url)
        else:
            if 'result' in data and data['result'] == 'success':
                self.logger.info('Apply form check successfully')
            else:
                self.errors.append('checking apply form "%s" fail: %s' % (response.url, data['error']))
                self.logger.error('checking apply form "%s" fail: %s' % (response.url, data['error']))
            yield data






