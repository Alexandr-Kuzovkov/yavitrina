# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import pkgutil
from monitor.extensions import PgSQLStoreMonitor
from pprint import pprint
import json

class JobLpSpider(scrapy.Spider):
    name = 'joblp'
    allowed_domains = ['xtramile.tech', 'xtramile.io']
    lua_src = pkgutil.get_data('monitor', 'lua/jobs_landing_page.lua')
    MAX_ROWS = 1000
    subject = 'Jobs langing page monitor'
    errors = {}
    status = None


    def __init__(self, employer_id=31, env='dev', status=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        if env not in ['dev', 'prod']:
            raise Exception('argument "env" must be "dev" or "prod"')
            exit(1)
        self.env = env
        try:
            if status is not None:
                status = int(status)
        except Exception, ex:
            raise Exception('argument "status" must be integer')
            exit(1)
        if status is not None and status not in [0, 1, 2, 3, 4]:
            raise Exception('argument "status" must be integer in range [0-4]')
            exit(1)
        self.status = status
        self.store = PgSQLStoreMonitor(env)

    def start_requests(self):
        countJobs = self.store.countJobs(self.employer_id, self.status)
        offsets = range(0, countJobs, self.MAX_ROWS)
        for offset in offsets:
            jobs = self.store.getJobs(self.employer_id, self.status, ['id', 'url', 'title', 'posted_at', 'category', 'job_type', 'description', 'company_slug', 'employer_id'], offset, self.MAX_ROWS)
            for job in jobs:
                if job['url'] == '':
                    continue
                if job['company_slug'][len(job['company_slug']) - 1] == '-':
                    continue
                request = SplashRequest(job['url'], self.check_landing_page, endpoint='execute', args={'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600})
                request.meta['job'] = job
                yield request

    def check_landing_page(self, response):
        self.logger.info('checking page from "%s"' % response.url)
        try:
            data = json.loads(response.text)
            job = response.meta['job']
        except Exception, ex:
            if response.url not in self.errors:
                self.errors[response.url] = []
            self.logger.error('checking page from "%s" fail' % response.url)
            self.errors[response.url].append('checking page from "%s" fail' % response.url)
            yield {'error': 'check page from "%s" fail' % response.url, 'job': job}
        else:
            try:
                self.check_content(response, data, job)
            except Exception, ex:
                if response.url not in self.errors:
                    self.errors[response.url] = []
                self.errors[response.url].append(' - %s' % ex)
            else:
                self.logger.info('checking page from "%s" successfully' % response.url)
            yield data

    def check_content(self, response, data, job):
        if response.url != job['url']:
            raise Exception('Field "%s" is not valid!' % 'url')
        if 'title' in data:
            #print ('data.title  = %; job.title' )
            self.check_match(data['title'], 'Job / ' + job['title'], 'title')
        else:
            raise Exception('Field "%s" is not exists or empty!' % 'title')
        self.check_exists(data, 'category')
        #TODO: get job_type from coreapi by id and thrn compare
        #if 'job_type' in data:
        #    self.check_match(data['job_type'], job['job_type'], 'job_type')
        #else:
        #    raise Exception('Field "%s" is not exists or empty!' % 'job_type')
        if job['description'] and len(job['description']) > 0:
            self.check_not_empty(data, 'description')
        self.check_not_empty(data, 'posted_at')
        self.check_not_empty(data, 'location')
        self.check_not_empty(data, 'profile_link')
        logo = self.store.getEmployerLogo(job)
        if logo is not None and len(logo) > 0:
            self.check_not_empty(data, 'logo_url')

    def check_match(self, value1, value2, key):
        if value1 == '' and value2 is None:
            return True
        if value1 != '' and value2 is None:
            raise Exception('Field "%s" is not valid, "%s" not equal to "%s"!' % (key, value1, value2))
        value2 = value2.strip().lower()
        value1 = value1.lower()
        if value1 != value2.decode('utf-8'):
            raise Exception('Field "%s" is not valid, "%s" not equal to "%s"!' % (key, value1, value2))
        return True

    def check_not_empty(self, data, key):
        if (key not in data) or (data[key] is None) or len(data[key]) == 0:
            raise Exception('Field "%s" is not exists or empty!' % key)
        return True

    def check_exists(self, data, key):
        if (key not in data) or (data[key] is None):
            raise Exception('Field "%s" is not exists!' % key)
        return True




