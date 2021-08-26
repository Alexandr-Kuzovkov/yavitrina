
# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import pkgutil
from monitor.extensions import PgSQLStoreMonitor
from pprint import pprint
import json
from pprint import pprint

class CompanyLpSpider(scrapy.Spider):
    name = 'companylp'
    allowed_domains = ['xtramile.tech', 'xtramile.io']
    lua_src = pkgutil.get_data('monitor', 'lua/companies_landing_page.lua')
    MAX_ROWS = 1000
    status = None
    subject = 'Companies langing page monitor'
    errors = {}

    def __init__(self, employer_id=31, env='dev', status=1, *args, **kwargs):
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
        companies = self.store.getCompanies(self.employer_id)
        pprint(len(companies))
        for company in companies:
            countJobs = self.store.countJobs(employer_id=company['employer_id'], status=self.status)
            company['profile'] = self.store.getCompanyProfile(company['company_slug'])
            company['name'] = self.store.getCompanyName(company['company_slug'])
            company['description'] = self.store.getCompanyDescription(company['company_slug'])
            offsets = range(0, countJobs, self.MAX_ROWS)
            if company['company_slug'][len(company['company_slug']) - 1] == '-':
                continue
            for offset in offsets:
                jobs = self.store.getJobs(company['employer_id'], self.status, ['id', 'title', 'slug'], offset, self.MAX_ROWS)
                if company['company_slug'][len(company['company_slug'])-1] == '-':
                    continue
                if self.env == 'prod':
                    url = 'https://%s.jobs.xtramile.io' % company['company_slug']
                elif self.env == 'dev':
                    url = 'https://%s.jobs.xtramile.tech' % company['company_slug']
                request = SplashRequest(url, self.check_landing_page, endpoint='execute', args={'wait': 1.0, 'lua_source': self.lua_src, 'timeout': 3600})
                request.meta['jobs'] = jobs
                request.meta['company'] = company
                yield request

    def check_landing_page(self, response):
        self.logger.info('checking page from "%s"' % response.url)
        try:
            data = json.loads(response.text)
            jobs = response.meta['jobs']
        except Exception, ex:
            if response.url not in self.errors:
                self.errors[response.url] = []
            self.errors[response.url].append('check page from "%s" of company "%s" fail' % (response.url, response.meta['company']['company_slug']))
            self.logger.error('check page from "%s" of company "%s" fail' % (response.url, response.meta['company']['company_slug']))
            yield {'error': 'check page from "%s" of company "%s" fail' % (response.url, response.meta['company']['company_slug'])}
        else:
            try:
                self.check_content(response, data)
            except Exception, ex:
                if response.url not in self.errors:
                    self.errors[response.url] = []
                self.errors[response.url].append(' - %s' % ex)
            else:
                self.logger.info('checking page from "%s" successfully' % response.url)
            yield data

    def check_content(self, response, data):
        jobs = response.meta['jobs']
        company = response.meta['company']
        if 'name' in data:
            self.check_match(data['name'].strip(), company['name'].strip(), 'name')
        else:
            raise Exception('Field "%s" is not exists or empty!' % 'name')
        if company['description'] and len(company['description']) > 0:
            if 'description' in data:
                self.check_not_empty(data, 'description')
            else:
                raise Exception('Field "%s" is not exists or empty!' % 'description')
        if 'website' in company['profile'] and company['profile']['website'] is not None and len(company['profile']['website']) > 0:
            if 'url' in data:
                self.check_match(data['url'], company['profile']['website'], 'website')
            else:
                raise Exception('Field "%s" is not exists or empty!' % 'url')
        if 'logo' in company['profile'] and company['profile']['logo']:
            if 'logo' in data:
                self.check_not_empty(data, 'logo')
            else:
                raise Exception('Field "%s" is not exists or empty!' % 'url')
        self.check_jobs(data, jobs)
        self.check_social_links(data, company)


    def check_match(self, value1, value2, key):
        if value1 == '' and value2 is None:
            return True
        if value1 != '' and value2 is None:
            raise Exception('Field "%s" is not valid, "%s" not equal to "%s"!' % (key, value1, value2))
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

    def check_jobs(self, data, jobs):
        for job in jobs:
            if not ('/'+job['slug'] in map(lambda i: i['url'], data['jobs'].values())):
                pprint('data=')
                pprint(data)
                pprint('job=')
                pprint(job)
                raise Exception('Company has %i jobs, but on page present %i; Job id=%i, title="%s" not present on page' % (len(jobs), len(data['jobs']), job['id'], job['title']))

    def check_social_links(self, data, company):
        if 'facebook' in company['profile'] and company['profile']['facebook'] is not None and len(company['profile']['facebook']) > 0:
            if company['profile']['facebook'] not in data['social_links'].values():
                raise Exception('Link to facebook not found!')

        if 'twitter' in company['profile'] and company['profile']['twitter'] is not None and len(company['profile']['twitter']) > 0:
            if company['profile']['twitter'] not in data['social_links'].values():
                raise Exception('Link to twitter not found!')

        if 'linkedin' in company['profile'] and company['profile']['linkedin'] is not None and len(company['profile']['linkedin']) > 0:
            if company['profile']['linkedin'] not in data['social_links'].values():
                raise Exception('Link to linkedin not found!')

        if 'instagram' in company['profile'] and company['profile']['instagram'] is not None and len(company['profile']['instagram']) > 0:
            if company['profile']['instagram'] not in data['social_links'].values():
                raise Exception('Link to instagram not found!')




