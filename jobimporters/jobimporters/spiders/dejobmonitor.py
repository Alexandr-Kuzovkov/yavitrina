# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreImport
from jobimporters.extensions import Geocode
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
import hashlib
from transliterate import translit
import requests
import re
import math

import scrapy
from pprint import pprint


class JobmonitorSpider(XMLFeedSpider):

    name = 'dejobmonitor'
    slugs = {}
    message_lines = []
    drain = False
    regularExp = re.compile('<jobs total=\"[0-9]+\"')

    def __init__(self, employer_id=119, limit=1000, last_page=None, first_page=1, worker=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        self.limit = int(limit)
        self.store = PgSQLStoreImport()
        self.geocode = Geocode()
        self.hash = hashlib.md5()
        self.split = False
        if 'drain' in kwargs:
            if kwargs['drain'].lower() == 'true':
                self.drain = True
        if worker is not None: #worker means numberOfSpider_numberOfAllSpiders, example 1_5
            try:
                self.worker_number = int(worker.split('_')[0])
                self.number_of_workers = int(worker.split('_')[1])
                if self.worker_number > self.number_of_workers:
                    raise Exception('worker_number can\'t be more than number_of_workers!')
                    exit(1)
                self.split = True
            except Exception, ex:
                self.split = False
        self.iterator = 'iternodes'  # you can change this; see the docs
        try:
            self.employer = self.store.getEmployerById(employer_id)
        except Exception, ex:
            raise Exception('Can\'t get employer data for "%s"!' % employer_id)
            exit()
        #If employer is non-xtramile will write jobs to old-jobs database
        if 'spider' in self.employer['metadata']:
            self.store = PgSQLStoreImport()
        else:
            raise Exception('Job import for employer id="%s" don\'t need' % employer_id)
            exit()
        count_jobs = self.get_count_jobs(self.employer['feed_url'])
        self.logger.info('Count jobs: {count}'.format(count=count_jobs))
        if last_page is None:
            last_page = int(math.ceil(count_jobs / self.limit) + 1)
        else:
            last_page = min(int(last_page), int(math.ceil(count_jobs / self.limit) + 1))
        if self.split:
            chunks = range(1, last_page, int(math.ceil(last_page/self.number_of_workers)))
            first_page = chunks[self.worker_number-1]
            if self.worker_number == self.number_of_workers:
                self.last_worker = True
            else:
                last_page = chunks[self.worker_number] - 1
        self.start_urls = map(lambda i: '%s&limit=%i&page=%i' % (self.employer['feed_url'], self.limit, i), range(int(first_page), int(last_page) + 1))
        #self.start_urls = ['&'.join([self.employer['feed_url'], 'limit=1', 'page=1'])]
        self.employer_feed_settings = self.store.getEmployerFeedSetting(self.employer['id'])
        #self.logger.warning(str(self.employer_feed_settings))
        if self.employer_feed_settings is None:
            raise Exception('"employer_feed_settings" not exists for employer with id %i' % self.employer['id'])
            exit()
        self.itertag = self.employer_feed_settings['job'].strip()
        self.slugs = self.store.getJobsSlugs(self.employer_id)
        self.jobGroups = self.store.getGroups(self.employer_id)
        self.companySlug = self.store.getCompanySlug(self.employer['id'])
        self.mapCategories = self.store.getMapCategories(self.employer['id'])

    def parse_node(self, response, node):
        Item = JobItem()
        drop_list = ['id', 'employer_id', 'root', 'job', 'created_at', 'updated_at']
        if self.employer_feed_settings['company'] is None:
            drop_list.append('company')
        #pprint(self.employer_feed_settings)
        job_id = node.xpath('@id').extract()[0]
        for key, value in self.employer_feed_settings.items():
            if key in drop_list:
                continue
            if value is not None and type(value) is str and len(value.strip()) > 0:
                selector = node.xpath(''.join([value, '//*|', value, '/text()']))
                if (selector is not None) and (len(selector) > 0):
                    Item[feed2itemMap[key]] = selector[0].extract()
                else:
                    Item[feed2itemMap[key]] = None
            else:
                Item[feed2itemMap[key]] = None

        try:
            self.hash.update(Item['url'])
        except Exception, ex:
            self.hash.update(self.transliterate(Item['url']))
        Item['external_id'] = job_id
        Item['uid'] = str(uuid.uuid1())
        #Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(self.employer['id'])])
        Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(104)]) #hardcode employer_id to 104
        Item['company_slug'] = self.companySlug
        slug = self.store.getJobSlug(Item['title'])
        count = 1
        currslug = slug
        while self.slugBusy(Item, currslug):
            currslug = '-'.join([slug, str(count)])
            count = count + 1
        Item['slug'] = currslug
        self.slugs[Item['slug']] = Item['external_unique_id']
        Item['employer_id'] = self.employer['id']
        attributes = {"cv": False, "lang": "en", "phone": False, "ext_url": "", "jobolizer": ""}
        Item['country'] = 'Germany'
        attributes['selectedCountry'] = {"countryCode": 'DE', "countryName": Item['country']}

        #salary
        value = 'salary'
        selector = node.xpath(''.join([value, '//*|', value, '/text()']))
        if (selector is not None) and (len(selector) > 0):
            attributes['salary'] = selector[0].extract()
        category = 9
        attributes['category'] = category
        Item['attributes'] = attributes
        Item['job_group_id'] = 143
        Item['posted_at'] = self.formatDate(Item['posted_at'])
        Item['expire_date'] = self.formatDate(Item['expire_date'])
        Item['status'] = self.employer['default_job_status']
        if 'company' not in Item or Item['company'] is None:
            Item['company'] = self.employer['name']
        Item['budget'] = self.employer['default_job_budget']
        Item['budget_spent'] = 0.0
        return Item

    def formatDate(self, datestr):
        try:
            day, month, year = datestr.replace('[', '').split('-')
        except Exception, ex:
            day, month, year = datestr.replace('[', '').split('.')
        return '-'.join([year, month, day])

    def slugBusy(self, item, slug):
        if slug in self.slugs and self.slugs[slug] != item['external_unique_id']:
            return True
        return False

    def transliterate(self, str):
        #pprint(str)
        try:
            str = translit(str.strip().lower(), reversed=True)
        except Exception, ex:
            str = str.strip().lower()
        return str

    def get_count_jobs(self, url):
        url = '&'.join([url, 'limit=1', 'page=1'])
        r = requests.get(url, verify=False)
        if r.status_code == 200:
            try:
                count = self.regularExp.search(r.text).group()[13:-1]
            except Exception, ex:
                self.logger.error('Can\'t get count jobs!')
                count = 0
            return int(count)



