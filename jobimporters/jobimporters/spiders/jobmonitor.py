# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreImport
from jobimporters.extensions import Geocode
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
import hashlib
from transliterate import translit
import re
import scrapy
from pprint import pprint


class JobmonitorSpider(XMLFeedSpider):

    name = 'jobmonitor'
    slugs = {}
    message_lines = []
    drain = False

    def __init__(self, employer_id=104, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        self.store = PgSQLStoreImport()
        self.geocode = Geocode()
        self.hash = hashlib.md5()
        self.iterator = 'iternodes'  # you can change this; see the docs
        try:
            self.employer = self.store.getEmployerById(self.employer_id)
        except Exception, ex:
            raise Exception('Can\'t get employer data for "%s"!' % employer_id)
            exit()
        #If employer is non-xtramile will write jobs to old-jobs database
        if 'spider' in self.employer['metadata']:
            self.store = PgSQLStoreImport()
        else:
            raise Exception('Job import for employer id="%s" don\'t need' % employer_id)
            exit()
        self.start_urls = self.employer['feed_url'].split(',')
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
        self.regularExp = re.compile('http:\/\/www.muenchenmarketingjobs.com\/job\/.*\.html')
        if 'drain' in kwargs:
            if kwargs['drain'].lower() == 'true':
                self.drain = True

    def parse_node(self, response, node):
        Item = JobItem()
        drop_list = ['id', 'employer_id', 'root', 'job', 'created_at', 'updated_at']
        if self.employer_feed_settings['company'] is None:
            drop_list.append('company')
        #pprint(self.employer_feed_settings)
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

        url = Item['url']
        Item['external_id'] = url[url.find('/job/')+5:url.find('.html')]
        Item['uid'] = str(uuid.uuid1())
        try:
            Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(self.employer['id'])])
        except Exception, ex:
            Item['external_unique_id'] = '_'.join([unicode(Item['external_id']), unicode(self.employer['id'])])
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
        if Item['city'] is None:
            Item['city'] = Item['title'].split(':').pop().strip()

        return Item

    def formatDate(self, datestr):
        day, month, year = datestr.replace('[', '').split('-')
        return '-'.join([year, month, day])

    def slugBusy(self, item, slug):
        if slug in self.slugs and self.slugs[slug] != item['external_unique_id']:
            return True
        return False

    def transliterate(self, str):
        try:
            str = translit(str.strip().lower(), reversed=True)
        except Exception, ex:
            str = str.strip().lower()
        return str


