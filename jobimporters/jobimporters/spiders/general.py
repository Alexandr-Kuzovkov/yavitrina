# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreImport
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
from jobimporters.items import GeneralCategoryMap
from jobimporters.extensions import Geocode
import scrapy
from pprint import pprint


class GeneralSpider(XMLFeedSpider):

    name = 'general'
    handle_httpstatus_list = [404, 400, 500, 413]
    drain = False
    categories = {}
    message_lines = []
    slugs = {}

    def __init__(self, employer_id=None, job_group=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        self.job_group = job_group
        self.store = PgSQLStoreImport()
        self.iterator = 'iternodes'  # you can change this; see the docs
        try:
            self.employer = self.store.getEmployerById(employer_id)
        except Exception, ex:
            raise Exception('Can\'t get employer data for "%s"!' % employer_id)
            exit()
        self.start_urls = self.employer['feed_url'].split(',')
        self.employer_feed_settings = self.store.getEmployerFeedSetting(self.employer['id'])
        if self.employer_feed_settings is None:
            raise Exception('"employer_feed_settings" not exists for employer with id %i' % self.employer['id'])
            exit()
        self.itertag = self.employer_feed_settings['job'].strip()
        self.slugs = self.store.getJobsSlugs(self.employer_id)
        self.companySlug = self.store.getCompanySlug(self.employer_id)
        self.geocode = Geocode()
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
                    Item[feed2itemMap[key]] = selector[0].extract().strip()
                else:
                    Item[feed2itemMap[key]] = None
            else:
                Item[feed2itemMap[key]] = None

        Item['uid'] = str(uuid.uuid1())
        Item['employer_id'] = self.employer['id']
        Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(self.employer['id'])]).strip().replace(' ', '').replace('\n', '').replace('\t', '')
        Item['status'] = self.employer['default_job_status']
        if 'company' not in Item or Item['company'] is None:
            Item['company'] = self.employer['name']
        Item['budget'] = self.employer['default_job_budget']
        Item['budget_spent'] = 0.0
        Item['company_slug'] = self.companySlug
        if Item['title'] is None:
            Item['title'] = ' '.join(Item['description'].split(' ')[0:2]).strip()
        slug = self.store.getJobSlug(Item['title'])
        count = 1
        currslug = slug
        while self.slugBusy(Item, currslug):
            currslug = '-'.join([slug, str(count)])
            count = count + 1
        Item['slug'] = currslug
        self.slugs[Item['slug']] = Item['external_unique_id']
        attributes = {"cv": False, "lang": "en", "phone": False, "ext_url": "", "jobolizer": ""}
        countryCode = None
        if 'country' in Item and Item['country'] is not None:
            if Item['country'].upper().strip() in self.geocode.iso2:
                countryCode = Item['country'].upper().strip()
            else:
                countryCode = self.geocode.country2iso(Item['country'])
            if countryCode is not None:
                countryName = self.geocode.isocode2countryinfo(countryCode)['Country']
                Item['country'] = countryName
            else:
                countryName = Item['country']
            attributes['selectedCountry'] = {"countryCode": countryCode, "countryName": countryName}
        Item['category'] = self.cleanCategory(Item['category'])
        self.countCategory(Item['category'])
        if Item['category'] in GeneralCategoryMap:
            attributes['category'] = GeneralCategoryMap[Item['category']]
        else:
            attributes['category'] = 30
        Item['attributes'] = attributes
        job_group_id = 1#self.store.getJobGroupId(self.employer_id, attributes['category'], countryCode)
        if job_group_id is not None:
            Item['job_group_id'] = job_group_id
        else:
            Item['job_group_id'] = self.job_group
        Item['url'] = 'https://%s.jobs.xtramile.io/%s' % (Item['company_slug'], Item['slug'])
        return Item

    def slugBusy(self, item, slug):
        if slug in self.slugs and self.slugs[slug] != item['external_unique_id']:
            return True
        return False

    def countCategory(self, category):
        if category in self.categories:
            self.categories[category] += 1
        else:
            self.categories[category] = 1

    def cleanCategory(self, category):
        if category is not None:
            category = ''.join(map(lambda i: i.strip(), category.split('\n')))
            if '&' in category:
                category = ' & '.join(map(lambda i: i.strip(), category.split('&')))
        return category
