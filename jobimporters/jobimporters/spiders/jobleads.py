# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreImport
from jobimporters.extensions import Geocode
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
from jobimporters.items import jobLeadsJobGroups
from jobimporters.items import jobLeadsMapCountries
from jobimporters.items import jobLeadsMapCountriesRev
from jobimporters.items import jobLeadsCategoryMap

import re

import scrapy
from pprint import pprint


class JobleadsSpider(XMLFeedSpider):

    name = 'jobleads'
    slugs = {}
    unknownCategory = False
    unknownGroup = False
    jobGroups = None
    mapCategories = None
    message_lines = []
    categories = {}
    drain = False
    regularExp = re.compile('jobId=.*?&')

    def __init__(self, employer_id=101, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        self.store = PgSQLStoreImport()
        self.geocode = Geocode()
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

        Item['uid'] = str(uuid.uuid1())
        Item['external_id'] = self.url2id(Item['url'])
        Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(self.employer['id'])])
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
        countryCode = None
        if 'country' in Item and Item['country'] is not None:
            if Item['country'] in jobLeadsMapCountries:
                Item['country'] = jobLeadsMapCountries[Item['country']]
            countryCode = self.geocode.country2iso(Item['country'])
            if countryCode is None:
                if Item['country'] in jobLeadsMapCountriesRev:
                    countryCode = self.geocode.country2iso(jobLeadsMapCountriesRev[Item['country']])
                else:
                    countryCode = None
            if countryCode is not None:
                countryName = self.geocode.isocode2countryinfo(countryCode)['Country']
            else:
                countryName = Item['country']
            attributes['selectedCountry'] = {"countryCode": countryCode, "countryName": countryName}
            Item['country'] = countryName
        #salary
        value = 'prizes_salary'
        selector = node.xpath(''.join([value, '//*|', value, '/text()']))
        if (selector is not None) and (len(selector) > 0):
            attributes['salary'] = selector[0].extract()
        category = self.getCategory(Item['category'], Item['country'])
        if category is not None:
            attributes['category'] = category
        else:
            attributes['category'] = 30
            self.unknownCategory = True
            self.message_lines.append('For job with URL="%s" and category="%s" and country="%s" category was not mapping' % (Item['url'], Item['category'], Item['country']))
        Item['attributes'] = attributes
        #Item['job_group_id'] = self.getJobGroup(category, Item['country'])
        Item['job_group_id'] = self.store.getJobGroupId(self.employer_id, attributes['category'], countryCode)
        Item['posted_at'] = self.formatDate(Item['posted_at'])
        Item['status'] = self.employer['default_job_status']
        if 'company' not in Item or Item['company'] is None:
            Item['company'] = self.employer['name']
        Item['budget'] = self.employer['default_job_budget']
        Item['budget_spent'] = 0.0
        return Item


    def formatDate(self, datestr):
        day, month, year = datestr.split('.')
        return '-'.join([year, month, day])

    def slugBusy(self, item, slug):
        if slug in self.slugs and self.slugs[slug] != item['external_unique_id']:
            return True
        return False

    def getJobGroup(self, category, country):
        if country in jobLeadsJobGroups:
            if category in jobLeadsJobGroups[country]:
                return jobLeadsJobGroups[country][category]
            else:
                self.message_lines.append('category %s was not found in jobLeadsJobGroups[%s]' % (category, country))
        else:
            self.message_lines.append('country %s was not found in jobLeadsJobGroups' % country)
        return None


    def getCategory(self, category, country):
        for row in self.mapCategories:
            if row['employer_category'] == category:
                if country not in self.categories:
                    self.categories[country] = {}
                if row['category_id'] not in self.categories[country]:
                    self.categories[country][row['category_id']] = 1
                else:
                    self.categories[country][row['category_id']] += 1
                return row['category_id']
        if category in jobLeadsCategoryMap:
            if country not in self.categories:
                self.categories[country] = {}
            if jobLeadsCategoryMap[category] not in self.categories[country]:
                    self.categories[country][jobLeadsCategoryMap[category]] = 1
            else:
                self.categories[country][jobLeadsCategoryMap[category]] += 1
            return jobLeadsCategoryMap[category]

        if 'unknown' not in self.categories:
            self.categories['unknown'] = [category]
        else:
            self.categories['unknown'].append(category)
        return None

    def url2id(self, url):
        try:
            id = self.regularExp.search(url).group()[6:-1]
        except Exception, ex:
            self.logger.error('external_id can\'t get from url!')
            id = None
        return id

