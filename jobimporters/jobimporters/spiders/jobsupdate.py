# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreJobsUpdate
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
import scrapy
import requests
import json
import time
from scrapy.loader import ItemLoader

class JobsUpdateSpider(scrapy.Spider):

    name = 'jobsupdate'
    employer_name = 'xtramile'
    campaignCache = {}
    companyCache = {}
    categories = {}
    employer_id = None
    company_name = None
    company_id = None
    store = None
    employer = None
    job_types = {}
    statuses = {'running': 1, 'starting': 1}

    def __init__(self, company_id=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.store = PgSQLStoreJobsUpdate()
        self.start_urls = ['https://api.xtramile.io/api/v1/items']
        self.categories = {}
        if company_id is not None:
            self.company_id = int(company_id)
            self.store.company_id = self.company_id

    def parse(self, response):
        items = json.loads(response.text)
        for item in items:
            if self.company_id is not None and self.company_id != item['companyId']:
                continue
            employer = self.store.getEmployerByMetadata({})
            if employer is None:
                continue
            self.employer_id = employer['id']
            self.store.employer_id = self.employer_id
            Item = JobItem()
            Item['title'] =item['title']
            Item['description'] = item['description']
            campaign = self.getCampaign(item['campaignId'])
            try:
                Item['posted_at'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(campaign['start_date']))
            except Exception, ex:
                Item['posted_at'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(item['dateAdd']))
            if 'lang' in item:
                lang = item['lang']
            else:
                lang = 'en'
            if lang not in self.job_types:
                self.getJobTypes(lang)
            if lang not in self.categories:
                self.categories[lang] = self.getCategories(lang)
            if item['attributes']['country'] is not None and len(item['attributes']['country']) == 2:
                Item['country'] = item['attributes']['country']
                try:
                    Item['city'] = campaign['audience']['countries'][0]['cities'][0]['name']
                except Exception, ex:
                    Item['city'] = None
            else:
                Item['country'] = 'FR'
                try:
                    Item['city'] = item['attributes']['country']
                except Exception, ex:
                    Item['city'] = None

            plsubdomain = 'jobs.xtramile.io'
            Item['url'] = 'https://%s.%s/%s' % (item['companySlug'], plsubdomain, item['slug'])
            Item['category'] = self.categories[lang].get(str(item['categoryId']), None)
            if self.company_name is None:
                company = self.getCompanyName(item['companyId'])
            else:
                company = self.company_name
            if self.filterCompany(company):
                continue
            Item['company'] = company
            Item['uid'] = str(uuid.uuid1())
            Item['employer_id'] = employer['id']
            Item['job_group_id'] = None
            Item['external_id'] = str(item['id'])
            Item['external_unique_id'] = '_'.join([str(item['id']), str(employer['id'])])
            Item['slug'] = item['slug']
            Item['company_slug'] = item['companySlug']
            Item['keywords'] = item['keywords']
            Item['is_editable'] = True
            if item['status'] in self.statuses:
                Item['status'] = self.statuses[item['status']]
            else:
                Item['status'] = 0
            Item['expire_date'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(campaign['end_date']))
            Item['budget'] = employer['default_job_budget']
            Item['budget_spent'] = 0.0
            attributes = {
                "cv": item.get('cv', False),
                "lang": lang,
                "phone": item.get('phone', False),
                "ext_url": item['extUrl'],
                "jobolizer": item['jobolizer'],
                "category": item['categoryId']
            }
            if 'jobtype' in item:
                attributes['jobType'] = item['jobtype']
            if 'img' in item:
                attributes['img'] = item['img']
            if 'youtube' in item:
                attributes['youtube'] = item['youtube']
            Item['attributes'] = attributes
            job_type = self.job_types[lang].get(str(item['jobtype']), None)
            if company == 'GRDF' and job_type == 'CDI':
                job_type = 'Alternance'
            Item['job_type'] = job_type
            yield Item

    def getCategories(self, lang='en'):
        res = requests.get('https://api.xtramile.io/api/v1/items/categories/?lang=%s' % lang)
        if res.status_code == 200:
            return res.json()
        else:
            return {}

    def getCampaign(self, campaignId):
        if campaignId in self.campaignCache:
            return self.campaignCache[campaignId]
        res = requests.get('https://api.xtramile.io/api/v1/campaigns/' + str(campaignId))
        if res.status_code == 200:
            d = res.json()
            self.campaignCache[campaignId] = d
            return d
        else:
            return {}

    def getCompanyName(self, companyId):
        if companyId in self.companyCache:
            return self.companyCache[companyId]
        res = requests.get('https://api.xtramile.io/api/v1/companies/' + str(companyId))
        if res.status_code == 200:
            name = res.json()['name']
            self.companyCache[companyId] = name
            return name
        else:
            return None

    def filterCompany(self, company):
        if company is None:
            return True
        if 'test'.lower() in company.lower():
            return True
        return False

    def getJobTypes(self, lang='en'):
        res = requests.get('https://api.xtramile.io/api/v1/items/job-types?lang=%s' % lang)
        if res.status_code == 200:
            self.job_types[lang] = res.json()