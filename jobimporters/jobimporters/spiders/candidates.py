# -*- coding: utf-8 -*-
from jobimporters.extensions import PgSQLStoreCandidates
from jobimporters.items import GenericItem
import scrapy
import requests
import json
import time

class CandidatesSpider(scrapy.Spider):

    name = 'candidates'
    employer_name = 'xtramile'
    campaignCache = {}
    companyCache = {}
    employerCache = {}
    categories = {}
    employer_id = None
    item_id = None
    company_name = None
    company_id = None
    store = None
    employer = None
    table = 'candidates'

    def __init__(self, table=None, company_id=None, item_id=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.store = PgSQLStoreCandidates()
        flds = {
            'candidates': {
                'pkey': 'created_at',
                'fld_list': 'name,email,phone,resume_file,linked_in_profile,user_ip,job_id,employer_id,created_at,updated_at'.split(','),
                'upd_fld_list': 'name,email,phone,resume_file,linked_in_profile,user_ip,job_id,employer_id,updated_at'.split(',')
            }
        }
        if table is not None:
            self.table = table
        self.store.table = self.table
        self.store.fld_list = []
        self.store.upd_fld_list = []
        if self.table in flds:
            self.store.fld_list = flds[self.table]['fld_list']
            self.store.upd_fld_list = flds[self.table]['upd_fld_list']
            self.store.setPkey(flds[self.table]['pkey'])
        self.start_urls = ['https://api.xtramile.io/api/v1/applicationusers']
        self.categories = {}

    def parse(self, response):
        items = json.loads(response.text)
        for item in items:
            if self.company_id is not None and self.company_id != item['companyId']:
                continue
            if self.item_id is not None and self.item_id != item['itemId']:
                continue
            if self.filterItem(item):
                continue
            if item['companyId'] in self.employerCache:
                employer = self.employerCache[item['companyId']]
            else:
                employer = self.store.getEmployerByMetadata({'spider': 'xtramile', 'employer_id': item['companyId']})
            if employer is None:
                continue
            self.employer_id = employer['id']
            Item = GenericItem()
            for fld in self.store.fld_list:
                if fld not in Item.fields:
                    Item.fields[fld] = scrapy.Field()
            Item['name'] = item['fields']['name']
            Item['email'] = item['fields']['email']
            try:
                Item['phone'] = item['fields']['phone']
            except Exception, ex:
                Item['phone'] = None
            try:
                Item['resume_file'] = int(item['fields']['resumeFile'])
            except Exception, ex:
                Item['resume_file'] = None
            try:
                Item['linked_in_profile'] = int(item['fields']['linkedInProfile'])
            except Exception, ex:
                Item['linked_in_profile'] = None
            Item['user_ip'] = item['userIp']
            Item['employer_id'] = employer['id']
            job = self.store.getJobByExternalUniqueid('_'.join([str(item['itemId']), str(employer['id'])]))
            if job is None:
                continue
            Item['job_id'] = job['id']
            Item['created_at'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(item['dateAdd']))
            Item['updated_at'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(item['dateUpd']))
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

    def filterItem(self, item):
        return False
