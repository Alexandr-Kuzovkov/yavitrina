# -*- coding: utf-8 -*-
from jobimporters.extensions import PgSQLStoreImport
import uuid
import scrapy
from pprint import pprint
import time
from pprint import pprint
import json
import math
from jobimporters.items import JobItem
from jobimporters.items import TataCategoryMap
from jobimporters.extensions import Geocode


class TataSpider(scrapy.Spider):

    name = 'tata'
    handle_httpstatus_list = [404, 400, 500, 413]
    drain = False
    categories = {}
    message_lines = []
    slugs = {}

    def __init__(self, employer_id=None, job_group=330, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = int(employer_id)
        self.store = PgSQLStoreImport()
        self.job_group = int(job_group)
        try:
            self.employer = self.store.getEmployerById(employer_id)
        except Exception, ex:
            raise Exception('Can\'t get employer data for "%s"!' % employer_id)
            exit()
        self.employer_feed_settings = self.store.getEmployerFeedSetting(self.employer['id'])
        #self.logger.warning(str(self.employer_feed_settings))
        if self.employer_feed_settings is None:
            raise Exception('"employer_feed_settings" not exists for employer with id %i' % self.employer['id'])
            exit()
        self.slugs = self.store.getJobsSlugs(self.employer_id)
        self.companySlug = self.store.getCompanySlug(self.employer_id)
        self.geocode = Geocode()
        if 'drain' in kwargs:
            if kwargs['drain'].lower() == 'true':
                self.drain = True

    def start_requests(self):
        url = 'https://ibegin.tcs.com/iBegin/jobs/search'
        yield scrapy.Request(url=url, callback=self.get_number_pages)

    def get_number_pages(self, response):
        request = self.get_request_api1(1, self.get_jobs_lists)
        yield request

    def get_jobs_lists(self, response):
        try:
            data = json.loads(response.text)
            #pprint(data)
        except Exception, ex:
            pprint(response.text)
        else:
            number_pages = int(math.ceil(data['data']['totalJobs']/10.0))
            for page in range(1, number_pages+1):
                request = self.get_request_api1(page, self.get_jobs_data)
                yield request


    def get_request_api1(self, page, callback):
        url = 'https://ibegin.tcs.com/iBegin/api/v1/jobs/searchJ?at=%s' % str(int(time.time() * 1000))
        body = {"jobCity": None, "jobSkill": None, "pageNumber": page, "userText": "", "jobTitleOrder": None,
                "jobCityOrder": None, "jobFunctionOrder": None, "jobExperienceOrder": None, "applyByOrder": None,
                "regular": True, "walkin": True}
        headers = {
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Encoding': 'gzip,deflate,br',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Host': 'ibegin.tcs.com',
            'Origin': 'https://ibegin.tcs.com',
            'Referer': 'https://ibegin.tcs.com/iBegin/jobs/search',
            'User-Agent': 'Mozilla/5.0(X11;Linux_x86_64)_AppleWebKit/537.36(KHTML,like_Gecko)Chrome/67.0.3396.99Safari/537.36'
        }
        request = scrapy.Request(url=url, method='POST', body=json.dumps(body), headers=headers, callback=callback)
        return request

    def get_request_api2(self, job_id, callback):
        url = 'https://ibegin.tcs.com/iBegin/api/v1/job/desc?at=%s' % str(int(time.time() * 1000))
        body = {"jobId": job_id}
        headers = {
            'Accept': 'application/json,text/plain,*/*',
            'Accept-Encoding': 'gzip,deflate,br',
            'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json;charset=UTF-8',
            'Host': 'ibegin.tcs.com',
            'Origin': 'https://ibegin.tcs.com',
            'Referer': 'https://ibegin.tcs.com/iBegin/jobs/%sJ' % str(job_id),
            'User-Agent': 'Mozilla/5.0(X11;Linux_x86_64)_AppleWebKit/537.36(KHTML,like_Gecko)Chrome/67.0.3396.99Safari/537.36'
        }
        request = scrapy.Request(url=url, method='POST', body=json.dumps(body), headers=headers, callback=callback)
        return request

    def get_jobs_data(self, response):
        try:
            data = json.loads(response.text)
            #pprint(data)
        except Exception, ex:
            pprint(response.text)
        else:
            for job in data['data']['jobs']:
                job_id = int(job['id'].replace('J', ''))
                request = self.get_request_api2(job_id, self.parse_job)
                yield request

    def parse_job(self, response):
        try:
            data = json.loads(response.text)
            #pprint(data)
        except Exception, ex:
            pprint(response.text)
        else:
            job = data['data']
            Item = JobItem()
            Item['url'] = 'https://ibegin.tcs.com/iBegin/jobs/%sJ' % str(job['jobId'])
            Item['title'] = job['title']
            Item['description'] = job['description']
            Item['state'] = job['location']
            Item['keywords'] = [job['qualifications']]
            Item['external_id'] = job['jobId']
            Item['uid'] = str(uuid.uuid1())
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
            Item['country'] = job['country']
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
            Item['category'] = job['functionName']
            self.countCategory(Item['category'])
            if Item['category'] in TataCategoryMap:
                attributes['category'] = TataCategoryMap[Item['category']]
            else:
                attributes['category'] = 30
            Item['attributes'] = attributes
            job_group_id = self.store.getJobGroupId(self.employer_id, attributes['category'], countryCode)
            if job_group_id is not None:
                Item['job_group_id'] = job_group_id
            else:
                Item['job_group_id'] = self.job_group
            Item['expire_date'] = job['applyby']
            Item['status'] = self.employer['default_job_status']
            if 'company' not in Item or Item['company'] is None:
                Item['company'] = self.employer['name']
            Item['budget'] = self.employer['default_job_budget']
            Item['budget_spent'] = 0.0
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



