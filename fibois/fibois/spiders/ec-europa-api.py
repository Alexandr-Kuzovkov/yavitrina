# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
from fibois.items import JobItem
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.scrapestack import ScrapestackRequest

class EcEuropaApiSpider(scrapy.Spider):
    name = 'ec-europa-api'
    allowed_domains = []
    dirname = 'ec-europa-api'
    lua_src = pkgutil.get_data('fibois', 'lua/ec-europa.lua')
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta_days = 1
    keywords = occupations
    #keywords = occupations[144:145]
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    delta = False
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    start_time = None
    max_runtime = 25200  # 7 hours

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True
        if keywords_limit:
            self.keywords_limit = int(keywords_limit)
        if keywords:
            if keywords == 'companies':
                self.keywords = companies
                self.keywords_type = 'companies'
        if self.keywords_limit is not None:
            self.keywords = self.keywords[0:self.keywords_limit]
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        self.start_time = int(time.time())

    def start_requests(self):
        for keyword in self.keywords:
            page = self.pagination[keyword]['page']
            body = {
                "keywords": [],
                "publicationPeriod": None,
                "occupationUris": [], "skillUris": [],
                "requiredExperienceCodes": [],
                "positionScheduleCodes": [],
                "sectorCodes": [],
                "availableLanguages": [],
                "educationLevelCodes": [],
                "positionOfferingCodes": [],
                "locationCodes": ["fr"],
                "euresFlagCodes": [],
                "otherBenefitsCodes": [],
                "requiredLanguages": [],
                "resultsPerPage": 50,
                "sortSearch": "BEST_MATCH",
                "page": page,
                "sessionId": "aqa63ou1wxptpzuxee0lg"
            }
            body['keywords'].append({"keyword": keyword, "specificSearchCode": "EVERYWHERE"})
            if self.delta:
                body['publicationPeriod'] = 'LAST_DAY'
            url = 'https://ec.europa.eu/eures/eures-apps/searchengine/page/jv-search/search'
            request = scrapy.Request(url, method='POST', body=json.dumps(body), headers={'Content-Type': 'application/json'}, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #print(response.text)
        data = json.loads(response.text)
        offers = data['jvs']
        total = data['numberRecords']
        keyword = response.meta['keyword']
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(total / 50.0))
        try:
            self.logger.info('for keyword: "{keyword}" count jobs: {count}; total: {total}; pages {pages}'.format(keyword=unicode(keyword, 'utf8'), count=len(offers), total=total, pages=self.pagination[keyword]['pages']))
        except Exception as ex:
            pass
        page_results = {'job_board': 'ec.europa.eu-api', 'job_board_url': 'https://ec.europa.eu', 'page_url': response.url, 'offers': []}
        for offer in offers:
            url = 'https://ec.europa.eu/eures/portal/jv-se/jv-details/{id}'.format(id=offer['id'])
            job = {'url': url}
            job['html'] = json.dumps(offer)
            job['title'] = offer['title']
            job['keyword_type'] = self.keywords_type
            job['company_name'] = offer['employer']['name']
            if 'description' in offer['employer'] and offer['employer']['description'] is not None:
                job['company_info'] = offer['employer']['description']
            if 'sectorCodes' in offer['employer'] and offer['employer']['sectorCodes'] is not None:
                job['sector'] = ','.join(offer['employer']['sectorCodes'])
            if 'FR' in offer and offer['FR'] is not None:
                job['location'] = ','.join(offer['FR'])
            job['publish_date'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(offer['creationDate']/1000))
            job['jobCategoriesCodes'] = offer['jobCategoriesCodes']
            job['description'] = offer['description']
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            apiUrl = 'https://ec.europa.eu/eures/eures-apps/searchengine/page/jv/id/{job_id}?lang=fr'.format(job_id=offer['id'])
            availableLanguages = offer['availableLanguages']
            if self.check_runtime():
                request = scrapy.Request(apiUrl, callback=self.parse_job_detail_page, dont_filter=True)
                request.meta['job'] = job
                request.meta['availableLanguages'] = availableLanguages
                page_results['offers'].append(job)
                yield request
        page = self.pagination[keyword]['page']
        if page < self.pagination[keyword]['pages'] and self.check_runtime():
            self.pagination[keyword]['page'] += 1
            body = {
                "keywords": [],
                "publicationPeriod": None,
                "occupationUris": [], "skillUris": [],
                "requiredExperienceCodes": [],
                "positionScheduleCodes": [],
                "sectorCodes": [],
                "availableLanguages": [],
                "educationLevelCodes": [],
                "positionOfferingCodes": [],
                "locationCodes": ["fr"],
                "euresFlagCodes": [],
                "otherBenefitsCodes": [],
                "requiredLanguages": [],
                "resultsPerPage": 50,
                "sortSearch": "BEST_MATCH",
                "page": page,
                "sessionId": "aqa63ou1wxptpzuxee0lg"
            }
            body['keywords'].append({"keyword": keyword, "specificSearchCode": "EVERYWHERE"})
            if self.delta:
                body['publicationPeriod'] = 'LAST_DAY'
            url = 'https://ec.europa.eu/eures/eures-apps/searchengine/page/jv-search/search'
            request = scrapy.Request(url, method='POST', body=json.dumps(body), headers={'Content-Type': 'application/json'}, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        offer = json.loads(response.text)
        job = response.meta['job']
        availableLanguages = response.meta['availableLanguages']
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'ec.europa.eu-api')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        lang = availableLanguages[0]
        if 'fr' in availableLanguages:
            lang = 'fr'
        if lang in offer['jvProfiles'] and 'requiredEducationLevelCode' in offer['jvProfiles'][lang] and offer['jvProfiles'][lang]['requiredEducationLevelCode'] is not None:
            if type(offer['jvProfiles'][lang]['requiredEducationLevelCode']) is list:
                l.add_value('education_level', ', '.join(offer['jvProfiles'][lang]['requiredEducationLevelCode']))
            else:
                l.add_value('education_level', offer['jvProfiles'][lang]['requiredEducationLevelCode'])
        if lang in offer['jvProfiles'] and 'requiredYearsOfExperience' in offer['jvProfiles'][lang] and offer['jvProfiles'][lang]['requiredYearsOfExperience'] is not None:
            l.add_value('experience_level', str(offer['jvProfiles'][lang]['requiredYearsOfExperience']))
        if lang in offer['jvProfiles'] and 'positionOfferingCode' in offer['jvProfiles'][lang] and offer['jvProfiles'][lang]['positionOfferingCode'] is not None:
            if type(offer['jvProfiles'][lang]['positionOfferingCode']) is list:
                l.add_value('contract_type', ', '.join(offer['jvProfiles'][lang]['positionOfferingCode']))
            else:
                l.add_value('contract_type', offer['jvProfiles'][lang]['positionOfferingCode'])
        if lang in offer['jvProfiles'] and 'description' in offer['jvProfiles'][lang] and offer['jvProfiles'][lang]['description'] is not None:
            content = offer['jvProfiles'][lang]['description']
        if lang in offer['jvProfiles'] and 'applicationInstructions' in offer['jvProfiles'][lang] and offer['jvProfiles'][lang]['applicationInstructions'] is not None:
            content += '. '
            content += ', '.join(offer['jvProfiles'][lang]['applicationInstructions'])
            content = self.h.handle(content).strip()
            l.add_value('content', content)
        location = self.parseLocations(offer, lang)
        if len(location) > 0:
            l.add_value('location', location)
        header = json.dumps(offer)
        html_content = json.dumps(offer)
        l.add_value('source', 'scrapy')
        l.add_value('header', header)
        l.add_value('html_content', html_content)
        yield l.load_item()
        job_details = {'html': html_content, 'title': header, 'url': job['url']}
        if not self.drain:
            self.es_exporter.insert_job_details_html(job_details)

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date

    def parseLocations(self, offer, lang):
        location = []
        if 'locations' in offer['jvProfiles'][lang] and len(offer['jvProfiles'][lang]) > 0:
            location.append('\n'.join(offer['jvProfiles'][lang]['locations'][0]['addressLines']))
            if offer['jvProfiles'][lang]['locations'][0]['buildingAddress'] is not None:
                location.append(offer['jvProfiles'][lang]['locations'][0]['buildingAddress'])
            if offer['jvProfiles'][lang]['locations'][0]['cityName'] is not None:
                location.append(offer['jvProfiles'][lang]['locations'][0]['cityName'])
            if offer['jvProfiles'][lang]['locations'][0]['countryCode'] is not None:
                location.append(offer['jvProfiles'][lang]['locations'][0]['countryCode'])
            if offer['jvProfiles'][lang]['locations'][0]['postalCode'] is not None:
                location.append(offer['jvProfiles'][lang]['locations'][0]['postalCode'])
            if offer['jvProfiles'][lang]['locations'][0]['region'] is not None:
                location.append(offer['jvProfiles'][lang]['locations'][0]['region'])
        return '\n'.join(location)

    def check_runtime(self):
        now = int(time.time())
        if now - self.start_time < self.max_runtime:
            return True
        return False






