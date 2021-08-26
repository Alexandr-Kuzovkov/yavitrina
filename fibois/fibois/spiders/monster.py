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
from fibois.keywords import last_posted
from fibois.scrapestack import ScrapestackRequest

class MonsterSpider(scrapy.Spider):
    name = 'monster.fr'
    allowed_domains = []
    dirname = 'monster'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
    #keywords = occupations[4:5]
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    lua_src = pkgutil.get_data('fibois', 'lua/html-render.lua')
    debug = False

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, debug=False, *args, **kwargs):
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
        if debug:
            self.debug = True

    def start_requests(self):
        if self.debug:
            self.rundebug()
            return
        for keyword in self.keywords:
            body = {
                "jobQuery":
                    {"locations":
                         [
                             {"address": "", "country": "fr"}
                         ],
                        "excludeJobs": [],
                        "companyDisplayNames": [],
                        "query": keyword,
                        "employmentTypes": []
                    },
                "offset": 0,
                "pageSize": 10,
                "searchId": "",
                "jobAdsRequest": {
                    "position": [5, 4, 3, 2, 2],
                    "placement": {
                        "component": "JSR_SPLIT_VIEW",
                        "appName": "monster"
                    }
                }
            }
            url = 'https://services.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/fr-fr'
            request = scrapy.Request(url, method='POST', body=json.dumps(body), headers={'Content-Type': 'application/json'}, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        # print(response.text)
        data = json.loads(response.text)
        try:
            offers = data['jobResults']
        except KeyError as ex:
            print('Key error!!!')
            pprint(data)
        total = data['estimatedTotalSize']
        search_id = data['searchId']
        keyword = response.meta['keyword']
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(total / 10.0))
        self.logger.info('for keyword: "{keyword}" count jobs: {count}; total: {total}; pages {pages}'.format(
            keyword=unicode(keyword, 'utf8'), count=len(offers), total=total, pages=self.pagination[keyword]['pages']))
        page_results = {'job_board': 'monster', 'job_board_url': 'https://www.monster.fr', 'page_url': response.url, 'offers': []}
        dates = []
        for offer in offers:
            url = self.bypath(offer, ['jobPosting', 'url'])
            job = {'url': url.encode('utf-8')}
            job['html'] = json.dumps(offer)
            job['title'] = self.bypath(offer, ['jobPosting', 'title'])
            job['keyword_type'] = self.keywords_type
            job['description'] = self.bypath(offer, ['jobPosting', 'description'])
            job['publish_date'] = self.bypath(offer, ['jobPosting', 'datePosted'])
            #job['id'] = self.bypath(offer, ['jobId'])
            dates.append(job['publish_date'])
            job['education_level'] = self.bypath(offer, ['jobPosting', 'educationRequirements'])
            job['salary'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['jobPosting', 'baseSalary', 'value', 'value']),
                self.bypath(offer, ['jobPosting', 'baseSalary', 'currency']),
                self.bypath(offer, ['jobPosting', 'baseSalary', 'value', 'unitText'])
            ]))
            job['search_term'] = unicode(keyword, 'utf8')
            #job['profile'] = self.h.handle(job['description'][job['description'].find('Profil recherche')-16:]).strip()
            job['contract_type'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['enrichments', 'employmentTypes', 0, 'name']),
                self.bypath(offer, ['enrichments', 'employmentTypes', 1, 'name']),
                ]))
            job['location'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'streetAddress']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressLocality']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressRegion']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'postalCode']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressCountry']),
                ]))
            job['postal_code'] = self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'postalCode'])
            job['company_name'] =self.bypath(offer, ['jobPosting', 'hiringOrganization', 'name'])
            job['sector'] = self.bypath(offer, ['jobPosting', 'industry'])
            job['jobCategoriesCodes'] = self.bypath(offer, ['jobPosting', 'occupationalCategory'])

            page_results['offers'].append(job)
            l = ItemLoader(item=JobItem(), response=response)
            for key, val in job.items():
                l.add_value(key, val)
            l.add_value('jobboard', 'monster.fr')
            l.add_value('scrapping_date', self.get_scrapping_date())
            l.add_value('contact', self.bypath(offer, ['apply', 'applyUrl']))
            l.add_value('url_origin', job['url'])
            content = self.bypath(offer, ['jobPosting', 'description'])
            content = self.h.handle(content).strip()
            header = ''
            html_content =json.dumps(offer)
            l.add_value('content', content)
            l.add_value('source', 'scrapy')
            l.add_value('header', header)
            l.add_value('html_content', html_content)
            yield l.load_item()

            job_details = {'html': html_content, 'title': header, 'url': job['url']}
            if not self.drain:
                self.es_exporter.insert_job_details_html(job_details)

        page = self.pagination[keyword]['page']
        if page < self.pagination[keyword]['pages'] and self.check_date_posted(dates):
            self.pagination[keyword]['page'] += 1
            offset = 10 * (self.pagination[keyword]['page'] - 1)
            body = {
                "jobQuery":
                    {"locations":
                        [
                            {"address": "", "country": "fr"}
                        ],
                        "excludeJobs": [],
                        "companyDisplayNames": [],
                        "query": keyword,
                        "employmentTypes": []
                    },
                "offset": offset,
                "pageSize": 10,
                "searchId": "",
                "jobAdsRequest": {
                    "position": [1, 2, 3, 4, 5],
                    "placement": {
                        "component": "JSR_SPLIT_VIEW",
                        "appName": "monster"
                    }
                },
                "searchId": search_id
            }

            url = 'https://services.monster.io/jobs-svx-service/v2/monster/search-jobs/samsearch/fr-fr'
            request = scrapy.Request(url, method='POST', body=json.dumps(body), headers={'Content-Type': 'application/json'}, callback=self.parse_job_list_page, dont_filter=True)
            request.meta['keyword'] = keyword
            yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date

    def bypath(self, d, path):
        if type(path) is not list:
            raise Exception('"path" must be a list')
        curr = d
        for item in path:
            if type(item) is str:
                if item in curr:
                    curr = curr[item]
                else:
                    return None
            elif type(item) is int:
                if (type(curr) is list or type(curr) is tuple) and len(curr) > item:
                    curr = curr[item]
                else:
                    return None
            else:
                raise Exception('item of path must be a integer or string')
        return unicode(curr)

    def check_date_posted(self, dates):
        if not self.delta:
            return True
        newest_date = self.get_newest_date(dates)
        now = datetime.datetime.now()
        delta_days = (now - newest_date).days
        if delta_days > 2:
            return False
        else:
            return True

    def get_newest_date(self, dates):
        newest_date = None
        if type(dates) is list:
            for date_str in dates:
                curr_date = self.str2datetime(date_str)
                if newest_date is None:
                    newest_date = curr_date
                else:
                    if newest_date < curr_date:
                        newest_date = curr_date
            if newest_date is None:
                return datetime.datetime(1970, 1, 1, 0, 0, 0)
            return newest_date
        return datetime.datetime.now()

    def str2datetime(self, date_str):
        date_str = ''.join(date_str.split('T')[0:1])
        d = list(map(lambda i: int(i.strip()), date_str.split('-')))
        dt = datetime.datetime(d[0], d[1], d[2], 0, 0, 0, 0)
        return dt


    def rundebug(self):
        self.logger.info('RUN DEBUG!!!!!!!!!!')
        dates = []
        keyword = 'monteur de charpentes bois'
        data = {}
        with open('files/monster-offers.json', 'r') as f:
            data = json.loads(f.read())
        for offer in data['jobResults']:
            url = self.bypath(offer, ['jobPosting', 'url'])
            job = {'url': url}
            job['html'] = json.dumps(offer)
            job['title'] = self.bypath(offer, ['jobPosting', 'title'])
            job['keyword_type'] = self.keywords_type
            job['description'] = self.bypath(offer, ['jobPosting', 'description'])
            job['publish_date'] = self.bypath(offer, ['jobPosting', 'datePosted'])
            job['id'] = self.bypath(offer, ['jobId'])
            dates.append(job['publish_date'])
            job['education_level'] = self.bypath(offer, ['jobPosting', 'educationRequirements'])
            job['salary'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['jobPosting', 'baseSalary', 'value', 'value']),
                self.bypath(offer, ['jobPosting', 'baseSalary', 'currency']),
                self.bypath(offer, ['jobPosting', 'baseSalary', 'value', 'unitText'])
            ]))
            job['search_term'] = unicode(keyword, 'utf8')
            job['profile'] = self.h.handle(job['description'][job['description'].find('Profil recherche') - 16:]).strip()
            job['contract_type'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['enrichments', 'employmentTypes', 0, 'name']),
                self.bypath(offer, ['enrichments', 'employmentTypes', 1, 'name']),
            ]))
            job['location'] = ' '.join(filter(lambda i: i is not None, [
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'streetAddress']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressLocality']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressRegion']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'postalCode']),
                self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'addressCountry']),
            ]))
            job['postal_code'] = self.bypath(offer, ['jobPosting', 'jobLocation', 0, 'address', 'postalCode'])
            job['company_name'] = self.bypath(offer, ['jobPosting', 'hiringOrganization', 'name'])
            job['sector'] = self.bypath(offer, ['jobPosting', 'industry'])
            job['jobCategoriesCodes'] = self.bypath(offer, ['jobPosting', 'occupationalCategory'])
            pprint(job)
        pprint(dates)






