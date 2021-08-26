# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from pprint import pprint
from scrapy_splash import SplashRequest
import json
from jobscrapers.items import PernodricardJob
from jobscrapers.extensions import Geocode
import pkgutil

class PernodricardSpider(scrapy.Spider):

    name = "pernodricard"
    publisher = "Pernod Ricard"
    publisherurl = 'http://www.pernod-ricard-uk.com/'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/pernodricard2.lua')
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["tas-pr.taleo.net/careersection/"]
        urls = [
            'https://tas-pr.taleo.net/careersection/prext/joblist.ftl#'
        ]
        for url in urls:
            yield SplashRequest(url, self.extractData, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})


    def extractData(self, response):
        fld_list = {
            'Organization': 'company',
            'Primary Location': 'location',
            'Job': 'category',
            'Job Type': 'job_type',
            'Job Posting': 'date',
            'Travel': 'travel',
            'Education Level': 'education'
        }
        jobs_data = json.loads(response.text)
        #f = open('jobs_data.txt', 'w')
        #f.write(str(jobs_data))
        #f.close()
        for job_data in jobs_data.values():
            l = ItemLoader(item=PernodricardJob(), response=response)
            for line in job_data.values():
                key = line.split('-')[0].strip()
                if key in fld_list:
                    if key == 'Job Posting':
                        l.add_value(fld_list[key], self.getPublishDate(line.split('-')[1].strip()))
                    elif key == 'Primary Location':
                        location = line[len(key)+3:].strip()
                        l.add_value('location', location)
                        countryinfo = self.geocode.city2countryinfo(location)
                        if countryinfo is not None:
                            l.add_value('country', countryinfo['ISO'])
                    else:
                        l.add_value(fld_list[key], line[len(key)+3:].strip())
                elif line.strip().find(u'Description') == 0:
                    l.add_value('description', line[len('Description')+1:])
                else:
                    l.add_value('title', line.split('-')[0].strip())
                    job_id = line.split('-').pop().strip()
                    l.add_value('id', job_id)
                    l.add_value('url', 'https://tas-pr.taleo.net/careersection/jobdetail.ftl?job=' + job_id)
            yield l.load_item()


    def getPublishDate(self, date_string):
        months = {u'Jan': 1, u'Feb': 2, u'Mar': 3, u'Apr': 4, u'May': 5, u'Jun': 6, u'Jul': 7, u'Aug': 8, u'Sep': 9,
                  u'Oct': 10, u'Nov': 11, u'Dec': 12}
        month, day, year = map(lambda i: i.strip(), date_string.split(' '))
        day = day.replace(',', '')
        day = int(day)
        month = months[month]
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date



