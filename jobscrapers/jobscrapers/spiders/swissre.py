# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from pprint import pprint
from scrapy_splash import SplashRequest
import json
from jobscrapers.extensions import Geocode
from jobscrapers.items import SwissreJob
import pkgutil

class SwissreSpider(scrapy.Spider):

    name = "swissre"
    publisher = "SwissRe"
    publisherurl = 'http://www.swissre.com/careers/'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/swissre.lua')
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["https://career2.successfactors.eu"]
        urls = [
            'https://career2.successfactors.eu/career?company=SwissRe&career_ns=job_listing_summary'
        ]
        for url in urls:
            yield SplashRequest(url, self.getJobLinks, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})


    def getJobLinks(self, response):
        jobs_data = json.loads(response.text)
        for job_link in jobs_data.values():
            url = response.urljoin(job_link)
            yield scrapy.Request(url=url, callback=self.parseJob)


    def parseJob(self, response):
        l = ItemLoader(item=SwissreJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.css('.joqReqDescription p strong span')[0].xpath('text()').extract()[0])
        l.add_value('location', response.xpath('//div[@tabindex="0"]/b/text()')[3].extract())
        l.add_value('id', response.xpath('//input[@id="career_job_req_id"]').xpath('@value')[0].extract())
        l.add_value('date', self.getPublishDate(response.xpath('//div[@tabindex="0"]/b/text()')[1].extract()))
        place = response.xpath('//div[@tabindex="0"]/b/text()')[2].extract()
        countryinfo = self.geocode.city2countryinfo(place)
        if countryinfo is not None:
            l.add_value('country', countryinfo['ISO'])
        else:
            l.add_value('country', None)
        l.add_value('job_type', response.xpath('//div[@tabindex="0"]/b/text()')[4].extract())
        l.add_value('category', response.css('span#mfield_mfield1').xpath('@onclick')[0].re('\[.*\]')[0].replace('[','').replace(']',''))
        l.add_value('description', response.css('div.joqReqDescription')[0].extract())
        yield l.load_item()


    def getPublishDate(self, date_string):
        day, month, year = map(lambda i: i.strip(), date_string.strip().split('/'))
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date



