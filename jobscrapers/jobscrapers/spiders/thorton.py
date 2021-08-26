# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from pprint import pprint
from scrapy_splash import SplashRequest
import json
from jobscrapers.items import JobItem
from jobscrapers.extensions import Geocode
import pkgutil

class ThortondSpider(scrapy.Spider):

    name = "thorton"
    publisher = "Grant Thorton"
    publisherurl = 'https://ukgrantt.wd3.myworkdayjobs.com/CareersGrantThornton'
    lua_src1 = pkgutil.get_data('jobscrapers', 'lua/thorton_links.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/thorton_job.lua')
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["ukgrantt.wd3.myworkdayjobs.com"]
        urls = [
            'https://ukgrantt.wd3.myworkdayjobs.com/CareersGrantThornton',
            'https://ukgrantt.wd3.myworkdayjobs.com/TraineeCareersGrantThornton'
        ]
        for url in urls:
            yield SplashRequest(url, self.extractLinks, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src1, 'timeout': 3600})


    def extractLinks(self, response):
        objects = json.loads(response.text)
        links = []
        for object in objects.values():
            object = json.loads(object)
            try:
                for item in object['body']['children'][1]['children'][0]['listItems']:
                    links.append(''.join(['/'.join(response.url.split('/')[:-1]), item['title']['commandLink']]))
            except Exception, ex:
                self.logger.warning('Links not found. Possable 0 result!')
        for link in links:
            yield SplashRequest(link, self.parseJob, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2, 'timeout': 3600})


    def parseJob(self, response):
        self.logger.info('Parsing Job from %s...' % response.url)
        s = response.text
        try:
            d = json.loads(s)
        except Exception, ex:
            #f = open('thorton_data.txt', 'w')
            #f.write(s)
            #f.close()
            yield SplashRequest(response.url, self.parseJob, endpoint='execute',
                            args={'wait': 0.5, 'lua_source': self.lua_src2, 'timeout': 3600})
        else:
            l = ItemLoader(item=JobItem(), response=response)
            l.add_value('url', response.url)
            l.add_value('title', d['openGraphAttributes']['title'])
            try:
                l.add_value('description', d['body']['children'][1]['children'][0]['children'][2]['text'])
            except Exception, ex:
                l.add_value('description', d['openGraphAttributes']['description'])
            location = d['body']['children'][1]['children'][0]['children'][0]['imageLabel']
            l.add_value('location', location)
            countryinfo = self.geocode.city2countryinfo(location)
            if countryinfo is not None:
                l.add_value('country', countryinfo['ISO'])
            else:
                l.add_value('country', None)
            l.add_value('date', self.getPublishDate(d['body']['children'][1]['children'][1]['children'][0]['imageLabel']))
            l.add_value('contract', d['body']['children'][1]['children'][1]['children'][1]['imageLabel'])
            l.add_value('referencenumber', d['body']['children'][1]['children'][1]['children'][2]['imageLabel'])
            yield l.load_item()


    def getPublishDate(self, date_string):
        now = int(time.time())
        SpD = 86400
        date = None
        words = map(lambda s: s.strip().replace('+', ''), date_string.split(' '))
        if 'Posted' not in words:
            return date
        if 'Yesterday' in words:
            date = now - SpD
        elif 'Today' in words:
            date = now
        elif 'Ago' in words:
            for word in words:
                if word.isdigit():
                    date = now - SpD * int(word)
        return date





