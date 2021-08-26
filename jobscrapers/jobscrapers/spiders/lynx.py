#!/usr/bin/env python
#coding=utf-8

import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import LynxJob
#from geocode import Geocode
from pprint import pprint
from jobscrapers.extensions import Geocode


class LynxSpider(scrapy.Spider):
    name = "lynx"
    publisher = 'Lynx'
    publisherurl = 'https://www.lynx-rh.com'
    geocode = Geocode()
    headers = {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
                'Origin':'https://www.lynx-rh.com',
                'Referer':'https://www.lynx-rh.com/offres-emploi',
                'Content-type':'application/x-www-form-urlencoded'
                }
    body = 'search=true&page=1&par_page=1000&tri=1&mots=&villes=&filters='

    def start_requests(self):
        allowed_domains = ["https://www.lynx-rh.com"]
        urls = [
            'https://www.lynx-rh.com/offres-emploi'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.getContent)


    def getContent(self, response):
        url = response.url
        yield scrapy.Request(url=url, callback=self.parsePage, method='POST', headers=self.headers, body=self.body)


    def parsePage(self, response):
        links = response.css('.search-result-inner h2 a').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(url=link, callback=self.parseJob)

    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        # raise Exception('Custom error')
        l = ItemLoader(item=LynxJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.css('h1').xpath('text()').extract()[0])
        try:
            l.add_value('date',
                        self.getPublishDate(self.getInfoByIcon(response, 'ico-eye')))
        except Exception, ex:
            l.add_value('date', None)
        try:
            l.add_value('contract', self.getInfoByIcon(response, 'ico-list'))
        except Exception, ex:
            l.add_value('contract', None)
        try:
            l.add_value('salary', self.getInfoByIcon(response, 'ico-euro'))
        except Exception, ex:
            l.add_value('salary', None)
        country = 'FR'
        try:
            location = response.css('p.widgettitle small').xpath('text()').extract()[0]
            l.add_value('location', location)
            countryinfo = self.geocode.city2countryinfo(location)
            if countryinfo is not None:
                country = countryinfo['ISO']
        except Exception, ex:
            l.add_value('location', None)
        try:
            id = response.url[:-1].split('/').pop()
            l.add_value('referencenumber', response.css('.section-inner strong').xpath('text()').extract()[0].replace('Ref: ',''))
        except Exception, ex:
            l.add_value('referencenumber', id)
        l.add_value('country', country)
        l.add_value('description', self.getDescription(response))

        yield l.load_item()

    def getInfoByIcon(self, response, icon):
            li_list = response.css('.list-icons li')
            i_class_list = response.css('.list-icons li i').xpath("@class").extract()
            try:
                index = i_class_list.index(icon)
                text = li_list[index].xpath('text()').extract()[1].strip()
                for rep in [' : ', 'Type de contrat', 'Date de publication', 'Salaire']:
                    text = text.replace(rep, '')
                return text.strip()
            except Exception, ex:
                return None

    def getPublishDate(self, date_string):
        day, month, year = date_string.strip().split('/')
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def getDescription(self, response):
        desc = ''.join(response.css('div.article-inner').extract())
        for rep in [' ', "\t", "\n"]:
            desc = desc.replace(rep, '')
        return desc



