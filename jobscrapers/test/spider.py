#!/usr/bin/env python
#coding=utf-8

import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import SocietegeneraleJob
#from geocode import Geocode
from pprint import pprint
from jobscrapers.extensions import Geocode


class SosietegeneraleSpider(scrapy.Spider):

    name = "societegenerale"
    publisher = 'Societe Generale'
    publisherurl = 'https://careers.societegenerale.com/'
    geocode = Geocode()


    def start_requests(self):
        allowed_domains = ["careers.societegenerale.com"]
        urls = [
            'https://careers.societegenerale.com/job-offers/Pracovn%C3%ADk-ce-klientsk%C3%A9ho-servisu--Rokycany-170009R9-cs',
            'https://careers.societegenerale.com/job-offers/Auditeur-SI---VIE-Casablanca--H-F--160000Z1-fr',
            'https://careers.societegenerale.com/job-offers/Senior-developer-16000HAI-cs',
            'https://careers.societegenerale.com/job-offers/Lead-business-analytik-16000KAK-cs',
            'https://careers.societegenerale.com/job-offers/Projektov%C3%BD-%C3%A1-mana%C5%BEer-ka---senior-RQ00115894-cs'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parseJob)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        if response.url == 'https://careers.societegenerale.com/Careers/sg404':
            return
        #raise Exception('Custom error')
        l = ItemLoader(item=SocietegeneraleJob(), response=response)
        l.add_value('url', response.url)
        try:
            l.add_value('title', response.css('div#sidebarLeft h1').xpath('text()')[0].extract())
        except IndexError, ex:
            self.logger.warning('Fail parsing job from %s' % response.url)
            return
        l.add_value('referencenumber', response.css('div.shadowBox div.shadowBoxContent span')[0].xpath('text()').extract()[0])
        l.add_value('date', self.getPublishDate(response))
        try:
            l.add_value('category', response.css('div.shadowBoxContent dd')[1].xpath('text()').extract()[0].strip())
        except Exception, ex:
            l.add_value('category', '')
        try:
            l.add_value('businessunit', response.css('div.shadowBoxContent dd')[2].xpath('text()').extract()[0].strip())
        except Exception, ex:
            l.add_value('businessunit', '')
        try:
            location = response.css('div.shadowBoxContent dd')[3].xpath('text()').extract()[0].strip()
        except Exception, ex:
            location = None
        l.add_value('location', location)
        countryinfo = self.geocode.city2countryinfo(location)
        if countryinfo is not None:
            l.add_value('country', countryinfo['Country'])
        else:
            l.add_value('country', None)
            self.logger.warning(location)
            self.logger.warning(countryinfo)
        try:
            l.add_value('contract', response.css('div.shadowBoxContent dd')[4].xpath('text()').extract()[0].strip())
        except Exception, ex:
            l.add_value('contract', '')
        l.add_value('description', self.getDescription(response))
        l.add_value('utime', int(time.time()))
        self.logger.info(str(l.load_item()))


    def getPublishDate(self, response):
        try:
            date_string = response.css('span.jobDate span').xpath('text()')[0].extract().strip()
        except Exception, ex:
            date_string = response.css('div.shadowBoxContent dd')[0].xpath('text()').extract()[0].strip()
        try:
            day, month, year = date_string.split('-')
        except Exception, ex:
            return None
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def getDescription(self, response):
        try:
            desc1 = ''.join(response.xpath(
                '//div[@id="section-environnement"]/text()|//div[@id="section-environnement"]//*').extract()).strip()
        except Exception, ex:
            desc1 = ''
        try:
            desc2 = ''.join(
                response.xpath('//div[@id="section-mission"]/text()|//div[@id="section-mission"]//*').extract()).strip()
        except Exception, ex:
            desc2 = ''
        try:
            desc3 = ''.join(
                response.xpath('//div[@id="section-profil"]/text()|//div[@id="section-profil"]//*').extract()).strip()
        except Exception, ex:
            desc3 = ''

        desc = ''.join([desc1, desc2, desc3]).strip()
        return desc


sp = SosietegeneraleSpider()
sp.start_requests()

