# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import OnepointJob
from jobscrapers.extensions import Geocode
import hashlib

class OnepointSpider(scrapy.Spider):

    name = "onepoint"
    publisher = "Onepoint"
    publisherurl = 'https://www.groupeonepoint.com/'
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["https://www.groupeonepoint.com"]
        urls = [
            'https://www.groupeonepoint.com/page/1/?s&post_type=post'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parsePage)


    def parsePage(self, response):
        self.logger.info('Parsing page %s ...' % response.url)
        links = response.css('span.headline a').xpath('@href').extract()
        for link in links:
            request = scrapy.Request(url=link, callback=self.parseJob)
            yield request

        select_to_link_next = response.css('p.pageNext a')
        if len(select_to_link_next) != 0:
            link_to_next = select_to_link_next.xpath('@href').extract()[0]
            yield scrapy.Request(url=link_to_next, callback=self.parsePage)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        #raise Exception('Custom error')
        l = ItemLoader(item=OnepointJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.css('h2 span.headline').xpath('text()')[0].extract().strip())
        l.add_value('date', self.getPublishDate(response.css('meta[property="article:published_time"]')))
        l.add_value('id', hashlib.md5(response.url).hexdigest())
        select = response.css('div.btArticleBody ul')
        country = 'FR'
        if len(select) != 0:
            info_list = select[(len(select) - 1)].xpath('li').xpath('text()').extract()
            info_list = map(lambda s: {s.split(':')[0].strip(): s.split(':')[1].strip()}, filter(lambda s: ':' in s, info_list))
            info = {}
            for info_item in info_list:
                for key, value in info_item.items():
                    info[key] = value
            if u'Expérience' in info:
                l.add_value('experience', info[u'Expérience'])
            if u'Type de contrat' in info:
                l.add_value('contract', info[u'Type de contrat'])
            if u'Localisation' in info:
                location = info[u'Localisation']
                l.add_value('location', location)
                countryinfo = self.geocode.city2countryinfo(location.split(' ')[0])
                if countryinfo is not None:
                    country = countryinfo['ISO']
            if u'Référence' in info:
                l.add_value('category', info[u'Référence'])
        l.add_value('country', country)
        l.add_value('description', response.css('div.btArticleBody').extract()[0])
        yield l.load_item()


    def getPublishDate(self, selector):
        if len(selector) == 0:
            return None
        date_string = selector.xpath('@content')[0].extract().split('T')[0]
        year, month, day = date_string.split('-')
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

