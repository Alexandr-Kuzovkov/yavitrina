# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import LorealJob


class LorealSpider(scrapy.Spider):

    name = "loreal"
    publisher = "L'OREAL"
    publisherurl = 'https://career.loreal.com'

    def start_requests(self):
        allowed_domains = ["career.loreal.com"]
        urls = [
            'https://career.loreal.com/careers/SearchJobs/?jobOffset=0'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parsePage)


    def parsePage(self, response):
        self.logger.info('Parsing page %s ...' % response.url)
        items = response.css('ul.jobList li.jobResultItem')
        for item in items:
            url = item.xpath('a[@class="readMore"]/@href').extract()[0]
            date = self.getPublishDate(item)
            request = scrapy.Request(url=url, callback=self.parseJob)
            request.meta['date'] = date
            yield request

        select_to_link_next = response.xpath("//a[contains(.//text(), 'Next')]")
        if len(select_to_link_next) != 0:
            link_to_next = select_to_link_next.xpath('@href').extract()[0]
            yield scrapy.Request(url=link_to_next, callback=self.parsePage)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        #raise Exception('Custom error')
        l = ItemLoader(item=LorealJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('referencenumber', response.url.split('/').pop().strip())
        l.add_value('title', response.css('h1').xpath('text()').extract()[0].strip())
        l.add_value('date', response.meta['date'])
        try:
            l.add_value('category', response.css('div.jobDetail').xpath('p/text()')[0].extract().strip())
        except Exception, ex:
            l.add_value('category', '')
        try:
            l.add_value('contract', response.css('div.jobDetail').xpath('p/text()')[1].extract().strip())
        except Exception, ex:
            l.add_value('contract', '')
        try:
            l.add_value('location', response.css('div.jobDetail').xpath('p/text()')[2].extract().strip())
        except Exception, ex:
            l.add_value('location', '')
        try:
            l.add_value('country', response.css('div.jobDetail').xpath('p/text()')[3].extract().strip())
        except Exception, ex:
            l.add_value('country', '')
        l.add_value('description',  self.getDescription(response))
        l.add_value('utime', int(time.time()))
        yield l.load_item()


    def getPublishDate(self, selector):
        months = {u'Jan': 1, u'Feb': 2, u'Mar': 3, u'Apr': 4, u'May': 5, u'Jun': 6, u'Jul': 7, u'Aug': 8, u'Sep': 9,
                  u'Oct': 10, u'Nov': 11, u'Dec': 12}
        date_string = selector.xpath('p[@class="jobDef"]/span')[2].xpath('text()').extract()[0].split('Published date:').pop().strip()
        day, month, year = date_string.split('-')
        day = int(day)
        month = months[month]
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def getDescription(self, response):
        desc = ''.join(response.xpath('//div[@class="jobDetailDescription"]//*|//div[@class="jobDetailDescription"]/text()').extract())
        return desc
