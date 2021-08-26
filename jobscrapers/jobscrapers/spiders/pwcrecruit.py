# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import PwcrecruitJob
from jobscrapers.extensions import Geocode



class PwcrecruitSpider(scrapy.Spider):

    name = "pwcrecruit"
    publisher = 'Job opportunities at PwC Luxembourg'
    publisherurl = 'https://pwcrecruit.pwc.lu'
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["pwcrecruit.pwc.lu"]
        urls = [
            'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/home.xhtml'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parseMainPage)

    def parseMainPage(self, response):
        list_data_id = response.css('li').xpath('@data-id').extract()
        for data_id in list_data_id:
            link = 'https://pwcrecruit.pwc.lu/eRecrutementJobs/view/job.xhtml?jobId=' + data_id
            yield scrapy.Request(url=link, callback=self.parseJob)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        # raise Exception('Custom error')
        l = ItemLoader(item=PwcrecruitJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.css('h2.title')[0].xpath('text()').extract()[0])
        l.add_value('category', response.css('div.etiquette')[0].xpath('text()')[0].extract().strip())
        l.add_value('date', self.getPublishDate(response))
        l.add_value('contract', response.css('ul.share')[0].xpath('//h4/text()')[1].extract())
        l.add_value('description', self.getDescription(response))
        l.add_value('experience', response.xpath('//header/h5')[2].xpath('text()').extract()[0].strip())
        l.add_value('referencenumber', response.css('h5.reference').xpath('text()')[0].extract().split('Ref:')[1].strip())
        l.add_value('country', self.geocode.country2iso('Luxembourg'))
        l.add_value('utime', int(time.time()))
        yield l.load_item()


    def getPublishDate(self, response):
        months = {u'Jan': 1, u'Feb': 2, u'Mar': 3, u'Apr': 4, u'May': 5, u'Jun': 6, u'Jul': 7, u'Aug': 8, u'Sep': 9,
                  u'Oct': 10, u'Nov': 11, u'Dec': 12}
        try:
            date_string = response.css('h5.desktop-only')[0].xpath('//span/text()')[3].extract()
            day, month, year = date_string.split(' ')
            day = int(day)
            month = months.get(month, 1)
            year = int(year)
            date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
            return date
        except Exception, ex:
            self.logger.error(ex)
            return None

    def getDescription(self, response):
        try:
            desc1 = ''.join(response.xpath('//div[@class="mission"]/text()|//div[@class="mission"]//*').extract())
        except Exception, ex:
            desc1 = ''
        desc2 = ''.join(response.css('section.job-profile ul').extract())
        desc = ''.join([desc1, desc2]).strip()
        return desc
