# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import ReseauallianceJob



class ReseauallianceSpider(scrapy.Spider):

    name = "reseaualliance"
    publisher = 'Réseau Alliance'
    publisherurl = 'http://reseaualliance.fr/'

    def start_requests(self):
        allowed_domains = ["reseaualliance.fr"]
        urls = [
            'http://reseaualliance.fr/offres'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parsePage)


    def parsePage(self, response):
        self.logger.info('Parsing page %s ...' % response.url)
        links = response.css('div.panel-body div.list-group a.row.list-group-item').xpath('@href').extract()
        for link in links:
            yield scrapy.Request(url=link, callback=self.parseJob)

        link_to_next = response.css('li.next a')[0].xpath('@href').extract()[0]
        if link_to_next != '#':
            yield scrapy.Request(url=link_to_next, callback=self.parsePage)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        # raise Exception('Custom error')
        l = ItemLoader(item=ReseauallianceJob(), response=response)
        l.add_value('url', response.url)
        l.add_value('referencenumber', response.url.split('/').pop().strip())
        l.add_value('title', response.css('h2.job-situation').xpath('text()').extract()[0].strip())
        l.add_value('location', response.css('span.job-activity span').xpath('text()')[0].extract().strip().replace('\t', ''))
        l.add_value('category', response.css('.panel-default span.job-activity small').xpath('text()')[0].extract().strip())
        l.add_value('date', self.getPublishDate(response))
        l.add_value('contract', response.css('h3.job-contract-type span').xpath('text()')[0].extract().strip())
        l.add_value('description', ''.join(response.xpath('//div[@class="job-details"]//*').extract()))
        l.add_value('country', u'France')
        l.add_value('city', response.css('span.job-activity span').xpath('text()')[0].extract().strip().replace('\t', '').split('/').pop().strip())
        l.add_value('utime', int(time.time()))
        yield l.load_item()

    def getPublishDate(self, response):
        months = {u'Janvier': 1, u'Février': 2, u'Mars': 3, u'Avril': 4, u'Mai': 5, u'Juin': 6, u'Juillet': 7,
                  u'Aout': 8, u'Septembre': 9, u'Octobre': 10, u'Novembre': 11, u'Décembre': 12}

        date_string = response.css('div.col-xs-12.col-sm-4.text-muted.text-right small b').xpath('text()')[0].extract().strip()
        day, month, year = date_string.split(' ')
        day = int(day)
        month = months.get(month, 1)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date
