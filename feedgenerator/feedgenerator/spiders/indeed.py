# -*- coding: utf-8 -*-
import scrapy
from feedgenerator.items import JobItem
from scrapy.loader import ItemLoader
import time
import requests
from transliterate import translit
import xml.etree.ElementTree as ET

class IndeedSpider(scrapy.Spider):

    name = 'indeed'
    publisher = 'myXtramile network'
    publisherurl = 'http://myxtramile.com/'
    feed = []
    source_feed_url = 'http://feeds.xtramile.io/indeed.xml'
    start_urls = [source_feed_url]
    item = 'job'
    root = 'source'

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        res = requests.get(self.source_feed_url)
        if res.status_code != 200:
            raise ('Can\' get source XML from %s' % self.source_feed_url)
            exit()
        root = ET.fromstring((res.text).encode('utf-8'))
        for job in root:
            for item in job:
                if item.tag == 'company':
                    self.feed.append(self.company2feedname(item.text))
        self.feed = list(set(self.feed))

    def parse(self, response):
        root = ET.fromstring((response.text).encode('utf-8'))
        for job in root:
            if len(job) == 0:
                continue
            l = ItemLoader(item=JobItem(), response=response)
            for tag in job:
                l.add_value(tag.tag, tag.text)
            yield l.load_item()


    def company2feedname(self, company):
        s = company.strip().replace(' ', '_').lower()
        s = self.removeNonValidChars(self.transliterate(s))
        return str('.'.join([s, 'xml']))

    def convertDate(self, date_string):
        year, month, day = map(lambda i: i.strip(), date_string.strip().split('-'))
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def transliterate(self, str):
        try:
            str = translit(str.strip().lower(), reversed=True)
        except Exception, ex:
            str = str.strip().lower()
        return str

    def removeNonValidChars(self, str):
        c = []
        valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-().'
        for ch in str:
            if ch in valid:
                c.append(ch)
            else:
                c.append('-')
        return ''.join(c)
