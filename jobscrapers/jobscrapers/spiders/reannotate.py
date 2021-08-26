# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import ReannotationItem
from jobscrapers.items import annotations_list
import time
import pkgutil
import os

class ReannotateSpider(scrapy.Spider):

    name = "reannotate"
    publisher = "Reannotate"
    publisherurl = 'http://localhost/'
    dirname = 'reannotate'
    limit = False
    drain = False
    spidername = None
    spiderdirname = None

    def __init__(self, spidername=None, spiderdirname=None, limit=False, drain=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        self.spidername = spidername
        self.spiderdirname = spiderdirname

    def start_requests(self):
        url = 'http://localhost'
        request = scrapy.Request(url, callback=self.reannotate)
        yield request

    def reannotate(self, response):
        Item = ReannotationItem()
        Item['spidername'] = self.spidername
        Item['spiderdirname'] = self.spiderdirname
        yield Item




