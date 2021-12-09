# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
import time
import html2text
import datetime
import logging
import urllib
import urlparse
import requests
from yavitrina.items import CategoryItem
from yavitrina.items import TagItem
from yavitrina.items import ProductCardItem
from yavitrina.items import ProductItem
from yavitrina.items import ImageItem
from yavitrina.items import SettingItem
from yavitrina.items import SettingValueItem
from yavitrina.scrapestack import ScrapestackRequest
from yavitrina.seleniumrequest import SelenuimRequest
from scrapy_headless import HeadlessRequest
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib import urlencode


class Test2Spider(scrapy.Spider):
    name = 'test2'
    dirname = 'test2'
    handle_httpstatus_list = [400, 404]
    logger = logging.getLogger()
    drain = False
    custom_settings = {
        'LOG_LEVEL': 'DEBUG'
    }
    db_import = None
    db_export = None

    def __init__(self, drain=False, noproxy=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True

    def start_requests(self):
        url = 'http://localhost:6800'
        request = scrapy.Request(url, callback=self.handle_request)
        yield request


    def handle_request(self, response):
        l = ItemLoader(item=SettingItem(), response=response)
        l.add_value('name', u'Застежка')
        yield l.load_item()
















