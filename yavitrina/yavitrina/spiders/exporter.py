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
from yavitrina.scrapestack import ScrapestackRequest
from yavitrina.seleniumrequest import SelenuimRequest
from scrapy_headless import HeadlessRequest
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from urllib import urlencode
from yavitrina.items import SettingItem
from yavitrina.items import SettingValueItem
from yavitrina.items import ExSettingItem
from yavitrina.items import ExSettingValueItem
from yavitrina.items import ExProductItem
from yavitrina.items import ExProductColorItem
from yavitrina.items import ExSearchProductItem
from yavitrina.items import ExProductImageItem
from yavitrina.items import ExProductPriceItem
from yavitrina.items import ExReviewItem
from yavitrina.items import ExCategoryItem
from yavitrina.items import ExCategorySearchItem
from yavitrina.items import ExProductCategoryItem
from yavitrina.items import ExTagItem
from yavitrina.items import ExProductSettingsItem
from yavitrina.items import ExNewCategoryItem
from yavitrina.items import ExCategoryHasSettingsItem


class ExporterSpider(scrapy.Spider):
    name = 'exporter'
    dirname = 'exporter'
    handle_httpstatus_list = [400, 404]
    logger = logging.getLogger()
    drain = False
    custom_settings = {
        'LOG_LEVEL': 'DEBUG'
    }
    clear_db = False
    db_import = None
    db_export = None

    def __init__(self, drain=False, cleardb=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if drain:
            self.drain = True
        if cleardb:
            self.clear_db = True

    def start_requests(self):
        url = 'http://localhost:6800'
        request = scrapy.Request(url, callback=self.starting_export)
        yield request


    def starting_export(self, response):
        #get latest
        tables = self.db_export._get_tables_list()
        pprint(tables)
        for table in tables:
            fld_lst = self.db_export._get_fld_list(table)
            print('{table}: ({columns})'.format(table=table, columns=','.join(fld_lst)))

















