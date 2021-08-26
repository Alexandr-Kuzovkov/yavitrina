# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import HaysJob
import time
import pkgutil
from scrapy_splash import SplashRequest


class HaysSpider(scrapy.Spider):

    name = "hays"
    publisher = "Hays"
    publisherurl = 'https://m.hays.fr/search'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/hays.lua')
    url_index = None
    dirname = 'hays'

    def __init__(self, url_index=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if url_index is not None:
            self.url_index = int(url_index)

    def start_requests(self):
        allowed_domains = ["https://m.hays.fr"]
        urls = [
            'https://m.hays.fr/search/?q=&location=&specialismId=&subSpecialismId=&locationf=&industryf=CONSTRUCTION,%20B%C3%82TIMENT%20%26%20TRAVAUX%20PUBLICS&sortType=0&jobType=-1&payTypefacet=-1&minPay=0&maxPay=11&jobSource=HaysGCJ',
            'https://m.hays.fr/search/?q=&location=paris,%20France&specialismId=&subSpecialismId=&locationf=&industryf=SUPPLY%20CHAIN,%20LOGISTIQUE&sortType=0&jobType=-1&payTypefacet=-1&minPay=0&maxPay=11&jobSource=HaysGCJ',
            'https://m.hays.fr/search/?q=&location=paris,%20France&specialismId=&subSpecialismId=&locationf=&industryf=INDUSTRIE%20%26%20PRODUCTION&sortType=0&jobType=-1&payTypefacet=-1&minPay=0&maxPay=11&jobSource=HaysGCJ',
            'https://m.hays.fr/search/?q=&location=paris,%20France&specialismId=&subSpecialismId=&locationf=&industryf=COMMERCE%20%26%20GRANDE%20DISTRIBUTION&sortType=0&jobType=-1&payTypefacet=-1&minPay=0&maxPay=11&jobSource=HaysGCJ'
        ]
        for index in range(len(urls)):
            if self.url_index is not None and index != self.url_index:
                continue
            url = urls[index]
            request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})
            parsed = urlparse.urlparse(url)
            request.meta['industry'] = '-'.join(urlparse.parse_qs(parsed.query)['industryf'])
            request.meta['search_url'] = url
            yield request


    def get_jobs_list(self, response):
        search_url = response.meta['search_url']
        industry = response.meta['industry']
        self.logger.info('Parsing page %s ...' % search_url)
        data = json.loads(response.text)
        self.logger.info('%i items was fetched' % len(data))
        for key, job_data in data.items():
            url = ''.join(['https://m.hays.fr', job_data['link']])
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['title'] = job_data['title']
            request.meta['subtitle'] = job_data['subtitle']
            request.meta['industry'] = industry
            request.meta['url'] = url
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=HaysJob())
        l.add_value('title', response.meta['title'])
        l.add_value('subtitle', response.meta['subtitle'])
        l.add_value('description', ' '.join(response.css('.hays-result-description')[0].xpath('text()').extract()).strip())
        l.add_value('industry', response.meta['industry'])
        l.add_value('name', self.get_job_name(response.meta['url']))
        yield l.load_item()

    def get_job_name(self, url):
        parsed = urlparse.urlparse(url)
        name = '-'.join(urlparse.parse_qs(parsed.query)['jobName']).replace('/', '-')
        return name

