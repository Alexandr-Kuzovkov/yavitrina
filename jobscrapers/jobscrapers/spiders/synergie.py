# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import SynergieItem
from jobscrapers.items import annotations_list
import time
import pkgutil
#from scrapy_splash import SplashRequest
import math
import re
import os

r_title = re.compile('^.*F/H')
r_city = re.compile('F/H,.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class SynergieSpider(scrapy.Spider):

    name = "synergie"
    publisher = "Synergie"
    publisherurl = 'https://synergie.fr/'
    #lua_src = pkgutil.get_data('jobscrapers', 'lua/pole-emploi.lua')
    url_index = None
    dirname = 'synergie'
    limit = False
    drain = False
    reannotate_only = False


    def __init__(self, limit=False, drain=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if ra:
            self.reannotate_only = True

    def start_requests(self):
        if self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            allowed_domains = ["https://synergie.fr"]
            url = 'https://www.synergie.fr'
            request = scrapy.Request(url, callback=self.get_count)
            yield request

    def get_count(self, response):
        url = 'https://www.synergie.fr/alert/offers/count'
        request = scrapy.Request(url, method='POST', callback=self.get_pages)
        yield request

    def get_pages(self, response):
        count = int(response.text)
        if self.limit:
            count = self.limit
        self.logger.info('Count={count}'.format(count=count))
        for page in range(0, int(math.ceil(count / 10.0))):
            url = 'https://www.synergie.fr/search'
            if page > 0:
                url = 'https://www.synergie.fr/search?page={page}'.format(page=page)
            request = scrapy.Request(url, callback=self.parse_page)
            request.meta['search_url'] = url
            yield request

    def parse_page(self, response):
        uris = response.css('h2.offer-title a').xpath('@href').extract()
        for uri in uris:
            url = ''.join(['https://www.synergie.fr', uri])
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['uri'] = uri.split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SynergieItem())
        l.add_value('name', response.meta['uri'])
        #title#
        full_title = response.css('div.offer-label h1').xpath('text()')[0].extract()
        title = r_title.search(full_title).group()
        l.add_value('title', title)
        l.add_value('title', 'position')
        city = r_city.search(full_title).group()[4:-1].strip()
        l.add_value('city', city)
        l.add_value('city', 'city')
        postal_code = r_postal_code.search(full_title).group()[1:-1]
        l.add_value('postal_code', postal_code)
        l.add_value('postal_code', 'postal_code')
        l.add_value('ref', ''.join(response.css('div.offer-reference').xpath('text()').extract() + response.css('div.offer-reference span').xpath('text()').extract()).strip())
        l.add_value('ref', 'O')
        #job_type
        job_type_l = response.css('div.contract-type .offer-detail-item > div')[0].xpath('text()').extract()[0].strip()
        #job_type_l = response.css('div[class="offer-detail-item clearfix"]')[0].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
        l.add_value('job_type_l', job_type_l)
        l.add_value('job_type_l', 'O')
        job_type = response.css('div.contract-type .offer-detail-item > div > div')[0].xpath('text()').extract()[0].strip()
        #job_type = response.css('div[class="offer-detail-item clearfix"]')[3].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
        l.add_value('job_type', job_type)
        l.add_value('job_type', 'contrat_type')
        #salary
        salary_l = response.css('div[class="offer-detail-item clearfix"]')[1].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
        l.add_value('salary_l', salary_l)
        l.add_value('salary_l', 'O')
        salary = response.css('div[class="offer-detail-item clearfix"]')[1].css('div')[0].css('div')[3].xpath('text()').extract()[0].strip()
        l.add_value('salary', salary)
        l.add_value('salary', 'salary')
        #category
        try:
            category_l = response.css('div[class="offer-detail-item clearfix"]')[2].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            l.add_value('category_l', category_l)
            l.add_value('category_l', 'O')
            category = response.css('div[class="offer-detail-item clearfix"]')[2].css('div')[0].css('div')[3].css('div')[2].xpath('text()').extract()[0].strip()
            l.add_value('category', category)
            l.add_value('category', 'O')
        except Exception:
            # city
            city_l = response.css('div[class="offer-detail-item clearfix"]')[4].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            l.add_value('city_l', city_l)
            l.add_value('city_l', 'O')
            city_full = response.css('div[class="offer-detail-item clearfix"]')[3].css('div')[0].css('div')[3].css('div')[2].xpath('text()').extract()[0].strip()
        else:
            # city
            city_l = response.css('div[class="offer-detail-item clearfix"]')[4].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            l.add_value('city_l', city_l)
            l.add_value('city_l', 'O')
            city_full = response.css('div[class="offer-detail-item clearfix"]')[4].css('div')[0].css('div')[3].css('div')[2].xpath('text()').extract()[0].strip()
        city = r_city2.search(city_full).group()[:-1].strip()
        l.add_value('city2', city)
        l.add_value('city2', 'city')
        postal_code = r_postal_code.search(city_full).group()[1:-1]
        l.add_value('postal_code2', postal_code)
        l.add_value('postal_code2', 'postal_code')
        #expirience
        try:
            experience_l = response.css('div[class="offer-detail-item clearfix"]')[3].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            l.add_value('experience_l', experience_l)
            l.add_value('experience_l', 'O')
            experience = response.css('div[class="offer-detail-item clearfix"]')[3].css('div')[0].css('div')[3].xpath('text()').extract()[0].strip()
            l.add_value('experience', experience)
            l.add_value('experience', 'experience')
        except Exception:
            pass
        #edication
        education_l = response.css('div[class="offer-detail-item clearfix"]')[5].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
        l.add_value('education_l', education_l)
        l.add_value('education_l', 'O')
        education = response.css('div[class="offer-detail-item clearfix"]')[5].css('div')[0].css('div')[3].xpath('text()').extract()[0].strip()
        l.add_value('education', education)
        l.add_value('education', 'education')
        start_time_l = response.css('div[class="offer-detail-item clearfix"]')[6].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
        l.add_value('start_time_l', start_time_l)
        l.add_value('start_time_l', 'O')
        try:
            start_time = response.css('div[class="offer-detail-item clearfix"]')[6].css('div')[0].css('div')[3].css('time').xpath('text()').extract()[0].strip()
        except Exception:
            start_time = response.css('div[class="offer-detail-item clearfix"]')[5].css('div')[0].css('div')[3].css('time').xpath('text()').extract()[0].strip()
        l.add_value('start_time', start_time)
        l.add_value('start_time', 'O')
        blocks = len(response.css('div[class="offer-detail-item clearfix"]'))
        if blocks > 8:
            end_time_l = response.css('div[class="offer-detail-item clearfix"]')[7].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            end_time = response.css('div[class="offer-detail-item clearfix"]')[7].css('div')[0].css('div')[3].css('time').xpath('text()').extract()[0].strip()
            l.add_value('end_time_l', end_time_l)
            l.add_value('end_time_l', 'O')
            l.add_value('end_time', end_time)
            l.add_value('end_time', 'O')

        #experience2
        try:
            experience2_l = response.css('div[class="offer-detail-item clearfix"]')[blocks-1].css('div')[0].css('div')[1].xpath('text()').extract()[0].strip()
            l.add_value('experience2_l', experience2_l)
            l.add_value('experience2_l', 'O')
            experience2 = ' '.join(map(lambda i: i.strip(), response.css('div.specialisation-item').xpath('text()').extract()))
            l.add_value('experience2', experience2)
            l.add_value('experience2', 'experience')
        except Exception:
            pass
        desc1 = response.css('div.offer-info-item h2')[0].xpath('text()').extract()
        desc2 = response.css('div.offer-info-item div')[0].xpath('text()').extract()
        desc3 = response.css('div.offer-info-item h2')[1].xpath('text()').extract()
        desc4 = response.css('div.offer-info-item div')[1].xpath('text()').extract()
        desc5 = response.css('div.offer-info-item h2')[2].xpath('text()').extract()
        desc = ' '.join(desc1 + desc2 + desc3 + desc4 + desc5)
        l.add_value('desc1', desc)
        l.add_value('desc1', 'O')
        desc2 = ' '.join(response.css('div[property="schema:description"]').xpath('text()').extract())
        l.add_value('desc2', desc2)
        l.add_value('desc2', 'mission')
        desc3 = response.css('div.offer-info-item h2')[3].xpath('text()').extract()[0]
        l.add_value('desc3', desc3)
        l.add_value('desc3', 'O')
        desc4 = ' '.join(response.css('div[property="schema:skills"]').xpath('text()').extract())
        l.add_value('desc4', desc4)
        l.add_value('desc4', 'hard_skills')
        yield l.load_item()

    def reannotate(self, response):
        self.logger.info('RUN REANNOTATE ONLY!!!')
        files_dir = self.settings.get('FILES_DIR', '')
        source_dir = os.path.sep.join([files_dir, self.dirname, 'src'])
        list_of_files = os.listdir(source_dir)
        self.logger.info('%i files will reannotate' % len(list_of_files))
        for name in list_of_files:
            with open(os.path.sep.join([source_dir, name]), 'r') as fi:
                item = json.load(fi, 'utf-8')
                item['name'] = [name[:-4]]
                yield item



