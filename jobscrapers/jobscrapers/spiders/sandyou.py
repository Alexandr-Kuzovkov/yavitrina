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
from scrapy_splash import SplashRequest
import math
import re

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class SandyouSpider(scrapy.Spider):

    name = "sandyou"
    publisher = "Sandyou"
    publisherurl = 'https://sandyou.fr/'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/sandyou.lua')
    url_index = None
    dirname = 'sandyou'
    limit = False
    drain = False

    def __init__(self, limit=False, drain=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True

    def start_requests(self):
        allowed_domains = ["https://sandyou.fr"]
        url = 'https://www.sandyou.fr/search?'
        request = SplashRequest(url, self.get_jobs_list, endpoint='execute',args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})
        yield request

    def get_jobs_list(self, response):
        data = json.loads(response.text)
        #pprint(data)
        values = data.values()
        if self.limit:
            values = values[:self.limit]
        for item in values:
            url = ''.join(['https://www.sandyou.fr', item['link']])
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['title'] = item['title']
            request.meta['name'] = url.split('/').pop()
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=SynergieItem())
        l.add_value('name', response.meta['name'])
        # title#
        title = response.css('div.offre-titre--first h1 a span').xpath('text()').extract()[0] + response.css('div.offre-titre--first h2').xpath('text()').extract()[0]
        l.add_value('title', title)
        l.add_value('title', 'position')
        mres = response.css('div.offre-titre--first h2 span span')[1].xpath('text()').extract()[0]
        city = r_city.search(mres).group()[:-2].strip()
        l.add_value('city', city)
        l.add_value('city', 'city')
        postal_code = r_postal_code.search(mres).group()[1:-1]
        l.add_value('postal_code', postal_code)
        l.add_value('postal_code', 'postal_code')
        l.add_value('ref', response.css('div.offre-titre--second h2 span ').xpath('text()').extract()[0])
        l.add_value('ref', 'O')
        # job_type
        job_type_l = response.css('section.offre-specifs div.row')[0].css('div.offre-specifs_item_top1 strong').xpath('text()').extract()[0]
        l.add_value('job_type_l', job_type_l)
        l.add_value('job_type_l', 'O')
        job_type = response.css('section.offre-specifs div.row')[0].css('div.offre-specifs_item_top1').xpath('span[@property="schema:employmentType"]').xpath('text()').extract()[0]
        l.add_value('job_type', job_type)
        l.add_value('job_type', 'contrat_type')
        # salary
        salary_l = response.css('section.offre-specifs div.row')[0].css('div.offre-specifs_item_top2 strong').xpath('text()').extract()[0]
        l.add_value('salary_l', salary_l)
        l.add_value('salary_l', 'O')
        salary = response.css('section.offre-specifs div.row')[0].css('div.offre-specifs_item_top2').xpath('span[@property="schema:baseSalary"]').xpath('text()').extract()[0]
        l.add_value('salary', salary)
        l.add_value('salary', 'salary')
        start_time_l = response.css('section.offre-specifs div.row')[1].css('div.offre-specifs_item_top1 strong').xpath('text()').extract()[0]
        l.add_value('start_time_l', start_time_l)
        l.add_value('start_time_l', 'O')
        start_time = response.css('section.offre-specifs div.row')[1].css('div.offre-specifs_item_top1 span').xpath('text()').extract()[0]
        l.add_value('start_time', start_time)
        l.add_value('start_time', 'O')
        try:
            end_time_l = response.css('section.offre-specifs div.row')[1].css('div.offre-specifs_item_top2 strong').xpath('text()').extract()[0]
            l.add_value('end_time_l', end_time_l)
            l.add_value('end_time_l', 'O')
            end_time = response.css('section.offre-specifs div.row')[1].css('div.offre-specifs_item_top2 span').xpath('text()').extract()[0]
            l.add_value('end_time', end_time)
            l.add_value('end_time', 'O')
        except Exception:
            pass

        # expirience
        try:
            experience_l = response.css('section.offre-specifs div.row')[2].css('div.offre-specifs_item_bottom strong').xpath('text()').extract()[0]
            l.add_value('experience_l', experience_l)
            l.add_value('experience_l', 'O')
            experience = response.css('section.offre-specifs div.row')[2].css('div.offre-specifs_item_bottom').xpath('span[@property="schema:industry"]').xpath('text()').extract()[0]
            l.add_value('experience', experience)
            l.add_value('experience', 'O')
        except Exception:
            pass

        desc1 = ' '.join(response.css('section.offre-description ul li.list-group-item')[0].css('h3, p').xpath('text()').extract())
        desc2 = ' '.join(response.css('section.offre-description ul li.list-group-item')[1].css('h3, span').xpath('text()').extract())
        desc3 = ' '.join(response.css('section.offre-description ul li.list-group-item')[2].css('h3').xpath('text()').extract())
        desc = ''.join(desc1 + desc2 + desc3)
        l.add_value('desc1', desc)
        l.add_value('desc1', 'O')
        desc2 = ''.join(response.css('span[property="schema:description"]').xpath('text()').extract())
        l.add_value('desc2', desc2)
        l.add_value('desc2', 'mission')
        desc3 = ' '.join(response.css('section.offre-description ul li.list-group-item')[3].css('h3').xpath('text()').extract())
        l.add_value('desc3', desc3)
        l.add_value('desc3', 'O')
        desc4 = ' '.join(response.css('span[property="schema:skills"]').xpath('text()').extract())
        l.add_value('desc4', desc4)
        l.add_value('desc4', 'hard_skills')
        yield l.load_item()


