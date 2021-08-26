# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
import scrapy
from jobscrapers.items import GermanpersonalItem
from pprint import pprint


class GermanPersonalSpider(XMLFeedSpider):

    name = 'germanpersonal'
    handle_httpstatus_list = [404, 400, 500, 413]
    drain = False
    full = False
    categories = {}
    category_codes = {}
    message_lines = []
    slugs = {}
    limit = 500.0
    dirname = 'germanpersonal'

    def __init__(self, employer_id=None, drain=False, full = False, job_group=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.iterator = 'iternodes'  # you can change this; see the docs
        self.itertag = 'PositionOpening'
        self.start_urls = ['http://www.germanpersonnel.de/persy/sl/p6megOGC']
        if drain:
            self.drain = True
        if full:
            self.full = True

    def parse_node(self, response, node):
        Item = GermanpersonalItem()
        Item['PositionRecordInfo'] = node.xpath('//PositionRecordInfo').extract()
        Item['PositionPostings'] = node.xpath('//PositionPostings').extract()
        Item['PositionProfile'] = node.xpath('//PositionProfile').extract()

        Item['original_url'] = node.xpath('//PositionPostings/PositionPosting/id/InternetReference/text()')[0].extract()
        Item['title'] = node.xpath('//PositionProfile/PositionDetail/PositionTitle/text()')[0].extract()
        Item['external_id'] = node.xpath('//PositionPostings/PositionPosting/id/idValue[@name="StaId"]/text()')[0].extract()
        category_code = node.xpath('//PositionProfile/PositionDetail/JobCategory/JobCategory/CategoryCode/text()')[0].extract()
        category_desc = node.xpath('//PositionProfile/PositionDetail/JobCategory/JobCategory/CategoryDescription/text()')[0].extract()
        Item['category'] = category_desc
        Item['category_code'] = category_code
        self.countCategory(Item['category'], category_code)
        return Item

    def countCategory(self, category, category_code):
        if category in self.categories:
            self.categories[category] += 1
        else:
            self.categories[category] = 1
        if category_code in self.category_codes:
            if category != self.category_codes[category_code]:
               self.category_codes[category_code] = category_code
        else:
            self.category_codes[category_code] = category
