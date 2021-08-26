# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
import time


class GcdApecFeedgeneratorItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


category2naf = {
    1: ['Accounting & Finance Jobs', '6499Z'],
    2: ['IT Jobs', '5829A'],
    3: ['Sales Jobs',  '4619B'],
    4: ['Customer Services Jobs',  '8299Z'],
    5: ['Engineering Jobs', '7490B'],
    6: ['HR & Recruitment Jobs', '7830Z'],
    7: ['Healthcare & Nursing Jobs', '8690F'],
    8: ['Hospitality & Catering Jobs', '5610A'],
    9: ['PR, Advertising & Marketing Jobs', '7311Z'],
    10: ['Logistics & Warehouse Jobs', '5210B'],
    11: ['Teaching, Training & Scientific Jobs', '8560Z'],
    12: ['Trade & Construction Jobs', '4120B'],
    13: ['Admin Jobs', '8299Z'],
    14: ['Legal Jobs', '6910Z'],
    15: ['Culture & Medias', '9003A'],
    16: ['Graduate Jobs', '8560Z'],
    17: ['Retail Jobs', '4719B'],
    18: ['Consultancy Jobs', '7022Z'],
    19: ['Manufacturing & Craftsmanship Jobs', '3299Z'],
    20: ['Agriculture & Environmental Jobs', '0150Z'],
    21: ['Social work Jobs', '8899B'],
    22: ['Travel Jobs', '7911Z'],
    23: ['Energy, Oil & Gas Jobs', '3530Z'],
    24: ['Property Jobs', '6831Z'],
    25: ['Charity & Voluntary Jobs', '9499Z'],
    26: ['Domestic help & Cleaning Jobs', '9700Z'],
    27: ['Installation & Maintenance Jobs', '8299Z'],
    28: ['Part time Jobs', '9499Z'],
    29: ['Defence jobs', '8422Z'],
    30: ['Other/General Jobs', '9499Z'],
    31: ['Chartered accountancy', '6920Z'],
    32: ['Logistics', '5320Z'],
    33: ['Setting/Control', '2910Z'],
    34: ['Automation/Robotics', '2910Z'],
    35: ['Drawing/Studies', '2910Z'],
    36: ['Electrical', '2910Z'],
    37: ['Maintenance', '2910Z'],
    38: ['Mounting/Assembly', '2910Z']
}






