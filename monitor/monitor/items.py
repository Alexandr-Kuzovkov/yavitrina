# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class LpJobItem(scrapy.Item):
    title = scrapy.Field()
    description = scrapy.Field()
    posted_at = scrapy.Field()
    category = scrapy.Field()
    location = scrapy.Field()
    job_type = scrapy.Field()
    profile_link = scrapy.Field()
    logo_url = scrapy.Field()
