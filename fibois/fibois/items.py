# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):
    url = scrapy.Field()
    company_info = scrapy.Field()
    title = scrapy.Field()
    content = scrapy.Field()
    contract_type = scrapy.Field()
    contract_info = scrapy.Field()
    profile = scrapy.Field()
    contract_duration = scrapy.Field()
    education = scrapy.Field()
    salary = scrapy.Field()
    location = scrapy.Field()
    city = scrapy.Field()
    postal_code = scrapy.Field()
    sector = scrapy.Field()
    publish_date = scrapy.Field()
    scrapping_date = scrapy.Field()
    search_term = scrapy.Field()
    contact = scrapy.Field()
    url_origin = scrapy.Field()
    jobboard = scrapy.Field()
    job_type = scrapy.Field()
    category = scrapy.Field()
    source = scrapy.Field()
    keyword_type = scrapy.Field()
    description = scrapy.Field()
    company_name = scrapy.Field()
    html = scrapy.Field()
    header = scrapy.Field()
    html_content = scrapy.Field()
    education_level = scrapy.Field()
    experience_level = scrapy.Field()
    jobCategoriesCodes = scrapy.Field()
    id = scrapy.Field()


class JobStatus(scrapy.Item):
    _id = scrapy.Field()
    url = scrapy.Field()
    url_status = scrapy.Field()



