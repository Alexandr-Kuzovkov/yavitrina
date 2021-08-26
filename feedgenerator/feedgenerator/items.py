# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Join, MapCompose, TakeFirst
from w3lib.html import remove_tags
import re
import HTMLParser

Feed2JobsMap = {
    'job_external_id': 'external_unique_id',
    'url': 'url',
    'title': 'title',
    'city': 'city',
    'state': 'state',
    'country': 'country',
    'description': 'description',
    'job_type': 'job_type',
    'company': 'company',
    'category': 'category',
    'posted_at': 'posted_at',
    'expire_date': 'expire_date',
    'logo': 'logo'
}


def str_strip(string):
    string = string.replace('\\', '\\\\')
    string = string.replace('\n', '\\n')
    string = string.replace('\t', '\\t')
    string = string.replace('\r', '')
    return string

def unescapeHtml(string):
    regexp = "&.+?;"
    list_of_html = re.findall(regexp, string) #finds all html entites in page
    for e in list_of_html:
        h = HTMLParser.HTMLParser()
        unescaped = h.unescape(e) #finds the unescaped value of the html entity
        string = string.replace(e, unescaped)
    return unicode(string)

def first32(string):
    return string[0:32]
def first128(string):
    return string[0:128]

def first512(string):
    return string[0:512]

def first2048(string):
    return string[0:2048]

def first8192(string):
    return string[0:8192]

class FeedgeneratorItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass



class JobItem(scrapy.Item):
    id = scrapy.Field()
    uuid = scrapy.Field()
    uid = scrapy.Field()
    external_id = scrapy.Field()
    external_uid = scrapy.Field()
    employer_id = scrapy.Field()
    job_group_id = scrapy.Field()
    url = scrapy.Field()
    link = scrapy.Field()
    title = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    country = scrapy.Field()
    description = scrapy.Field()
    job_type = scrapy.Field()
    jobtype = scrapy.Field()
    experience = scrapy.Field()
    company = scrapy.Field()
    category = scrapy.Field()
    posted_at = scrapy.Field()
    expire_at = scrapy.Field()
    status = scrapy.Field()
    budget = scrapy.Field()
    budget_spent = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
    referencenumber = scrapy.Field()
    date = scrapy.Field()
    content = scrapy.Field()
    publishDate = scrapy.Field()
    location = scrapy.Field()
    cpc = scrapy.Field()
    logo = scrapy.Field()
    contract_length = scrapy.Field()
    contract_type = scrapy.Field()
    tag = scrapy.Field()
    region1 = scrapy.Field()
    region2 = scrapy.Field()
    salary = scrapy.Field()
    job_id = scrapy.Field()


class JobInTreeItem(scrapy.Item):
    ANNOUNCER = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first128), output_processor=TakeFirst())
    RECRUITER = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first128), output_processor=TakeFirst())
    MAXCV = scrapy.Field(output_processor=TakeFirst())
    CONTRACT = scrapy.Field(input_processor=MapCompose(first32), output_processor=TakeFirst())
    JOBSTATUS = scrapy.Field(input_processor=MapCompose(first32), output_processor=TakeFirst())
    EXPERIENCE = scrapy.Field(input_processor=MapCompose(first32), output_processor=TakeFirst())
    PAY = scrapy.Field(input_processor=MapCompose(first32), output_processor=TakeFirst())
    AVAILABILITY = scrapy.Field(output_processor=TakeFirst())
    CONTACT = scrapy.Field(input_processor=MapCompose(first512), output_processor=TakeFirst())
    COUNTRY = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    REGION = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    DEPARTMENT = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    CITY = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    SECTOR = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    FUNCTION = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    REFERENCE = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    TITLE = scrapy.Field(input_processor=MapCompose(first128), output_processor=TakeFirst())
    LINK = scrapy.Field(input_processor=MapCompose(first2048), output_processor=TakeFirst())
    DESCJOB = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first8192, unescapeHtml, output_processor=TakeFirst()))
    DESCCOMPANY = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first8192), output_processor=TakeFirst())
    DESCPROFIL = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first8192), output_processor=TakeFirst())
    DESCINFO = scrapy.Field(input_processor=MapCompose(remove_tags, str_strip, first8192), output_processor=TakeFirst())


contract_types = {'fr':
                      {1:'CDI', 2:'CDD', 3:'Stage', 4:'Freelance', 5:'Autres', 6 :'Alternance'},
                  'en':
                      {1:'Full-time', 2:'Part-time', 3:'Internship', 4:'Freelance', 5:'Other', 6:'Apprenticeship'}
                  }

jobijoba_contract_types = {1: 'rolling_contract', 2: 'fixed_term_contract', 3: 'internship', 4: 'independent', 5: 'interim', 6: 'internship'}

jobijoba_contract_length = {1: 'full_time', 2: 'part_time'}







