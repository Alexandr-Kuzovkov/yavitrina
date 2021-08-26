# -*- coding: utf-8 -*-
from scrapy.spiders import XMLFeedSpider
from jobimporters.extensions import PgSQLStoreImport
from jobimporters.items import JobItem
import uuid
from jobimporters.items import feed2itemMap
from jobimporters.items import GeneralCategoryMap
from jobimporters.items import germanpersonalCategories
from jobimporters.extensions import Geocode
import scrapy
from pprint import pprint


class GermanPersonalSpider(XMLFeedSpider):

    name = 'germanpersonal'
    handle_httpstatus_list = [404, 400, 500, 413]
    drain = False
    categories = {}
    category_codes = {}
    message_lines = []
    slugs = {}

    def __init__(self, employer_id=None, job_group=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.employer_id = employer_id
        self.job_group = job_group
        self.store = PgSQLStoreImport()
        self.iterator = 'iternodes'  # you can change this; see the docs
        try:
            self.employer = self.store.getEmployerById(employer_id)
        except Exception, ex:
            raise Exception('Can\'t get employer data for "%s"!' % employer_id)
            exit()
        self.start_urls = self.employer['feed_url'].split(',')
        self.employer_feed_settings = self.store.getEmployerFeedSetting(self.employer['id'])
        if self.employer_feed_settings is None:
            raise Exception('"employer_feed_settings" not exists for employer with id %i' % self.employer['id'])
            exit()
        self.itertag = self.employer_feed_settings['job'].strip()
        self.slugs = self.store.getJobsSlugs(self.employer_id)
        self.companySlug = self.store.getCompanySlug(self.employer_id)
        self.geocode = Geocode()
        if 'drain' in kwargs:
            if kwargs['drain'].lower() == 'true':
                self.drain = True

    def parse_node(self, response, node):
        Item = JobItem()
        drop_list = ['id', 'employer_id', 'root', 'job', 'created_at', 'updated_at']
        if self.employer_feed_settings['company'] is None:
            drop_list.append('company')
        #pprint(self.employer_feed_settings)
        for key, value in self.employer_feed_settings.items():
            if key in drop_list:
                continue
            if value is not None and type(value) is str and len(value.strip()) > 0:
                selector = node.xpath(''.join([value, '//*|', value, '/text()']))
                if (selector is not None) and (len(selector) > 0):
                    Item[feed2itemMap[key]] = selector[0].extract().strip()
                else:
                    Item[feed2itemMap[key]] = None
            else:
                Item[feed2itemMap[key]] = None

        Item['company'] = node.xpath('//PositionRecordInfo/id')[0].xpath('@idOwner')[0].extract()
        #Item['external_id'] = node.xpath('//PositionPostings/PositionPosting/id/idValue[@name="StaId"]/text()')[0].extract()
        #take external_id from url, instead xml field

        Item['external_id'] = Item['url'].split('/').pop().split('-')[1]
        try:
            Item['state'] = node.xpath('//PositionProfile/PositionDetail/PhysicalLocation/Area[@type="region"]/Value/text()')[0].extract()
        except Exception:
            Item['state'] = node.xpath('//PositionProfile/PositionDetail/PhysicalLocation/Area[@type="municipality"]/Value/text()')[0].extract()
        Item['country'] = node.xpath('//PositionProfile/PositionDetail/PhysicalLocation/Area[@type="countrycode"]/Value/text()')[0].extract()
        Item['description'] = self.extractDescription(node)
        try:
            Item['job_type'] = node.xpath('//PositionProfile/PositionDetail/PositionSchedule/text()')[0].extract()
        except Exception:
            Item['job_type'] = None
        category_code = node.xpath('//PositionProfile/PositionDetail/JobCategory/JobCategory/CategoryCode/text()')[0].extract()
        category_desc = node.xpath('//PositionProfile/PositionDetail/JobCategory/JobCategory/CategoryDescription/text()')[0].extract()
        Item['category'] = category_desc
        Item['posted_at'] = node.xpath('//PositionPostings/PositionPosting/id/LastModificationDate/text()')[0].extract()
        Item['posted_at'] = self.formatDate(Item['posted_at'])
        Item['city'] = node.xpath('//PositionProfile/PositionDetail/PhysicalLocation/Area[@type="municipality"]/Value/text()')[0].extract()
        Item['uid'] = str(uuid.uuid1())
        Item['employer_id'] = self.employer['id']
        Item['external_unique_id'] = '_'.join([str(Item['external_id']), str(self.employer['id'])]).strip().replace(' ', '').replace('\n', '').replace('\t', '')
        Item['status'] = self.employer['default_job_status']
        if 'company' not in Item or Item['company'] is None:
            Item['company'] = self.employer['name']
        Item['budget'] = self.employer['default_job_budget']
        Item['budget_spent'] = 0.0
        Item['company_slug'] = self.companySlug
        if Item['title'] is None:
            Item['title'] = ' '.join(Item['description'].split(' ')[0:2]).strip()
        slug = self.store.getJobSlug(Item['title'])
        count = 1
        currslug = slug
        while self.slugBusy(Item, currslug):
            currslug = '-'.join([slug, str(count)])
            count = count + 1
        Item['slug'] = currslug
        self.slugs[Item['slug']] = Item['external_unique_id']
        attributes = {"cv": False, "lang": "en", "phone": False, "ext_url": "", "jobolizer": ""}
        countryCode = None
        if 'country' in Item and Item['country'] is not None:
            if Item['country'].upper().strip() in self.geocode.iso2:
                countryCode = Item['country'].upper().strip()
            else:
                countryCode = self.geocode.country2iso(Item['country'])
            if countryCode is not None:
                countryName = self.geocode.isocode2countryinfo(countryCode)['Country']
                Item['country'] = countryName
            else:
                countryName = Item['country']
            attributes['selectedCountry'] = {"countryCode": countryCode, "countryName": countryName}
        self.countCategory(Item['category'], category_code)
        if int(category_code) in germanpersonalCategories:
            attributes['category'] = germanpersonalCategories[int(category_code)][1]
        else:
            attributes['category'] = 30
        Item['attributes'] = attributes
        job_group_id = self.store.create_job_group(self.employer_id, Item['category'], attributes['category'], countryCode)
        if job_group_id is not None:
            Item['job_group_id'] = job_group_id
        else:
            Item['job_group_id'] = self.job_group

        return Item

    def slugBusy(self, item, slug):
        if slug in self.slugs and self.slugs[slug] != item['external_unique_id']:
            return True
        return False

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

    def formatDate(self, datestr):
        try:
            day, month, year = datestr.replace('[', '').split('-')
        except Exception, ex:
            day, month, year = datestr.replace('[', '').split('.')
        return '-'.join([year, month, day])

    def extractDescription(self, node):
        sections = node.xpath('//PositionProfile/FormattedPositionDescription')
        res = []
        for section in sections:
            try:
                name = section.xpath('Name/text()')[0].extract()
                text = ''.join(section.xpath('Value//*|').extract())
                text = section.xpath('Value/text()')[0].extract()
                res.append(name)
                res.append(text)
            except Exception, ex:
                pass
        return ' '.join(res)

