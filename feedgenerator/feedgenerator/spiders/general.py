# -*- coding: utf-8 -*-
import scrapy
from feedgenerator.items import JobItem
from feedgenerator.extensions import PgSQLStoreFeedExport
from feedgenerator.extensions import Geoname
from scrapy.loader import ItemLoader
from feedgenerator.items import Feed2JobsMap as fldMap
from feedgenerator.items import jobijoba_contract_types
from feedgenerator.items import jobijoba_contract_length
from pprint import pprint


class GeneralSpider(scrapy.Spider):

    name = 'general'
    publisher = 'myXtramile network'
    publisherurl = 'http://myxtramile.com/'
    facebookBoardIds = []

    def __init__(self, board_id=None, board_name=None, default=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.store = PgSQLStoreFeedExport()
        self.geoname = Geoname()
        if board_id is not None:
            self.board = self.store.getBoardById(board_id)
            try:
                self.board = self.store.getBoardById(board_id)
            except Exception, ex:
                raise Exception('Can\'t get Board for board_id=%s' % str(board_id))
        elif board_name is not None:
            try:
                self.board = self.store.getBoardByName(board_name)
            except Exception, ex:
                raise Exception('Can\'t get Board for board_name=%s' % str(board_name))
        elif default is not None:
            self.board = None
        else:
            raise Exception('At least one of parameters: \'board-id\' or \'board_name\' or \'default\' must being')

        if self.board is None:
            self.logger.info('Board is None! Will create default feed...')
            self.feed = 'default.xml'
            self.employer_ids = self.store.getEmployerIds()
            self.board_feed_settings = None
            self.item = 'job'
            self.root = 'source'
            self.date_format = '%Y-%m-%d'
        else:
            self.feed = self.board['feed']
            self.employer_ids = self.store.getEmployerIds(self.board['id'])
            self.board_feed_settings = self.store.getBoardFeedSetting(self.board['id'])
            if 'date_format' in self.board_feed_settings:
                self.date_format = self.board_feed_settings['date_format']
            else:
                self.date_format = '%Y-%m-%d'
            self.item = self.board_feed_settings['job']
            self.root = self.board_feed_settings['root']
            self.facebookBoardIds = self.store.getFacebookBoardIds()
            if self.board_feed_settings is None:
                raise Exception('"board_feed_settings" not exists for board with id %i' % self.board['id'])
                exit()

        try:
            self.start_urls = ['http://localhost']
        except Exception, ex:
            self.start_urls = ['http://localhost:6800']

    def parse(self, response):
        jobs = self.store.getJobs(self.board['id'])
        runFlag = self.store.checkHistory(jobs, self.board['id'])
        # temporarily disable check of last run
        runFlag = True
        if runFlag == False:
            self.logger.info('Generation of the feed is not required!')
            raise Exception('Generation of the feed is not required!')
        for job in jobs:
            if self.is_demo_job(job):
                continue
            if job['employer_id'] == 99 and self.board['id'] in [31, 45]:
                continue
            Item = JobItem()
            if self.board_feed_settings is not None:
                for key, value in self.board_feed_settings.items():
                    if key in ['id', 'job_board_id', 'root', 'job', 'cpc', 'created_at', 'updated_at', 'date_format',
                               'separate_by_country', 'allowed_countries', 'required_fields', 'url']:
                        continue
                    if value is not None and type(value) is str and len(value.strip()) > 0:
                        if value not in Item.fields:
                            Item.fields[value] = scrapy.Field()
                        try:
                            if (value in Item) and (Item[value] is not None) and (job[fldMap[key]] is not None):
                                Item[value] = ', '.join([Item[value], job[fldMap[key]]])
                            elif (value in Item) and (Item[value] is not None):
                                pass
                            else:
                                Item[value] = job[fldMap[key]]
                        except Exception, ex:
                            Item[value] = None
                Item['cpc'] = self.store.getCpc(job,self.board['id'])
                Item[self.board_feed_settings['url']] = 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], self.board['uuid'])
                #Item['url'] = 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], self.board['uuid'])

                #if job['employer_id'] == 99 and self.board['id'] == 45:
                #    Item[self.board_feed_settings['url']] = 'https://pixel.xtramile.io/t/450986ab-cccf-48df-9ec4-fa04fed82da1?s=2b56e0a2-b604-408e-a6db-fa35cae8fca7'
                #if job['employer_id'] == 99 and self.board['id'] == 31:
                #    Item[self.board_feed_settings['url']] = 'https://pixel.xtramile.io/t/450986ab-cccf-48df-9ec4-fa04fed82da1?s=ee18161d-f701-4acf-9c16-78109b8b7661'

                if self.board['name'] in ['DensouTest', 'Neuvoo', 'NominalTechnoTest']:
                    Item['logo'] = self.store.getEmployerLogo(job)
                if self.board['name'] in ['Jobrapido']:
                    if Item['country'] != 'FR':
                        Item['country'] = 'FR'
                        Item['location'] = 'Abroad'
                if self.board['name'] in ['Jobijoba']:
                    if 'jobType' in job['attributes']:
                        Item['contract_type'] = jobijoba_contract_types.get(int(job['attributes']['jobType']), None)
                    country_info = self.geoname.isocode2countryinfo(Item['country_name'])
                    if country_info is not None and 'Country' in country_info:
                        Item['country_name'] = country_info['Country']
                    else:
                        Item['country_name'] = None
                    if 'jobType' in job['attributes']:
                        Item['contract_length'] =jobijoba_contract_length.get(int(job['attributes']['jobType']), None)
                    if len(job['keywords']) > 0:
                        Item['tag'] = ','.join(job['keywords'])
                    location = self.geoname.city2location(job['city'])
                    if location is not None:
                        Item['region1'] = location['subdiv1']['name']
                        Item['region2'] = location['subdiv2']['name']
                if self.board['id'] in [45, 58, 59]:
                    Item['url'] = job['url']
                    if 'salary' in job['attributes']:
                        Item['salary'] = job['attributes']['salary']
                if self.board['id'] in self.facebookBoardIds:
                    Item['job_id'] = job['id']
                if job['employer_id'] in [104]:
                    if job['city'] is not None:
                        Item['city'] = job['city']
                    else:
                        Item['city'] = 'München'
            else:
                for key, value in job.items():
                    if key in ['employer_id', 'expire_date', 'uid', 'external_id', 'job_group_id']:
                        continue
                    Item[key] = value
            yield Item


    def is_demo_job(self, job):
        if job['employer_id'] in [31]:
            return True
        if 'demo' in job['title'].lower().split(' '):
            return True
        if 'demo' in job['title'].lower().split('-'):
            return True
        return False



