# -*- coding: utf-8 -*-
import scrapy
from feedgenerator.items import JobInTreeItem
from feedgenerator.extensions import PgSQLStoreFeedExport
from scrapy.loader import ItemLoader
from feedgenerator.extensions import Geoname


class JobInTreeSpider(scrapy.Spider):

    name = 'jobintree'
    publisher = 'JOBINTREE'
    publisherurl = 'http://myxtramile.com/'

    def __init__(self, board_id=None, board_name=None, default=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.store = PgSQLStoreFeedExport()
        self.geoname = Geoname('FR')
        if board_id is not None:
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
            raise Exception('"board_id" is required paremeter!')
            exit()
        else:
            self.feed = self.board['feed']
            self.employer_ids = self.store.getEmployerIds(self.board['id'])
            self.date_format = '%d/%m/%Y'
        #self.start_urls = ['http://localhost']
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
            l = ItemLoader(item=JobInTreeItem())
            l.add_value('ANNOUNCER', 'XTRAMILE SAS')
            l.add_value('RECRUITER', job['company'])
            l.add_value('MAXCV', 0)
            if job['job_type'] is not None:
                l.add_value('CONTRACT', job['job_type'])
            else:
                l.add_value('CONTRACT', 'indifférent')
            #l.add_value('CONTRACT', 'indifférent')
            l.add_value('JOBSTATUS', None)
            l.add_value('EXPERIENCE', None)
            l.add_value('PAY', None)
            #l.add_value('AVAILABILITY', job['posted_at'])
            #l.add_value('AVAILABILITY', job['created_at'])
            l.add_value('CONTACT', None)
            l.add_value('COUNTRY', job['country'])
            if job['employer_id'] in [104]:
                if job['city'] is not None:
                    l.add_value('CITY', job['city'])
                else:
                    l.add_value('CITY', 'München')
            else:
                l.add_value('CITY', job['city'])
            location = self.geoname.city2location(job['city'])
            if location is not None:
                l.add_value('REGION', location['subdiv1']['name'])
                l.add_value('DEPARTMENT', location['subdiv2']['name'])
            else:
                l.add_value('REGION', None)
                l.add_value('DEPARTMENT', None)
            if job['category'] is not None:
                l.add_value('SECTOR', job['category'])
            else:
                l.add_value('SECTOR', None)
            l.add_value('FUNCTION', None)
            l.add_value('REFERENCE', job['external_id'])
            l.add_value('TITLE', job['title'])
            l.add_value('LINK', 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], self.board['uuid']))
            l.add_value('DESCJOB', job['description'])
            l.add_value('DESCCOMPANY', None)
            l.add_value('DESCPROFIL', None)
            l.add_value('DESCINFO', None)
            #l.add_value('cpc', self.store.getCpc(job['employer_id'], self.board['id']))

            yield l.load_item()



