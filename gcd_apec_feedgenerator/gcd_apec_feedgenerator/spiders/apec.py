# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import scrapy
import logging
from gcd_apec_feedgenerator.extensions import PgSQLStoreGcd
from gcd_apec_feedgenerator.items import *
from pprint import pprint
import pkgutil
import sys
import tempfile
import time
import pkgutil
from gcd_apec_feedgenerator import apecapi
from gcd_apec_feedgenerator.extensions import Geocode


class ApecSpider(scrapy.Spider):
    name = 'apec'
    allowed_domains = ['localhost']
    JOB_BOARD_ID = 169
    logger = logging.getLogger(__name__)
    store = PgSQLStoreGcd()
    geocode = Geocode()
    employers = {}
    service = None
    naf_codes = None
    new_published = []

    def __init__(self, board_id=None, port=None, show_urls=None, debug=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.debug = debug
        self.port = port
        self.show_urls = show_urls
        if board_id is not None:
            self.JOB_BOARD_ID = int(board_id)
        self.load_naf_codes()
        self.email_template = pkgutil.get_data('gcd_apec_feedgenerator', 'res/new_job_apec_elodie_email.html')

    def start_requests(self):
        self.service = apecapi.ApecAPI(self)
        if self.debug is not None:
            self.logger.warning('Only debug method will be run!')
            self.mydebug()
            return
        url = 'http://localhost'
        if self.port is not None:
            url = ':'.join([url, str(self.port)])
        request = scrapy.Request(url, callback=self.parse)
        yield request

    def parse(self, response):
        self.logger.info('Refreshing materialized view...')
        self.store.refresh_view()
        self.logger.info('Job board ID=%i' % self.JOB_BOARD_ID)
        self.logger.info('Fetching APEC positions statuses…')
        statuses, stat, urls = self.get_offer_statuses()
        self.logger.info('Statuses of %i APEC positions fetched' % len(statuses))
        self.logger.info('Positions statistic by statuses: %s' % str(stat))
        self.logger.info('Fetching employers…')
        employers = self.store.getEmployers()
        self.logger.info('Fetched %s employers…' % len(employers))
        for employer in employers:
            self.employers[employer['id']] = employer
        job_board = self.store.get_job_board(self.JOB_BOARD_ID)
        self.logger.info('Fetching jobs…')
        count_jobs = self.store.count_jobs(self.JOB_BOARD_ID)
        job_per_once = self.settings.get('JOBS_PER_ONCE', 500)
        offsets = range(0, count_jobs, job_per_once)
        job_ids = []
        self.logger.info('Creating/updating jobs...')
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
            jobs = self.store.get_jobs(self.JOB_BOARD_ID, offset, job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))
            #pprint(jobs)
            #creating/updating jobs
            for job in jobs:
                job_ids.append(str(job['id']))
                employer = self.employers[job['employer_id']]
                if str(job['id']) not in statuses.keys():
                    res = self.create_position(job, employer)
                    if 'error' in res:
                        self.logger.error(res['error'])
                else:
                    if statuses[str(job['id'])] == 'SUSPENDUE':
                        res = self.change_position_status(str(job['id']), 'PUBLIEE')
                        if 'error' in res:
                            self.logger.error(res['error'])
                    else:
                        self.logger.info('Can\'t enable position for job id=%i because it status is "%s"' % (job['id'], statuses[str(job['id'])]))

        #deleting jobs
        self.logger.info('Deleting jobs...')
        for id, status in statuses.items():
            if id not in job_ids and status in ['PUBLIEE']:
                res = self.change_position_status(id, 'SUSPENDUE')
                if 'error' in res:
                    self.logger.error(res['error'])
        #fetch new published jobs
        self.new_published = self.get_new_published(urls)
        if self.show_urls is not None:
            self.logger.info('Published jobs:')
            for item in self.new_published:
                self.logger.info('%s: %s' % (item['id'], item['url']))
            if len(self.new_published) == 0:
                self.logger.info('Nothing is published')
        self.logger.info('Done')

    def load_naf_codes(self):
        content = pkgutil.get_data('gcd_apec_feedgenerator', 'res/naf.code.csv')
        codes = filter(lambda k: len(k) >=3 and 'Code' not in k[0],
                       map(lambda i: map(lambda j: j.strip(), i.split('\t')), content.split('\n')))
        self.naf_codes = codes

    def get_offer_statuses(self):
        res = self.service.get_list_recruiter_position_openings()
        statuses = {}
        stat = {}
        urls = {}
        for row in res:
            id = row['staffingOrder']['OrderId']['IdValue'][0]['_value_1']
            res2 = self.service.get_position_status(id)
            if type(res2) is list and len(res2) > 0 and 'status' in res2[0]:
                status = res2[0]['status']
                statuses[id] = status
                if status in stat:
                    stat[status] += 1
                else:
                    stat[status] = 1
                if status in ['PUBLIEE']:
                    urls[id] = res2[0]['positionUrl']
        return statuses, stat, urls


    def create_position(self, job, employer):
        self.logger.info('openPosition for job id="%s"' % str(job['id']))
        res = self.service.open_position(job, employer)
        if ('error' in res):
            self.logger.debug(str(res))
        self.output_errors(self.get_exceptions_messages(res))
        return res

    def change_position_status(self, id, status):
        valid_statuses = ['SUSPENDUE', 'AVALIDER', 'FERMEE', 'AMODIFIER', 'PUBLIEE']
        if status not in valid_statuses:
            raise Exception('Status must be in %s' % str(valid_statuses))
        self.logger.debug('updatePositionStatus for id="%s"' % id)
        res = self.service.update_position_status(id, status)
        if ('error' in res):
            self.logger.debug(str(res))
        self.output_errors(self.get_exceptions_messages(res))
        return res

    def get_exceptions_messages(self, res):
        messages = []
        try:
            if res and 'PayloadDisposition' in res:
                if res['PayloadDisposition'] and 'EntityDisposition' in res['PayloadDisposition'] and type(res['PayloadDisposition']['EntityDisposition']) is list:
                    for item in res['PayloadDisposition']['EntityDisposition']:
                        if item and 'EntityException' in item:
                            if item['EntityException'] and 'Exception' in item['EntityException'] and type(item['EntityException']['Exception']) is list:
                                for item2 in item['EntityException']['Exception']:
                                    if item2 and 'ExceptionMessage' in item2:
                                        messages.append(item2['ExceptionMessage'])
        except Exception, ex:
            self.logger.error(ex.message)
        return messages

    def output_errors(self, messages):
        if type(messages) is list and len(messages) > 0:
            for message in messages:
                self.logger.error(message)

    def get_new_published(self, urls):
        new_published = []
        statuses, stat, urls_latest = self.get_offer_statuses()
        for id in urls_latest.keys():
            if id not in urls.keys():
                new_published.append({'id': id, 'url': urls_latest[id]})
        return new_published


    def mydebug(self):
        self.logger.info('Debug...')
        #employers = self.store.getEmployers()
        #self.logger.info('Fetched %s employers…' % len(employers))
        #for employer in employers:
        #    self.employers[employer['id']] = employer
        ''''''
        #get_list_recruiter_position_openings
        res = self.service.get_list_recruiter_position_openings(False)
        #pprint(res)
        #return
        #pprint(res[0])
        positions = []
        statuses = {}
        for row in res:
            id = row['staffingOrder']['OrderId']['IdValue'][0]['_value_1']
            if id:
                positions.append(row['staffingOrder']['OrderId']['IdValue'][0]['_value_1'])
            #else:
            #    pprint(row)
        pprint(positions)
        pprint(len(positions))
        for id in positions:
            res = self.service.get_position_status(id)
            pprint(res)
            if type(res) is list and len(res) > 0 and 'status' in res[0]:
                statuses[id] = res[0]['status']
        pprint(statuses)

        #pprint('-'*60 + 'Request XML:')
        #xml = self.service.get_list_recruiter_position_openings(return_raw_request=True)
        #pprint(xml)

        #get_position_status
        #res = self.service.get_position_status('15084/6')
        #pprint(res)
        #pprint('-'*60 + 'Request XML:')
        #xml = self.service.get_position_status('1924/214422093', return_raw_request=True)
        #open('out3.xml', 'w').write(xml)
        #pprint(xml)

        '''
        #update_position_status ('SUSPENDUE', 'AVALIDER', 'FERMEE', 'AMODIFIER')
        res = self.service.get_position_status('2498/206448577')
        pprint(res)
        time.sleep(2)
        res = self.service.update_position_status('2498/206448577', 'FERMEE')
        pprint(res)
        pprint('-'*60 + 'Request XML:')
        xml = self.service.update_position_status('2498/206448577','FERMEE', return_raw_request=True)
        pprint(xml)
        time.sleep(2)
        res = self.service.get_position_status('2498/206448577')
        pprint(res)
        '''
        '''
        #get statuses of all positions
        statuses = {}
        res = self.service.get_list_recruiter_position_openings()
        for pos in res:
            time.sleep(2)
            status = self.service.get_position_status(pos['staffingOrder']['OrderId']['IdValue'][0]['_value_1'])
            pprint(status)
            statuses[pos['staffingOrder']['OrderId']['IdValue'][0]['_value_1']] = status
        open('statuses.txt', 'w').write(str(statuses))
        '''
        '''
        jobs = self.store.get_jobs(self.JOB_BOARD_ID, 0, 2)
        job = jobs[0]
        job['id'] = str(job['id']) + '/8'
        pprint(job['id'])
        res = self.service.open_position(job, employers[job['employer_id']])
        #pprint(jobs[0])
        #xml = self.service.open_position(jobs[0], return_raw_request=True)
        #open('out2.xml', 'w').write(xml)
        pprint(res)
        time.sleep(3)
        res = self.service.get_position_status(job['id'])
        pprint(res)

        code = self.geocode.city2insee_code('Metz')
        pprint(code)

        statuses, stat, urls = self.get_offer_statuses()
        for id, url in urls.items():
            self.new_published.append({'id': id, 'url': url})
        pprint(self.new_published)
        '''