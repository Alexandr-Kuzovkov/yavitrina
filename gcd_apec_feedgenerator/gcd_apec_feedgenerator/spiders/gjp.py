# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import scrapy
import logging
from gcd_apec_feedgenerator.extensions import PgSQLStoreGcd
from pprint import pprint
import pkgutil
import sys
from gcd_apec_feedgenerator.google_api import GoogleIndexAPI
import tempfile
import datetime
import time

#https://developers.google.com/search/docs/data-types/job-posting

class GjpSpider(scrapy.Spider):
    name = 'gjp'
    allowed_domains = ['localhost']
    # Configuration file
    JOB_BOARD_ID = 168
    # Google's API Variables
    SCOPE = 'https://www.googleapis.com/auth/jobs'
    d = os.path.dirname(sys.modules['gcd_apec_feedgenerator'].__file__)
    credential_data = pkgutil.get_data('gcd_apec_feedgenerator', 'res/index_api_credentials.json')
    tf = tempfile.NamedTemporaryFile()
    tf.write(credential_data)
    tf.read()
    CREDENTIAL_FILE = tf.name
    service = GoogleIndexAPI(credential_file=CREDENTIAL_FILE)
    store = PgSQLStoreGcd()
    employers = {}
    created_jobs = []
    updated_jobs = []
    logger = logging.getLogger(__name__)
    force_job_groups = None

    def __init__(self, board_id=None, port=None, force=False, force_job_groups=None, debug=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.debug = debug
        self.port = port
        if board_id is not None:
            self.JOB_BOARD_ID = int(board_id)
        if force_job_groups is not None:
            self.force_job_groups = map(lambda i: int(i.strip()), force_job_groups.split(';'))

    def start_requests(self):
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
        self.logger.info('Force job_groups: %s' % str(self.force_job_groups))
        self.logger.info('Refreshing materialized view...')
        self.store.refresh_view()
        self.logger.info('Job board ID=%i' % self.JOB_BOARD_ID)
        self.logger.info('Fetching jobs…')
        job_board = self.store.get_job_board(self.JOB_BOARD_ID)
        if self.force_job_groups is not None:
            count_jobs = self.store.count_jobs_in_groups(self.force_job_groups)
        else:
            count_jobs = self.store.count_jobs(self.JOB_BOARD_ID)
        self.logger.info('%i jobs found…' % count_jobs)
        job_per_once = self.settings.get('JOBS_PER_ONCE', 500)
        offsets = range(0, count_jobs, job_per_once)
        self.logger.info('Fetching employers…')
        employers = self.store.getEmployers()
        self.logger.info('Fetched %s employers…' % len(employers))
        for employer in employers:
            self.employers[employer['id']] = employer
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
            if self.force_job_groups is not None:
                jobs = self.store.get_jobs_in_groups(self.force_job_groups, offset, job_per_once)
            else:
                jobs = self.store.get_jobs(self.JOB_BOARD_ID, offset, job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))
            #pprint(jobs)
            #creating/updating jobs
            attributes = {}
            for job in jobs:
                employer = self.employers[job['employer_id']]
                res = self.update_job_url(job)
                if res is not None and 'error' not in res:
                    job['attributes']['google_index_api'] = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time())))
                    attributes[job['id']] = job['attributes']
            #self.logger.info('Update URL:')
            #pprint(attributes)
            self.store.update_jobs_attributes(attributes)

        #deleting jobs
        if self.force_job_groups is None:
            count_jobs = self.store.count_jobs_for_remove(self.JOB_BOARD_ID)
            offsets = range(0, count_jobs, job_per_once)
            for offset in offsets:
                self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
                jobs = self.store.get_jobs_for_remove(self.JOB_BOARD_ID, offset, job_per_once)
                self.logger.info('...fetched %i jobs' % len(jobs))
                attributes = {}
                for job in jobs:
                    employer = self.employers[job['employer_id']]
                    res = self.remove_job_url(job)
                    if res is not None and 'error' not in res:
                        if 'google_index_api' in job['attributes']:
                            del job['attributes']['google_index_api']
                            attributes[job['id']] = job['attributes']
                #self.logger.info('Remove URL:')
                #pprint(attributes)
                self.store.update_jobs_attributes(attributes)

    def update_job_url(self, job):
        res = self.service.get_notification_status(job['url'])
        #update URL if not exists
        if 'error' in res and 'status' in res['error']:
            if res['error']['status'] == 'NOT_FOUND':
                return self.service.update_url(job['url'])
        #update URL if removed
        latestRemove = None
        latestUpdate = False
        if 'latestRemove' in res and 'notifyTime' in res['latestRemove']:
            latestRemove = self.str2datetime(res['latestRemove']['notifyTime'])
        if 'latestUpdate' in res and 'notifyTime' in res['latestUpdate']:
            latestUpdate = self.str2datetime(res['latestUpdate']['notifyTime'])
        if latestRemove is not None and latestUpdate is not None:
            if latestRemove > latestUpdate:
                return self.service.update_url(job['url'])
        if latestRemove is not None and latestUpdate is None:
            return self.service.update_url(job['url'])
        #update URL if job was updated after posting
        if latestRemove is not None and latestUpdate is not None:
            if latestRemove < latestUpdate:
                if latestUpdate < job['updated_at']:
                    return self.service.update_url(job['url'])
        return None

    def remove_job_url(self, job):
        res = self.service.get_notification_status(job['url'])
        #if URL not exists
        if 'error' in res and 'status' in res['error']:
            if res['error']['status'] == 'NOT_FOUND':
                return None
        #If URL was removed
        latestRemove = None
        latestUpdate = False
        if 'latestRemove' in res and 'notifyTime' in res['latestRemove']:
            latestRemove = self.str2datetime(res['latestRemove']['notifyTime'])
        if 'latestUpdate' in res and 'notifyTime' in res['latestUpdate']:
            latestUpdate = self.str2datetime(res['latestUpdate']['notifyTime'])
        if latestRemove is not None and latestUpdate is not None:
            if latestRemove > latestUpdate:
                return None
        if latestRemove is not None and latestUpdate is None:
            return None
        return self.service.remove_url(job['url'])

    def str2datetime(self, timestr):
        d = map(lambda i: int(i), timestr.split('T')[0].split('-'))
        t = map(lambda i: int(i), timestr.split('T')[1].split('.')[0].split(':'))
        return datetime.datetime(d[0], d[1], d[2], t[0], t[1], t[2], 0)


    def mydebug(self):
        self.logger.info('Debug...')
        #url = 'https://demo-company.jobs.xtramile.tech/fi-g-mna-analyst-(hf)-qttr-10'
        '''
        res = self.service.update_url(url)
        pprint(res)
        time.sleep(5)
        res = self.service.remove_url(url)
        pprint(res)
        time.sleep(10)
        '''
        '''
        count_jobs = self.store.count_jobs(self.JOB_BOARD_ID)
        job_per_once = self.settings.get('JOBS_PER_ONCE', 500)
        offsets = range(0, count_jobs, job_per_once)
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
            jobs = self.store.get_jobs(self.JOB_BOARD_ID, offset, job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))
            attributes = {}
            for job in jobs:
                res = self.service.remove_url(job['url'])
                pprint(res)
            time.sleep(10)
            #for job in jobs:
            #    res = self.service.get_notification_status(job['url'])
            #    pprint(res)

        '''
        urls = ['https://jobstoday.jobs.xtramile.tech/waitress-56', 'https://jobs.xtramile.tech/?company=jobstoday&job=head-chef-27']
        for url in urls:
            res = self.service.update_url(url)
            time.sleep(1)
            pprint(res)





