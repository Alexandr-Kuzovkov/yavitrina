# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import scrapy
import logging
from gcd_apec_feedgenerator.extensions import PgSQLStoreGcd
from pprint import pprint
import pkgutil
import sys
from gcd_apec_feedgenerator.google_api import GoogleGloudJobDiscovery
import tempfile

class GcdSpider(scrapy.Spider):
    name = 'gcd'
    allowed_domains = ['localhost']
    # Configuration file
    JOB_BOARD_ID = 168
    # Google's API Variables
    SCOPE = 'https://www.googleapis.com/auth/jobs'
    d = os.path.dirname(sys.modules['gcd_apec_feedgenerator'].__file__)
    #CREDENTIAL_FILE = os.path.join(d, 'res/cloudjobs_creds.json')
    credential_data = pkgutil.get_data('gcd_apec_feedgenerator', 'res/cloudjobs_creds.json')
    tf = tempfile.NamedTemporaryFile()
    tf.write(credential_data)
    tf.read()
    CREDENTIAL_FILE = tf.name
    DISCOVERY_JSON_FILE = pkgutil.get_data('gcd_apec_feedgenerator', 'res/cloudjobs_discovery_file.json')
    #job_service = GoogleGloudJobDiscovery(scope=SCOPE, credential_file=CREDENTIAL_FILE, discovery_json_file=DISCOVERY_JSON_FILE)
    job_service = None
    store = PgSQLStoreGcd()
    employers = {}
    created_jobs = []
    updated_jobs = []
    logger = logging.getLogger(__name__)

    def __init__(self, board_id=None, force=False, debug=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.debug = debug
        if board_id is not None:
            self.JOB_BOARD_ID = int(board_id)

    def start_requests(self):
        if self.debug is not None:
            self.logger.warning('Only debug method will be run!')
            self.mydebug()
            return
        url = 'http://localhost'
        request = scrapy.Request(url, callback=self.parse)
        yield request

    def parse(self, response):
        self.logger.info('Refreshing materialized view...')
        self.store.refresh_view()
        self.logger.info('Job board ID=%i' % self.JOB_BOARD_ID)
        self.logger.info('Fetching jobs…')
        job_board = self.store.get_job_board(self.JOB_BOARD_ID)
        count_jobs = self.store.count_jobs(self.JOB_BOARD_ID)
        job_per_once = self.settings.get('JOBS_PER_ONCE', 500)
        offsets = range(0, count_jobs, job_per_once)
        self.logger.info('Fetching employers…')
        employers = self.store.getEmployers()
        self.logger.info('Fetched %s employers…' % len(employers))
        for employer in employers:
            self.employers[employer['id']] = employer
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
            jobs = self.store.get_jobs(self.JOB_BOARD_ID, offset, job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))
            #pprint(jobs)
            #creating/updating jobs
            for job in jobs:
                employer = self.employers[job['employer_id']]
                #pprint(employer)
                if 'google_cd_name' not in employer['metadata']:
                    company = self.job_service.create_company({'displayName': employer['name'], 'distributorCompanyId': employer['uid']})
                    if type(company) is dict and 'name' in company:
                        company_name = company['name']
                        self.store.save_gcd_company_name(employer, company_name)
                    else:
                        continue
                else:
                    company_name = employer['metadata']['google_cd_name']
                #pprint(job)

                url = 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], job_board['uuid'])
                if 'google_cd_job_name' not in job['attributes']:
                    gjob = {'requisitionId': str(job['id']), 'jobTitle': job['title'], 'description': job['description'], 'applicationUrls': url, 'companyName': company_name}
                    google_job = self.job_service.create_job_with_required_fields(gjob, batch=True)
                else:
                    gjob = {'requisitionId': str(job['id']), 'jobTitle': job['title'], 'description': job['description'], 'applicationUrls': url, 'companyName': company_name}
                    google_job = self.job_service.update_job(gjob, job['attributes']['google_cd_job_name'], batch=True)
                yield {'id': job['id'], 'attributes': job['attributes'], 'gjob': gjob}

        #deleting jobs
        count_jobs = self.store.count_jobs_for_delete(self.JOB_BOARD_ID)
        offsets = range(0, count_jobs, job_per_once)
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(count_jobs, offset + job_per_once - 1)))
            jobs = self.store.get_jobs_for_delete(self.JOB_BOARD_ID, offset, job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))
            for job in jobs:
                employer = self.employers[job['employer_id']]
                company_name = employer['metadata']['google_cd_name']
                gjob = {'requisitionId': str(job['id']), 'jobTitle': job['title'], 'description': job['description'], 'applicationUrls': job['url'], 'companyName': company_name}
                job_name = job['attributes']['google_cd_job_name']
                self.job_service.delete_job(gjob, job_name, batch=True)
                yield {'id': job['id'], 'attributes': job['attributes'], 'gjob': gjob}

    def mydebug(self):
        self.logger.info('Debug...')
        #pprint(self.job_service.get_company_list())
        jobs = self.job_service.get_job_list()
        pprint(jobs)
        pprint('Count jobs=%i' % len(jobs))





