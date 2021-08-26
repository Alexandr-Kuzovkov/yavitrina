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
import uuid
from feedgenerator.pipelines import MyXmlItemExporter as Exporter
import tempfile
from feedgenerator.azurestorage import AzureStorage
import os
import time
from feedgenerator.mythread import myThread

class GeneralSpider(scrapy.Spider):

    name = 'general4'
    publisher = 'myXtramile network'
    publisherurl = 'https://xtramile.io/'
    facebookBoardIds = []
    toAzure = None
    handle_httpstatus_list = [404, 400]
    azurestorage = None
    files = []
    date_format = '%Y-%m-%d'
    exported_jobs = {}
    threads = []

    def __init__(self, azure=None, force=False, port=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self.store = PgSQLStoreFeedExport()
        self.geoname = Geoname()
        self.azurestorage = azure
        self.port = port
        if str(force).lower() == 'true':
            force = True
        else:
            force = False
        self.job_boards = self.store.getJobBoardsNeedUpdate(force)
        self.job_boards_feed_settings = self.store.getBoardFeedSettingAll()
        for id, board_feed_settings in self.job_boards_feed_settings.items():
            if 'date_format' not in board_feed_settings:
                self.job_boards_feed_settings[id]['date_format'] = '%Y-%m-%d'
        self.exporters = {}
        self.feeds = {}
        self.tempfiles = {}


    def start_requests(self):
        if len(self.job_boards) == 0:
            self.logger.info('No job_boards to update. Exiting...')
            return
        self.feeds_folder = self.settings.get('FEED_DIR', '')
        self.feeds_folder_local = self.settings.get('FEED_DIR_LOCAL', '')
        if self.settings.get('AZURE_ENABLE', False) or self.azurestorage is not None:
            self.azurestorage = AzureStorage(self.settings.get('AZURE_ACCOUNT_NAME', ''), self.settings.get('AZURE_ACCOUNT_KEY', ''))
        self.facebookBoardIds = self.store.getFacebookBoardIds()
        self.logger.info('refreshing view...')
        self.store.refresh_view()
        self.logger.info('done')
        for job_board in self.job_boards:
            self.start_board(job_board)
        url = 'http://localhost'
        if self.port is not None:
            url = ':'.join([url, str(self.port)])
        request = scrapy.Request(url, callback=self.processing)
        yield request


    def processing(self, response):
        self.job_ids = self.store.get_job_ids()
        self.boards_for_job = self.get_boards_for_job(self.job_ids)
        self.logger.info('%i jobs found' % len(self.job_ids))
        self.job_per_once = self.settings.get('JOBS_PER_ONCE', 5000)
        offsets = range(0, len(self.job_ids), self.job_per_once)
        for offset in offsets:
            self.logger.info('...fetch jobs  %i - %i' % (offset, min(len(self.job_ids), offset + self.job_per_once - 1)))
            jobs = self.store.get_jobs(self.job_ids, offset, offset+self.job_per_once)
            self.logger.info('...fetched %i jobs' % len(jobs))

            for job_board in self.job_boards:
                job_board_id = job_board['id']
                if job_board_id not in self.job_boards_feed_settings:
                    continue
                if job_board['done']:
                    continue
                exported = self.export_jobs(jobs, job_board, self.job_boards_feed_settings[job_board_id])
                self.logger.info('...exporting  %i jobs to job_board %i done' % (exported, job_board_id))
                self.exported_jobs[job_board_id] += exported
                if self.exported_jobs[job_board_id] >= job_board['number_jobs']:
                    self.finish_board(job_board)
                    self.push_to_azure(job_board)

        self.logger.info('...finish exporting')
        for job_board in self.job_boards:
            self.finish_board(job_board)

        if self.azurestorage is not None:
            self.logger.info('...push to Azure Storage')
            for job_board in self.job_boards:
                self.push_to_azure(job_board)
        self.logger.info('All done')


    def start_board(self, job_board):
        job_board_id = job_board['id']
        if job_board_id not in self.job_boards_feed_settings:
            return
        self.logger.info('...start job_board %i' % job_board_id)
        self.exported_jobs[job_board_id] = 0
        self.feeds[job_board_id] = job_board['feed']
        self.tempfiles[job_board_id] = tempfile.TemporaryFile()
        root_element = self.job_boards_feed_settings[job_board_id]['root']
        item_element = self.job_boards_feed_settings[job_board_id]['job']
        self.exporters[job_board_id] = Exporter(self.tempfiles[job_board_id], item_element=item_element,
                                                root_element=root_element, spider=self)
        self.exporters[job_board_id].setPublisher(self.publisher)
        self.exporters[job_board_id].setPublisherUrl(self.publisherurl)
        self.exporters[job_board_id].start_exporting()

    def finish_board(self, job_board):
        job_board_id = job_board['id']
        if job_board_id not in self.job_boards_feed_settings:
            return
        if job_board['done']:
            return
        self.logger.info('...finishing job_board %i' % job_board_id)
        self.exporters[job_board_id].finish_exporting()
        try:
            feed_file = open('%s/%s' % (self.feeds_folder, self.feeds[job_board_id]), 'w+b')
            full_path_to_file = os.path.abspath('%s/%s' % (self.feeds_folder, self.feeds[job_board_id]))
        except IOError, ex:
            feed_file = open('%s/%s' % (self.feeds_folder_local, self.feeds[job_board_id]), 'w+b')
            full_path_to_file = os.path.abspath('%s/%s' % (self.feeds_folder_local, self.feeds[job_board_id]))
        fp = self.tempfiles[job_board_id]
        fp.seek(0)
        feed_file.write(fp.read())
        fp.close()
        feed_file.close()
        date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        self.store.changeFeedUpdated(job_board_id, date)
        job_board['done'] = True

    def push_to_azure(self, job_board):
        time.sleep(3)
        job_board_id = job_board['id']
        if job_board_id not in self.job_boards_feed_settings:
            return
        if job_board['azure_done']:
            return
        try:
            feed_file = open('%s/%s' % (self.feeds_folder, self.feeds[job_board_id]), 'r+b')
            full_path_to_file = os.path.abspath('%s/%s' % (self.feeds_folder, self.feeds[job_board_id]))
        except IOError, ex:
            feed_file = open('%s/%s' % (self.feeds_folder_local, self.feeds[job_board_id]), 'r+b')
            full_path_to_file = os.path.abspath('%s/%s' % (self.feeds_folder_local, self.feeds[job_board_id]))
        feed_file.close()
        if self.azurestorage is not None:
            self.threads.append(myThread(full_path_to_file, self.azurestorage, self.logger))
            self.threads[len(self.threads) - 1].start()
            job_board['azure_done'] = True

    def export_jobs(self, jobs, job_board, board_feed_settings):
        exported = 0
        for job in jobs:
            if self.is_demo_job(job):
                continue
            if job['employer_id'] == 99 and job_board['id'] in [31, 45]:
                continue
            if job_board['id'] not in self.boards_for_job[job['id']]:
                continue
            Item = {}
            if board_feed_settings is not None:
                for key, value in board_feed_settings.items():
                    if key in ['id', 'job_board_id', 'root', 'job', 'cpc', 'created_at', 'updated_at', 'date_format',
                               'separate_by_country', 'allowed_countries', 'required_fields', 'url']:
                        continue
                    if value is not None and type(value) is str and len(value.strip()) > 0:
                        try:
                            if (value in Item) and (Item[value] is not None) and (job[fldMap[key]] is not None):
                                Item[value] = ', '.join([Item[value], job[fldMap[key]]])
                            elif (value in Item) and (Item[value] is not None):
                                pass
                            else:
                                Item[value] = job[fldMap[key]]
                        except Exception, ex:
                            Item[value] = None
                Item['cpc'] = self.store.getCpc(job, job_board['id'])
                Item[board_feed_settings['url']] = 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], job_board['uuid'])
                #Item['url'] = 'https://pixel.xtramile.io/t/%s?s=%s' % (job['uid'], self.board['uuid'])

                #if job['employer_id'] == 99 and self.board['id'] == 45:
                #    Item[self.board_feed_settings['url']] = 'https://pixel.xtramile.io/t/450986ab-cccf-48df-9ec4-fa04fed82da1?s=2b56e0a2-b604-408e-a6db-fa35cae8fca7'
                #if job['employer_id'] == 99 and self.board['id'] == 31:
                #    Item[self.board_feed_settings['url']] = 'https://pixel.xtramile.io/t/450986ab-cccf-48df-9ec4-fa04fed82da1?s=ee18161d-f701-4acf-9c16-78109b8b7661'

                if job_board['name'] in ['DensouTest', 'Neuvoo', 'NominalTechnoTest']:
                    Item['logo'] = self.store.getEmployerLogo(job)
                #if job_board['name'] in ['Jobrapido']:
                #    if Item['country'] != 'FR':
                #        Item['country'] = 'FR'
                #        Item['location'] = 'Abroad'
                if job_board['name'] in ['Jobijoba']:
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
                if job_board['id'] in [45, 58, 59]:
                    Item['url'] = job['url']
                    if 'salary' in job['attributes']:
                        Item['salary'] = job['attributes']['salary']
                if job_board['id'] in self.facebookBoardIds:
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
            self.exporters[job_board['id']].export_item(Item)
            exported += 1
        return exported


    def is_demo_job(self, job):
        if job['employer_id'] in [31]:
            return True
        if 'demo' in job['title'].lower().split(' '):
            return True
        if 'demo' in job['title'].lower().split('-'):
            return True
        return False

    def get_boards_for_job(self, job_ids):
        boards_for_job = {}
        for item in job_ids:
            boards_for_job[item['job_id']] = item['boards']
        return boards_for_job



