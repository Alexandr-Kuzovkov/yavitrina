# -*- coding: utf-8 -*-
import scrapy
from scrapy_splash import SplashRequest
import pkgutil
from monitor.extensions import PgSQLStoreMonitor
from pprint import pprint
import json
import requests
import time
import pkg_resources
import os

class JobApplySpider(scrapy.Spider):
    name = 'fakeapply'
    allowed_domains = ['xtramile.tech', 'xtramile.io']
    subject = 'Make fake apply'
    errors = []
    url = None
    file_size = 0
    task = {916654: 19, 916645: 18, 916640: 20, 916590: 21, 915661: 19, 916691: 20, 909926: 20}

    def __init__(self, env='dev', *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if env not in ['dev', 'prod']:
            raise Exception('argument "env" must be "dev" or "prod"')
            exit(1)
        self.env = env
        self.store = PgSQLStoreMonitor(env)

    def start_requests(self):
        for job_id, count in self.task.items():
            job = self.store.getJobById(job_id)
            for i in range(0, count):
                self.logger.info('%i apply for job id=%i' % (i+1, job_id))
                self.job = job
                if self.env == 'prod':
                    url = 'https://service.xtramile.io/api/candidates/with/upload'
                elif self.env == 'dev':
                    url = 'https://service.xtramile.tech/api/candidates/with/upload'
                token = self.store.getAdminToken()
                resource_package = 'monitor'  # Could be any module/package name
                resource_path = '/'.join(('files', 'test.pdf'))
                cv_file = pkg_resources.resource_stream(resource_package, resource_path)
                files = {'cv': ('cv.pdf', cv_file, 'application/pdf', {'Expires': '0'})}
                email = 'user' + str(int(time.time())) + '@mail.com'
                candidate = {
                    "userIp": "127.0.0.1",
                    "name": "candidate",
                    "email": email,
                    "phone": '+71929%s' % str(int(time.time()))[4:10],
                    "job": "/api/jobs/" + str(job['id']),
                    "uid": self.store.getEmployerUid(job['employer_id'])
                }
                payload = {'candidate': json.dumps(candidate)}
                headers = {'Authorization': token}
                result = requests.post(url, data=payload, files=files, headers=headers, verify=False)
                cv_file.seek(0, os.SEEK_END)
                self.file_size = cv_file.tell()
                request = scrapy.Request('http://127.0.0.1', callback=self.check_apply)
                request.meta['result'] = result
                yield request

    def check_apply(self, response):
        self.logger.info('checking apply "%s"' % response.url)
        result = response.meta['result']
        if result.status_code != 201:
            self.errors.append('Response code: %i' % result.status_code)
            self.logger.error('Response code: %i' % result.status_code)
        try:
            data = json.loads(result.text)
            print data
        except Exception, ex:
            self.errors.append('Response not valid JSON')
            self.logger.error('Response not valid JSON')
        candidate = self.store.getCandidateByEmail(data['email'])
        if candidate is None:
            self.errors.append('Candidate id=%i not found' % data['id'])
            self.logger.error('Candidate id=%i not found' % data['id'])
        if self.env == 'prod':
            url = 'https://service.xtramile.io/api/files/%i/download' % candidate['resume_file']
        elif self.env == 'dev':
            url = 'https://service.xtramile.tech/api/files/%i/download' % candidate['resume_file']
        headers = {'Authorization': self.store.getAdminToken()}
        print 'sleeping...'
        time.sleep(20)
        result = requests.head(url, headers=headers, verify=False)
        if result.status_code == 200 and result.headers['Content-Type'] == 'application/pdf' and int(result.headers['Content-Length']) == self.file_size:
            pass
        else:
            self.errors.append('File id=%i not found or invalid' % candidate['resume_file'])
            self.logger.error('File id=%i not found or invalid' % candidate['resume_file'])
        yield data







