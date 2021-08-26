# -*- coding: utf-8 -*-
from __future__ import print_function
from apiclient.discovery import build_from_document, build
from oauth2client.service_account import ServiceAccountCredentials
import pkgutil
import sys
import os
import json
import httplib2
import logging
import urllib

class GoogleGloudJobDiscovery:

    SCOPE = 'https://www.googleapis.com/auth/jobs'
    CREDENTIAL_FILE = None
    DISCOVERY_JSON_FILE = None
    job_service = None
    company_model = {'displayName': str, 'distributorCompanyId': str}
    job_model = {'requisitionId': [str, unicode], 'jobTitle': [str, unicode], 'description': [str, unicode], 'applicationUrls': [str, unicode], 'companyName': [str, unicode]}
    created_jobs = []
    updated_jobs = []
    deleted_jobs = []
    create_requests = []
    update_requests = []
    delete_requests = []
    MAX_BATCH = 25

    def __init__(self, scope, credential_file, discovery_json_file):
        self.SCOPE = scope
        self.CREDENTIAL_FILE = credential_file
        self.DISCOVERY_JSON_FILE = discovery_json_file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CREDENTIAL_FILE, self.SCOPE)
        http = credentials.authorize(httplib2.Http())
        document = json.loads(self.DISCOVERY_JSON_FILE)
        self.job_service = build_from_document(document, 'jobs', 'v2', http=http)
        self.logger = logging.getLogger(__name__)

    def check_data(self, data, model):
        for name, data_type in model.items():
            if name not in data:
                self.logger.error('Field "%s" is required!' % name)
                return False
            if type(data_type) is list:
                if type(data[name]) not in data_type:
                    self.logger.error('Field "%s" must have type "%s" but has type "%s"!' % (name, str(data_type), type(data[name])))
            else:
                if type(data[name]) is not data_type:
                    self.logger.error('Field "%s" must have type "%s" but has type "%s"!' % (name, str(data_type), type(data[name])))
                    return False
        return True

    def create_company(self, company):
        """Creates company with required fields."""
        if not self.check_data(company, self.company_model):
            self.logger.error('Company must have format: "%s"' % str(self.company_model))
            return None
        self.logger.info('Create gcd company for employer "%s"' % company['displayName'])
        company = {
            'displayName': company['displayName'],
            'distributorCompanyId': company['distributorCompanyId']
        }
        try:
            created_company = self.job_service.companies().create(body=company).execute()
            return created_company
        except Exception, ex:
            self.logger.warning(ex)
            return None

    def get_company(self, company_name):
        """Gets company with given name"""
        company = self.job_service.companies().get(name=company_name).execute()
        #print(json.dumps(company, indent=2))
        return company

    def create_job_with_required_fields(self, job, batch=False):
        """Creates basic job with given company_name"""
        if not self.check_data(job, self.job_model):
            self.logger.error('Job must have format: "%s"' % str(self.job_model))
            return None
        self.logger.info('Creating gcd job for job id=%s...' % job['requisitionId'])
        create_job_request = {
            'job': job
        }
        if not batch:
            created_job = self.job_service.jobs().create(body=create_job_request).execute()
            #print(json.dumps(created_job, indent=2))
            return created_job
        else:
            request = self.job_service.jobs().create(body=create_job_request)
            self.create_requests.append(request)
            return request

    def get_job(self, job, job_name):
        """Gets job with given name"""
        job = self.job_service.jobs().get(name=job_name).execute()
        #pprint(json.dumps(job, indent=2))
        return job

    def update_job(self, job, job_name, batch=False):
        """Updates job title with given job name."""
        if not self.check_data(job, self.job_model):
            self.logger.error('Job must have format: "%s"' % str(self.job_model))
        self.logger.info('Updating gcd job for job id=%s...' % job['requisitionId'])
        update_job_request = {
            'job': job,
            'updateJobFields': 'jobTitle'
        }
        if not batch:
            updated_job = self.job_service.jobs().patch(name=job_name, body=update_job_request).execute()
            #pprint(json.dumps(updated_job, indent=2))
            return updated_job
        else:
            request = self.job_service.jobs().patch(name=job_name, body=update_job_request)
            self.update_requests.append(request)
            return request

    def delete_job(self, job, job_name, batch=False):
        """Deleting job"""
        if not self.check_data(job, self.job_model):
            self.logger.error('Job must have format: "%s"' % str(self.job_model))
        self.logger.info('Deleting gcd job for job id=%s...' % job['requisitionId'])
        if not batch:
            self.job_service.jobs().delete(name=job_name).execute()
            return job
        else:
            request = self.job_service.jobs().delete(name=job_name)
            self.delete_requests.append(request)
            self.deleted_jobs.append(job)
            return job

    def get_company_list(self):
        """Get companies list"""
        ls = self.job_service.companies().list().execute()
        return ls

    def get_job_list(self, company_name=None):
        """Get jobs list"""
        jobs = []
        if company_name is not None:
            filter_string = 'companyName = "%s"' % company_name
            res2 = self.job_service.jobs().list(filter=filter_string).execute()
            if 'jobs' in res2:
                jobs += res2['jobs']
        else:
            res = self.get_company_list()
            for company in res['companies']:
                company_name = company['name']
                filter_string = 'companyName = "%s"' % company_name
                res2 = self.job_service.jobs().list(filter=filter_string).execute()
                if 'jobs' in res2:
                    jobs += res2['jobs']
        return jobs

    def create_callback(self, request_id, response, exception):
        """Callback for batch create."""
        #print(response)
        self.created_jobs.append(response)

    def update_callback(self, request_id, response, exception):
        """Callback for batch update."""
        #print(response)
        self.updated_jobs.append(response)

    def delete_callback(self, request_id, response, exception):
        """Callback for batch delete."""
        pass

    def execute_batch_create(self):
        self.logger.info('Executing batch create')
        offsets = range(0, len(self.create_requests), self.MAX_BATCH)
        for offset in offsets:
            batch_create = self.job_service.new_batch_http_request()
            for request in self.create_requests[offset:self.MAX_BATCH]:
                batch_create.add(request, callback=self.create_callback)
            batch_create.execute()

    def execute_batch_update(self):
        self.logger.info('Executing batch update')
        offsets = range(0, len(self.update_requests), self.MAX_BATCH)
        for offset in offsets:
            batch_update = self.job_service.new_batch_http_request()
            for request in self.update_requests[offset:self.MAX_BATCH]:
                batch_update.add(request, callback=self.update_callback)
            batch_update.execute()

    def execute_batch_delete(self):
        self.logger.info('Executing batch delete')
        offsets = range(0, len(self.delete_requests), self.MAX_BATCH)
        for offset in offsets:
            batch_delete = self.job_service.new_batch_http_request()
            for request in self.delete_requests[offset:self.MAX_BATCH]:
                batch_delete.add(request, callback=self.delete_callback)
            batch_delete.execute()



class GoogleIndexAPI:

    SCOPES = ["https://www.googleapis.com/auth/indexing"]
    ENDPOINT = "https://indexing.googleapis.com/v3/urlNotifications:publish"
    CREDENTIAL_FILE = None
    service = None
    http = None

    def __init__(self, credential_file):
        self.CREDENTIAL_FILE = credential_file
        credentials = ServiceAccountCredentials.from_json_keyfile_name(self.CREDENTIAL_FILE, self.SCOPES)
        self.http = credentials.authorize(httplib2.Http())
        self.logger = logging.getLogger(__name__)

    def update_url(self, url):
        content = {'url': url, 'type': 'URL_UPDATED'}
        try:
            response, content = self.http.request(self.ENDPOINT, method='POST', body=json.dumps(content))
        except Exception, ex:
            return {'error': ex.message}
        return json.loads(content.replace('\n', '').strip())

    def remove_url(self, url):
        content = {'url': url, 'type': 'URL_DELETED'}
        try:
            response, content = self.http.request(self.ENDPOINT, method='POST', body=json.dumps(content))
        except Exception, ex:
            return {'error': ex.message}
        return json.loads(content.replace('\n', '').strip())

    def get_notification_status(self, url):
        url = "https://indexing.googleapis.com/v3/urlNotifications/metadata?url=%s" % urllib.quote_plus(url)
        try:
            response, content = self.http.request(url, method='GET')
        except Exception, ex:
            return {'error': ex.message}
        return json.loads(content.replace('\n', '').strip())




