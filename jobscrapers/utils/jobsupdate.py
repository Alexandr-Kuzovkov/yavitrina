#!/usr/bin/env python
#coding=utf-8

import psycopg2
import logging
from pprint import pprint
import json
import sys
import requests
import time
from unidecode import unidecode
import getopt
from dbacc import *

class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'


    def __init__(self, conf):
        self.dbname = conf.get('dbname')
        self.dbhost = conf.get('dbhost')
        self.dbport = conf.get('dbport')
        self.dbuser = conf.get('dbuser')
        self.dbpass = conf.get('dbpass')

    def dbopen(self):
        if self.conn is None:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.dbuser, password=self.dbpass, host=self.dbhost, port=self.dbport)
            self.cur = self.conn.cursor()

    def dbclose(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _get_fld_list(self, table, dbclose=False):
        self.dbopen()
        if '.' in table:
            table = table.split('.').pop()
        self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (self.schema, table))
        res = self.cur.fetchall()
        if res is not None:
            res = map(lambda i: i[0], res)
        if dbclose:
            self.dbclose()
        return res

    def _get(self, table, field_list=None, tail='', data=None):
        self.dbopen()
        if field_list is None:
            field_list = self._get_fld_list(table)
        sql = ' '.join(['SELECT', ','.join(field_list), 'FROM', table, tail, ';'])
        if data is None:
            self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        data = self.cur.fetchall()
        res = []
        for row in data:
            d = {}
            for i in range(len(row)):
                d[field_list[i]] = row[i]
            res.append(d)
        self.dbclose()
        return res

    def _getraw(self, sql, field_list, data=None):
        self.dbopen()
        if data is None:
            self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        data = self.cur.fetchall()
        res = []
        for row in data:
            if len(field_list) != len(row):
                raise Exception('Number fields in fields list no match number columns in result!')
            d = {}
            for i in range(len(row)):
                d[field_list[i]] = row[i]
            res.append(d)
        self.dbclose()
        return res

    def _exec(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        return res

    def _count_rows(self, table):
        sql = ' '.join(['SELECT count(*) AS count FROM', table])
        res = self._getraw(sql, ['count'])
        return int(res[0]['count'])

    def _clear_table(self, table):
        sql = ' '.join(['DELETE FROM', table])
        self.dbopen()
        try:
            self.cur.execute(sql)
        except psycopg2.Error, ex:
            print ex
            self.conn.rollback()
            self.dbclose()
        else:
            self.conn.commit()

    def _get_tables_list(self):
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type='BASE TABLE'"
        res = self._getraw(sql, ['table_name'])
        return map(lambda i: i['table_name'], res)

    def _insert(self, table, data):
        if type(data) is not list:
            raise Exception('Type of data must be list!')
        self.dbopen()
        for row in data:
            if type(row) is not dict:
                raise Exception('Type of row must be dict!')
            sql = ' '.join(['INSERT INTO', table, '(', ','.join(row.keys()), ') VALUES (', ','.join(['%s' for i in row.keys()]), ');'])
            try:
                values = map(lambda val: self._serialise_dict(val), row.values())
                self.cur.execute(sql, values)
            except psycopg2.Error, ex:
                self.conn.rollback()
                self.dbclose()
                print ex
                return {'result': False, 'error': ex}
        self.conn.commit()
        return {'result': True}

    def _serialise_dict(self, val):
        if type(val) is dict:
            return json.dumps(val)
        else:
            return val

    def _update(self, table, data, where, ident):
        if type(data) is not dict:
            raise Exception('Type of data must be dict!')
        self.dbopen()
        set_list = []
        for fld in data.keys():
            set_list.append(''.join([fld, '=%s']))
        sql = ' '.join(['UPDATE', table, 'SET', ','.join(set_list), 'WHERE', where])
        try:
            values = map(lambda val: self._serialise_dict(val), data.values())
            values.append(ident)
            self.cur.execute(sql, values)
        except psycopg2.Error, ex:
            self.dbclose()
            print ex
            return {'result': False, 'error': ex}
        self.conn.commit()
        return {'result': True}


class PgSQLJobsUpdate(PgSQLStore):
    def getXtramileJobsInBackend(self):
        fld_list = 'jobs.id,jobs.title,jobs.employer_id,jobs.external_id,jobs.external_unique_id,jobs.company,employers.name'
        sql = ' '.join(['SELECT', fld_list, "FROM jobs INNER JOIN employers ON jobs.employer_id=employers.id WHERE jobs.url LIKE '%jobs.xtramile.io%'"])
        return self._getraw(sql, fld_list.split(','))

    def updateJob(self, job_id, data):
        table = 'jobs'
        self._update(table, data, 'id=%s', job_id)


class JobsUpdate:
    store = None
    logger = None
    companyCache = {}
    campaignCache = {}

    def __init__(self, dbparams):
        self.store = PgSQLJobsUpdate(dbparams)
        self.logger = logging.getLogger('JobsUpdate')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(1)

    def coreApiRequest(self, url):
        res = requests.get(url)
        if res.status_code == 200:
            return res.json()
        else:
            return {}

    def getCompanyName(self, companyId):
        if companyId in self.companyCache:
            return self.companyCache[companyId]
        res = requests.get('http://api.xtramile.io/api/v1/companies/' + str(companyId))
        if res.status_code == 200:
            name = res.json()['name']
            self.companyCache[companyId] = name
            return name
        else:
            return None

    def filterCompany(self, company):
        if company is None:
            return True
        if 'test'.lower() in company.lower():
            return True
        return False

    def getCampaign(self, campaignId):
        if campaignId in self.campaignCache:
            return self.campaignCache[campaignId]
        res = requests.get('http://api.xtramile.io/api/v1/campaigns/' + str(campaignId))
        if res.status_code == 200:
            d = res.json()
            self.campaignCache[campaignId] = d
            return d
        else:
            return {}

    def coreApiInfo(self):
        items = self.coreApiRequest('http://api.xtramile.io/api/v1/items')
        self.logger.info('Items:')
        self.logger.info(len(items))
        for item in items:
            company = self.getCompanyName(item['companyId'])
            campaign = self.getCampaign(item['campaignId'])
            if company is not None:
                ast = '*'
            else:
                ast = ''
            try:
                self.logger.info('id=%i, title=%s, company=%s, campaigns=%s, start_date=%s %s' % (item['id'], item['title'], company, campaign['title'], time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(campaign['start_date'])), ast))
            except KeyError, ex:
                self.logger.info('id=%i, title=%s, company=%s, %s' % (item['id'], item['title'], company, ast))
        self.logger.info('-' * 60)

        campaigns = self.coreApiRequest('http://api.xtramile.io/api/v1/campaigns')
        self.logger.info('Campaigns:')
        for item in campaigns:
            self.logger.info('title=%s, id=%i' % (item['title'], item['id']))
        self.logger.info('-' * 60)

        companies = self.coreApiRequest('http://api.xtramile.io/api/v1/companies')
        self.logger.info('Companies:')
        self.logger.info(len(companies))
        for item in companies:
            self.logger.info('name=%s, id=%i' % (item['name'], item['id']))
        self.logger.info('-' * 60)


    def test(self):
        self.logger.info('Test mode')
        self.coreApiInfo()

    def run(self):
        self.logger.info('Run jobs update')
        xtramile_jobs = self.store.getXtramileJobsInBackend()
        self.logger.info('%i jobs for update' % len(xtramile_jobs))
        count = 0
        for backend_job in xtramile_jobs:
            count += 1
            self.logger.info('%i/%i Updating job id=%i' % (count, len(xtramile_jobs), backend_job['jobs.id']))
            coreapi_job = self.coreApiRequest('http://api.xtramile.io/api/v1/items/%s' % backend_job['jobs.external_id'])
            coreapi_company = self.coreApiRequest('http://api.xtramile.io/api/v1/companies/%i' % coreapi_job['companyId'])
            coreapi_campaign = self.coreApiRequest('http://api.xtramile.io/api/v1/campaigns/%i' % coreapi_job['campaignId'])
            slug = coreapi_job['slug']
            company_slug = coreapi_job['companySlug']
            attributes = {"cv": False, "lang": "en", "phone": False, "ext_url": "", "jobolizer": ""}
            posted_at = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(coreapi_campaign['start_date']))
            updated_at = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time())))
            data = {'slug': slug, 'company_slug': company_slug, 'attributes': attributes, 'posted_at': posted_at, 'updated_at': updated_at}
            self.store.updateJob(backend_job['jobs.id'], data)
            self.logger.info('Job id=%i was updated with data=%s' % (backend_job['jobs.id'], str(data)))
        self.logger.info('End jobs update')


jobsupdate = JobsUpdate(local)
test = False

try:
    optlist, args = getopt.getopt(sys.argv[1:], 't', ['test'])
    if '-t' in map(lambda item: item[0], optlist):
        test = True
    elif '--test' in map(lambda item: item[0], optlist):
        test = True
except Exception, ex:
    print 'Usage: %s options' % sys.argv[0]
    print 'Options:  -t or --test - Run test code. Optional'
    print ex.message
    exit(1)

if test:
    jobsupdate.test()
else:
    jobsupdate.run()






