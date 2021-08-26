#coding=utf-8
import requests
import json
import sqlite3
import urllib
import psycopg2
import logging
import json
from ConfigParser import *
import os


class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    #schema = 'jobs'
    schema = 'public'
    jobs_table = 'jobs'
    employers_table = 'employers'
    employer_feed_settings_table = 'employer_feed_settings'
    employers_table = 'employers'
    db = {'default': None, 'legacy': None}
    dbname = None
    dbhost = None
    dbport = None
    dbuser = None
    dbpass = None
    curr_db = None


    def __init__(self, config_file=None, log_is_enabled=False):
        if log_is_enabled:
            print("log is enabled!")
        config_file = None
        path = ['/home/user1/db.conf', '/home/ubuntu/db.conf', '/home/root/db.conf']
        self.conf = ConfigParser()
        if config_file is None:
            for filename in path:
                if os.path.isfile(filename):
                    self.conf.read(filename)
                    break
        else:
            self.conf.read(config_file)
        sections = self.conf.sections()
        print(sections)
        if ('prod' not in sections) or 'coreapi_prod' not in sections:
            raise Exception('Config file error!')
        self.dbname = self.conf.get('prod', 'dbname')
        self.dbhost = self.conf.get('prod', 'dbhost')
        self.dbport = self.conf.get('prod', 'dbport')
        self.dbuser = self.conf.get('prod', 'dbuser')
        self.dbpass = self.conf.get('prod', 'dbpass')

        self.dbname2 = self.conf.get('coreapi_prod', 'dbname')
        self.dbhost2 = self.conf.get('coreapi_prod', 'dbhost')
        self.dbport2 = self.conf.get('coreapi_prod', 'dbport')
        self.dbuser2 = self.conf.get('coreapi_prod', 'dbuser')
        self.dbpass2 = self.conf.get('coreapi_prod', 'dbpass')

    @classmethod
    def from_crawler(self, crawler):
        self.settings = crawler.settings
        return self(self.settings)

    def getSettings(self):
        return self.settings

    def dbopen(self, option='default'):
        if self.curr_db != option:
            self.curr_db = option
            self.dbclose()
        if self.conn is None:
            if option == 'default':
                self.conn = psycopg2.connect(dbname=self.dbname, user=self.dbuser, password=self.dbpass, host=self.dbhost, port=self.dbport)
                self.cur = self.conn.cursor()
            elif option == 'legacy':
                self.conn = psycopg2.connect(dbname=self.dbname2, user=self.dbuser2, password=self.dbpass2, host=self.dbhost2, port=self.dbport2)
                self.cur = self.conn.cursor()
            else:
                pass

    def dbclose(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _get_fld_list(self, table, dbclose=False, option='default'):
        if '.' in table:
            table = table.split('.').pop()
        self.dbopen(option)
        self.cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (self.schema, table))
        res = self.cur.fetchall()
        if res is not None:
            res = map(lambda i: i[0], res)
        if dbclose:
            self.dbclose()
        return res

    def _get(self, table, field_list=None, where='', data=None,  option='default'):
        self.dbopen(option)
        if field_list is None:
            field_list = self._get_fld_list(table)
        sql = ' '.join(['SELECT', ','.join(field_list), 'FROM', table, 'WHERE', where, ';'])
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

    def _get_part(self, table, field_list=None, tail='', data=None, option='default'):
        self.dbopen(option)
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

    def _getraw(self, sql, field_list, data=None, option='default'):
        self.dbopen(option)
        if data is None:
            # print sql
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

    def _count_rows(self, table, where=None, data=None):
        if where is None:
            sql = ' '.join(['SELECT count(*) AS count FROM', table])
            res = self._getraw(sql, ['count'])
        else:
            sql = ' '.join(['SELECT count(*) AS count FROM', table, 'WHERE', where])
            if data is None:
                res = self._getraw(sql, ['count'])
            else:
                res = self._getraw(sql, ['count'], data)
        return int(res[0]['count'])


class PgSQLStoreMonitor(PgSQLStore):

    def getJobs(self, employer_id=None, status=None, fld_list=None, offset=0, limit=None):
        table = '.'.join([self.schema, self.jobs_table])
        if status is None:
            if employer_id is not None:
                return self._get_part(table, field_list=fld_list, tail='WHERE employer_id=%s ORDER BY id OFFSET %s LIMIT %s', data=[employer_id, offset, limit])
            else:
                return self._get_part(table, field_list=fld_list, tail='ORDER BY id OFFSET %s LIMIT %s', data=[offset, limit])
        else:
            if employer_id is not None:
                return self._get_part(table, field_list=fld_list, tail='WHERE employer_id=%s AND status=%s ORDER BY id OFFSET %s LIMIT %s', data=[employer_id, status, offset, limit])
            else:
                return self._get_part(table, field_list=fld_list, tail='WHERE status=%s ORDER BY id OFFSET %s LIMIT %s', data=[status, offset, limit])



    def countJobs(self, employer_id=None, status=None):
        table = '.'.join([self.schema, self.jobs_table])
        if status is None:
            if employer_id is not None:
                return self._count_rows(table, 'employer_id=%s LIMIT 5', [employer_id])
            else:
                return self._count_rows(table, 'TRUE LIMIT 5')
        else:
            if employer_id is not None:
                return self._count_rows(table, 'employer_id=%s AND status=%s LIMIT 5', [employer_id, status])
            else:
                return self._count_rows(table, 'status=%s LIMIT 5', [status])


    def getCompanies(self, employer_id=None):
        if employer_id is None:
            sql = 'SELECT employer_id, company_slug FROM jobs GROUP BY employer_id, company_slug'
            return self._getraw(sql, field_list=['employer_id', 'company_slug'], data=None)
        else:
            sql = 'SELECT employer_id, company_slug FROM jobs WHERE employer_id=%s GROUP BY employer_id, company_slug'
            return self._getraw(sql, field_list=['employer_id', 'company_slug'], data=[employer_id])


    def getCompanyProfile(self, company_slug):
        sql = 'SELECT profile FROM company WHERE slug=%s'
        res = self._getraw(sql, field_list=['profile'], data=[company_slug], option='legacy')
        if type(res) is list and len(res) > 0:
            return res[0]['profile']
        return {}

    def getCompanyName(self, company_slug):
        sql = 'SELECT name FROM company WHERE slug=%s'
        res = self._getraw(sql, field_list=['name'], data=[company_slug], option='legacy')
        if type(res) is list and len(res) > 0:
            return res[0]['name']
        return ''

    def getCompanyDescription(self, company_slug):
        sql = 'SELECT description FROM company WHERE slug=%s'
        res = self._getraw(sql, field_list=['description'], data=[company_slug], option='legacy')
        if type(res) is list and len(res) > 0:
            return res[0]['description']
        return ''

    def getDemoJob(self, slug):
        table = self.jobs_table
        res = self._get(table, field_list=None, where='slug=%s', data=[slug])
        if len(res) > 0:
            return res[0]
        return None

    def getJobById(self, job_id):
        table = self.jobs_table
        res = self._get(table, field_list=None, where='id=%s', data=[job_id])
        if len(res) > 0:
            return res[0]
        return None

    def getEmployerUid(self, employer_id):
        table = self.employers_table
        res = self._get(table, ['uid'], where='id=%s', data=[employer_id])
        if len(res) > 0:
            return res[0]['uid']
        return None

    def getAdminToken(self):
        sql = 'SELECT id FROM extendedaccesstoken WHERE userid=(SELECT id FROM xuser WHERE isadmin = TRUE)'
        res = self._getraw(sql, field_list=['id'], data=None, option='default')
        if len(res) > 0:
            return res[0]['id']
        return None

    def getCandidateByEmail(self, email):
        table = 'candidates'
        res = self._get(table, field_list=None, where="email=%s AND name='monitor-candidate'", data=[email])
        if len(res) > 0:
            return res[0]
        return None

    def getEmployerLogo(self, job):
        table = '.'.join([self.schema, self.employers_table])
        res = self._get(table, ['uid'], 'id=%s', [job['employer_id']])
        if len(res) > 0:
            uid = res[0]['uid']
            sql = "SELECT profile->>'logo' AS logo FROM company WHERE profile->>'uid' = %s"
            res = self._getraw(sql, field_list=['logo'], data=[uid], option='legacy')
            if len(res) > 0 and res[0]['logo'] is not None:
                return 'https://api.xtramile.io/api/v1/files/%s/download' % res[0]['logo']
        return None



