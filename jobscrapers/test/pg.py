#!/usr/bin/env python
#coding=utf-8

import psycopg2
from pprint import pprint
import uuid
import scrapy
import json


#dbname = 'xtramile'
#dbhost = 'api.xtramile.tech'
#dbport = 5432
#dbuser = 'xtramile'
#dbpass = 'xtramileDev'

dbname = 'xtramile_prod'
dbhost = 'tools.xtramile.tech'
dbport = 5432
dbuser = 'postgres'
dbpass = 'eshi1Ro0'

class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'

    def __init__(self, log_is_enabled=False):
        if log_is_enabled:
            print("log is enabled!")

    @classmethod
    def from_crawler(self, crawler):
        self.settings = crawler.settings
        return self(self.settings)

    def getSettings(self):
        return self.settings

    def dbopen(self):
        if self.conn is None:
            self.conn = psycopg2.connect(dbname=dbname, user=dbuser, password=dbpass, host=dbhost, port=dbport)
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


    def _get(self, table, field_list=None, where='', data=None):
        self.dbopen()
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

    def getEmployers(self, employer_name):
        employers = self._get('jobs.employers',
                        ['id', 'name', 'url', 'feed_url', 'feed_updated_at', 'default_job_budget'],
                        'name=%s', (employer_name,))
        return employers

    def getEmployerFeedSetting(self, employer_id):
        field_list = ['id', 'employer_id', 'root', 'job', 'job_external_id', 'url', 'title', 'city',
                      'state', 'country', 'description', 'job_type', 'category', 'posted_at', 'created_at', 'updated_at']
        employer_feed_settings = self._get('jobs.employer_feed_settings', field_list, 'employer_id=%s', (employer_id,))
        if len(employer_feed_settings) > 0:
            return employer_feed_settings[0]
        else:
            return None

    def getEmployerdByName(self, employer_name, fld_list=None):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=fld_list, where='name=%s', data=[employer_name])
        if len(employers) > 0:
            return employers[0]
        return None

    def getEmployers(self, employer_name):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=None, where='name=%s', data=(employer_name,))
        return employers

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception, ex:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        self.dbclose()
        if len(res) > 0:
            return res[0]
        else:
            return None


db = PgSQLStore()
'''
pprint(db.getEmployers('Societe Generale'))
pprint(db.getEmployers('Job opportunities at PwC Luxembourg'))
pprint(db.getEmployers('Réseau Alliance'))
pprint(db.getEmployers("L'OREAL"))

pprint(db.getEmployerFeedSetting(1))
pprint(db.getEmployerFeedSetting(2))
pprint(db.getEmployerFeedSetting(3))

pprint(db.getEmployers("L'OREAL"))
pprint(db.getEmployerFeedSetting(4))
'''


'''
fld_list = 'url,title,posted_at,updated_at'.split(',')
rows = db._get('public.jobs', fld_list, 'id=%s', [143])
pprint(rows)
print rows[0]['posted_at'].strftime('/%d/%m/%Y')
'''


#pprint(db._get_fld_list('employers'))
#pprint(db.getEmployerdByName('Adista'))
#pprint(db.getEmployers('Adista'))
metadata = {}
metadata['spider'] = 'onepoint'
pprint(db.getEmployerByMetadata(json.dumps(metadata)))
pprint(db.getEmployerByMetadata(metadata))

metadata['spider'] = 'xtramile'
metadata['employer_id'] = 58
pprint(db.getEmployerByMetadata(json.dumps(metadata)))
pprint(db.getEmployerByMetadata(metadata))
