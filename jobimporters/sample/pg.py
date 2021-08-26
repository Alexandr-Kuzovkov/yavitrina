#!/usr/bin/env python
#coding=utf-8

#This spider is only for copy CoreApi jobs to Backend
#Not need run permanetly

import psycopg2
from dbacc import *
from pprint import pprint

class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    #schema = 'jobs'
    schema = 'public'
    jobs_table = 'jobs'
    employers_table = 'employers'
    employer_feed_settings_table = 'employer_feed_settings'

    dbname = 'dbname'
    dbhost = 'dbhost'
    dbport = 0
    dbuser = 'dbuser'
    dbpass = 'dbpass'

    pkey = 'external_unique_id' #primary key for jobs_table
    employer_id = None
    job_status = {
        'INACTIVE': {'code': 0, 'desc': 'Inactive'},
        'ACTIVE': {'code': 1, 'desc': 'Active'},
        'EXPIRED': {'code': 2, 'desc': 'Expired'},
        'MANUALLY_ACTIVE': {'code': 3, 'desc': 'Manually active'},
        'MANUALLY_INACTIVE': {'code': 4, 'desc': 'Manually inactive'}
    }

    def __init__(self, conf, log_is_enabled=False):
        self.dbname = conf.get('dbname')
        self.dbhost = conf.get('dbhost')
        self.dbport = conf.get('dbport')
        self.dbuser = conf.get('dbuser')
        self.dbpass = conf.get('dbpass')
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
        if '.' in table:
            table = table.split('.').pop()
        self.dbopen()
        self.cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (self.schema, table))
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


class PgSQLStoreImport(PgSQLStore):

    def getEmployers(self, employer_name):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=None, where='name=%s', data=(employer_name,))
        return employers

    def getEmployerById(self, employer_id):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=None, where='id=%s', data=(employer_id,))
        if employers is not None:
            return employers[0]
        return None

    def getEmployerFeedSetting(self, employer_id):
        self.employer_id = employer_id
        table = '.'.join([self.schema, self.employer_feed_settings_table])
        employer_feed_settings = self._get(table, field_list=None, where='employer_id=%s', data=(employer_id,))
        if len(employer_feed_settings) > 0:
            return employer_feed_settings[0]
        else:
            return None

    def createTemporaryTable(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        self.dropTemporaryTable(temporaryTable)
        self.cur.execute(' '.join(['CREATE TABLE IF NOT EXISTS', temp_table, 'AS SELECT * FROM ', jobs_table, 'LIMIT 1;']))
        self.cur.execute(' '.join(['DELETE FROM', temp_table, ';']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN created_at SET DEFAULT now()']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN updated_at SET DEFAULT now()']))

    def insertItemToTemporaryTable(self, item, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        #table = '.'.join([self.schema, self.jobs_table])
        sql = ' '.join(['INSERT INTO', table, '(', ','.join(item.keys()), ') VALUES (', ','.join(['%s' for i in item.keys()]), ');'])
        self.cur.execute(sql, item.values())

    def mergeTables(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        pkey = self.pkey

        #adding new
        fld_list = 'uid,external_id,external_unique_id,employer_id,job_group_id,url,title,city,state,country,description,job_type,company,category,posted_at,expire_date,status,budget,budget_spent'.split(',')
        sql = ' '.join(['INSERT INTO', jobs_table, '(', ','.join(fld_list), ') SELECT', ','.join(fld_list), 'FROM', temp_table, 'WHERE', pkey, 'IN (SELECT', pkey, 'FROM', temp_table, 'EXCEPT SELECT', pkey, 'FROM', jobs_table, ');'])
        self.cur.execute(sql)

        #marking deleted as archive
        sql = ' '.join(['UPDATE', jobs_table, "SET status=", str(self.job_status['EXPIRED']['code']), "WHERE", pkey, 'IN (SELECT', pkey, 'FROM', jobs_table, 'EXCEPT SELECT', pkey, 'FROM', temp_table, ') AND employer_id=', str(self.employer_id), ';'])
        self.cur.execute(sql)

        #updating job that already exists as expired
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,updated_at,status'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.',pkey, ')']))
        sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM',jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')','AND employer_id=', str(self.employer_id), 'AND status=', str(self.job_status['EXPIRED']['code']), ';'])
        # 'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey) AND jobs_table.status=2'
        self.cur.execute(sql)

        #updating existing
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,updated_at'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.', pkey, ')']))
        sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM', jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', 'AND employer_id=', str(self.employer_id), ';'])
        #'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey)'
        self.cur.execute(sql)

    def dropTemporaryTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        self.cur.execute(' '.join(['DROP TABLE IF EXISTS', table]))



pg = PgSQLStoreImport(tools)

pprint(pg.getEmployerById(6))
