#!/usr/bin/env python
#coding=utf-8

###############################################
## Copy part of data from one database to other
###############################################
import psycopg2
import logging
from pprint import pprint
import json
import sys
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
            #print sql
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

    def _count_rows(self, table, query=False):
        sql = ' '.join(['SELECT count(*) AS count FROM', table])
        if query is True:
            sql = ' '.join(['SELECT count(*) AS count FROM (', table, ') t'])
        res = self._getraw(sql, ['count'])
        return int(res[0]['count'])

    def _clear_table(self, table):
        sql = ' '.join(['DELETE FROM', table])
        self.dbopen()
        try:
            self.cur.execute(sql)
        except psycopg2.Error, ex:
            self.conn.rollback()
            self.dbclose()
            raise ex
        else:
            self.conn.commit()
            self.dbclose()

    def _get_tables_list(self):
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type='BASE TABLE'"
        res = self._getraw(sql, ['table_name'])
        return map(lambda i: i['table_name'], res)

    def _insert(self, table, data):
        if type(data) is not list:
            raise Exception('Type of data must be list!')
        self.dbopen()
        #self.cur.execute(' '.join(["SELECT setval('", table+'_id_seq', "', (SELECT max(id) FROM", table, '));']))
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


class Datacopy():
    MAX_ROWS = 500
    src = None
    dest = None
    logger = None
    exclude_tables = ['schema_migrations', 'ar_internal_metadata', 'access_token', 'refresh_token', 'client', 'subscriptionterm', 'subscription', 'adnet','files_migration']
    exclude_seq = ['extendedaccesstoken', 'files_migration']

    def __init__(self, src=None, dest=None):
        if src is None or dest is None:
            raise Exception('source or destination db fail!')
        self.src = PgSQLStore(src)
        self.dest = PgSQLStore(dest)
        self.logger = logging.getLogger('Datacopy')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(1)
        self.logger.info('RUN datacopy2')


    def copy(self, table, where, max_rows=None):
        self.logger.info('...run copy from %s with WHERE="%s"' % (table, where))
        if max_rows is not None:
            self.MAX_ROWS = max_rows
        sql = ' '.join(['SELECT * FROM', table, 'WHERE', where])
        count = self.src._count_rows(sql, query=True)
        if count == 0:
            return
        offsets = range(0, count, self.MAX_ROWS)
        self.logger.info('Copy %i rows from table "%s"' % (count, table))
        agree = raw_input("Your agree?(y/n) ")
        if agree.lower() != 'y':
            print 'Operation was  interrupted by user!'
            return
        try:
            for offset in offsets:
                self.logger.info('...fetch rows  %i - %i from table "%s"' % (offset, min(count, offset + self.MAX_ROWS - 1), table))
                data = self.src._get(table, field_list=None, tail="WHERE "+where+" ORDER BY id OFFSET %s LIMIT %s", data=[offset, self.MAX_ROWS])
                self.logger.info('...fetched %i rows' % len(data))
                res = self.dest._insert(table, data)
                if res['result']:
                    self.logger.info('...data inserted')
                else:
                    raise psycopg2.Error('Error while inserting data: %s' % str(res['error']))
        except psycopg2.Error, ex:
            self.logger.info('...Error: %s' % ex)
        else:
            sql = "select setval('%s_id_seq', (select max(id) from %s))" % (table, table)
            self.logger.info("Setting '%s_id_seq'" % table)
            self.dest._exec(sql)
        self.logger.info('END datacopy2')


datacopy = Datacopy(dev, prod)
datacopy.copy('stats', "job_board_id=16 and employer_id in (99,101) and user_token<>'facebook:stat' ")








