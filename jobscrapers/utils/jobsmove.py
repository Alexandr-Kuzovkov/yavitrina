#!/usr/bin/env python
#coding=utf-8

########################################################
## Move non-clients jobs from prod database to old_jobs
## Before copy data from prod database to old_jobs
## datadase all tables in old_jobs will cleared
########################################################

import psycopg2
import logging
import json
from dbacc import *
import inspect


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

    def _count_rows_where(self, table, where):
        sql = ' '.join(['SELECT count(*) AS count FROM', table, 'WHERE', where])
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

    def _clear_table_where(self, table, where):
        sql = ' '.join(['DELETE FROM', table, 'WHERE', where])
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


class JobsMove():
    MAX_ROWS = 500
    src = None
    dest = None
    logger = None
    #list of employers that need keep in prod database
    employers_query = '''SELECT id FROM employers WHERE metadata::json->>'spider' ISNULL AND id<>6 AND id <> 9 AND id<> 28'''

    def __init__(self, src=None, dest=None):
        if src is None or dest is None:
            raise Exception('source or destination db fail!')
        self.src = PgSQLStore(src)
        self.dest = PgSQLStore(dest)
        self.logger = logging.getLogger('JobsMove')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(1)
        self.logger.info('RUN JobsMove')

    def delete_candidates(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        try:
            where = 'employer_id NOT IN (%s)' % self.employers_query
            self.src.dbopen()
            self.src.cur.execute(''.join(['DELETE FROM candidates WHERE ', where]))
            self.src.conn.commit()
            where = 'job_id IN (SELECT id FROM jobs WHERE employer_id NOT IN (%s))' % self.employers_query
            self.src.cur.execute(''.join(['DELETE FROM candidates WHERE ', where]))
            self.src.conn.commit()
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])
        self.src.dbclose()

    def delete_stats(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        try:
            where = 'duplicate_id IN (SELECT id FROM stats WHERE employer_id NOT IN (%s))' % (self.employers_query,)
            self.src.dbopen()
            self.src.cur.execute(''.join(['UPDATE stats SET duplicate=FALSE, duplicate_id=NULL WHERE ', where]))
            self.src.conn.commit()
            where = 'employer_id NOT IN (%s)' % self.employers_query
            self.src.cur.execute(''.join(['DELETE FROM stats WHERE ', where]))
            self.src.conn.commit()
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])
        self.src.dbclose()

    def delete_jobs(self):
        self.delete_stats()
        self.delete_candidates()
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        table = 'jobs'
        try:
            where = 'id not in (SELECT id FROM jobs WHERE employer_id IN (%s))' % self.employers_query
            self.src._clear_table_where(table, where)
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])

    def delete_employers(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        table = 'employers'
        try:
            where = 'id NOT IN (%s)' % self.employers_query
            self.src._clear_table_where(table, where)
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
           self.logger.info('...%s done' % inspect.stack()[0][3])

    def delete_employer_feed_settings(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        table = 'employer_feed_settings'
        try:
            where = 'employer_id NOT IN (%s)' % self.employers_query
            self.src._clear_table_where(table, where)
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])

    def delete_job_groups(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        table = 'job_groups'
        try:
            where = 'employer_id NOT IN (%s)' % self.employers_query
            self.src._clear_table_where(table, where)
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])


    def delete_employer_job_boards(self):
        self.logger.info('...Run %s...' % inspect.stack()[0][3])
        table = 'employer_job_boards'
        try:
            where = 'employer_id NOT IN (%s)' % self.employers_query
            self.src._clear_table_where(table, where)
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('...%s done' % inspect.stack()[0][3])

    def clear_table(self, table):
        self.logger.info('...clear table "%s"' % table)
        self.dest._clear_table(table)
        self.logger.info('...done "%s"' % table)

    def run(self, max_rows=None):
        self.logger.info('...run copy')
        if max_rows is not None:
            self.MAX_ROWS = max_rows
        self.clear_table('jobs')
        self.clear_table('job_groups')
        self.clear_table('employer_feed_settings')
        self.clear_table('employers')
        self.copy_table('employers', 'TRUE')
        where = 'employer_id NOT IN (%s)' % self.employers_query
        self.copy_table('employer_feed_settings', where)
        self.copy_table('job_groups', 'TRUE')
        where = 'id NOT IN (SELECT id FROM jobs WHERE employer_id IN (%s))' % self.employers_query
        self.copy_table('jobs', where)
        self.logger.info('END datacopy')
        self.delete_jobs()
        self.delete_employer_feed_settings()
        self.delete_job_groups()
        self.delete_employer_job_boards()
        self.delete_employers()
        self.logger.info('END JobsMove')

    def copy_table(self, table, where):
        count = self.src._count_rows_where(table, where)
        if count == 0:
            return
        offsets = range(0, count, self.MAX_ROWS)
        self.logger.info('Copy table "%s" with %i rows' % (table, count))
        try:
            for offset in offsets:
                self.logger.info('...fetch rows  %i - %i from table "%s"' % (offset, min(count, offset + self.MAX_ROWS - 1), table))
                fld_list = self.src._get_fld_list(table)
                sql = ' '.join(['SELECT', ','.join(fld_list), 'FROM', table, 'WHERE', where, 'ORDER BY id OFFSET %s LIMIT %s'])
                data = self.src._getraw(sql, field_list=fld_list, data=[offset, self.MAX_ROWS])
                self.logger.info('...fetched %i rows' % len(data))
                res = self.dest._insert(table, data)
                if res['result']:
                    self.logger.info('...data inserted')
                else:
                    raise psycopg2.Error('Error while inserting data: %s' % str(res['error']))
        except psycopg2.Error, ex:
            self.logger.error('ERROR: %s' % ex.pgerror)
        else:
            self.logger.info('END copy table "%s"' % table)


jobsmove = JobsMove(copy_prod_local, old_jobs_local)
jobsmove.run()
