#coding=utf-8

import sqlite3
import time
import os
from mylogger import logger2 as logger
import re


class SqLite:

    db_file = '/data/data.sqlite'
    conn = None
    cur = None

    def __init__(self, queries):
        self._dbopen()
        for query in queries:
            self.cur.execute(query)
        self.conn.commit()



    def _dbopen(self):
        if self.conn is None:
            try:
                self.conn = sqlite3.connect(self.db_file)
            except Exception as ex:
                raise Exception(self.__class__ + 'Can\'t open database file!')
                exit()

        if self.cur is None and self.conn is not None:
            self.cur = self.conn.cursor()


    def _dbclose(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    '''
    returning the query
    @param query query string with? at the insertion point
    @param data data tuple
    '''
    def _query(self, query, data=None, close=False):
        self._dbopen()
        success = True
        try:
            if data is not None:
                self.cur.execute(query, data)
            else:
                self.cur.execute(query)
            res = self.cur.fetchall()
        except sqlite3.OperationalError as ex:
            logger.error(str(ex))
            success = False
        finally:
            if close:
                self._dbclose()
        if success:
            return res
        else:
            return success

    '''
    query execution without returning result
    @param query query string with? at the insertion point
    @param data data tuple
    '''
    def _execute(self, query, data=None, close=False):
        self._dbopen()
        success = True
        try:
            if data is not None:
                self.cur.execute(query, data)
            else:
                self.cur.execute(query)
            self.conn.commit()
        except sqlite3.OperationalError as ex:
            success = False
            logger.error(str(ex))
        finally:
            if close:
                self._dbclose()
        return success


    '''
    insert data into a table
    @param table table name
    @param data_rows list of data dict
    '''
    def _insert(self, table, data, close=False):
        self._dbopen()
        success = True
        try:
            if len(data) == 0:
                raise Exception('Data list is empty!')
            for row in data:
                query = ' '.join(['INSERT INTO', table, '(', ','.join(row.keys()), ') VALUES (', ','.join(['?' for i in row.keys()]), ');'])
                self.cur.execute(query, list(row.values()))
            self.conn.commit()
        except Exception as ex:
            logger.error(str(ex))
            success = False
            return success
        finally:
            if close:
                self._dbclose()
        return success


    '''
    getting data from a table
    @param table table name
    @param conditions a list of dictionaries with query terms (what’s after where)
    '''
    def _get(self, table, conditions=None, close=False):
        self._dbopen()
        success = True
        q = ['SELECT * FROM', table, 'WHERE']
        tail = []
        cond_data = []
        if conditions is not None:
            if type(conditions) is list:
                for condition in conditions:
                    key, val = condition.items()[0]
                    tail.append(''.join([key, '?']))
                    cond_data.append(val)
            elif type(conditions) is dict:
                for key, val in conditions.items():
                    tail.append(''.join([key, '?']))
                    cond_data.append(val)
            q.append(' AND '.join(tail))
        else:
            q.append('1')
        query = ' '.join(q)
        #print(query)
        try:
            self.cur.execute(query, cond_data)
            data = self.cur.fetchall()
            field_list = self._get_fld_list(table)
            res = []
            for row in data:
                d = {}
                for i in range(len(row)):
                    d[field_list[i]] = row[i]
                res.append(d)
        except sqlite3.OperationalError as ex:
            logger.error(str(ex))
            success = False
        finally:
            if close:
                self._dbclose()
        if success:
            return res
        else:
            return None

    '''
        getting data from a table
        @param table table name
        @param conditions a list of dictionaries with query terms (what’s after where)
        '''

    def _getraw(self, sql, field_list,  data=None, close=False):
        self._dbopen()
        success = True
        try:
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
                d = {}
                for i in range(len(row)):
                    d[field_list[i]] = row[i]
                res.append(d)
        except sqlite3.OperationalError as ex:
            logger.error(str(ex))
            success = False
        finally:
            if close:
                self._dbclose()
        if success:
            return res
        else:
            return None

    '''
    deleting data from a table
    @param table table name
    @param conditions a list of dictionaries with query terms (what’s after where)
    '''
    def _delete(self, table, conditions=None, close=False):
        self._dbopen()
        success = True
        q = ['DELETE FROM', table, 'WHERE']
        tail = []
        data = []
        if conditions is not None:
            if type(conditions) is list:
                for condition in conditions:
                    key, val = condition.items()[0]
                    tail.append(''.join([key, '?']))
                    data.append(val)
            elif type(conditions) is dict:
                for key, val in conditions.items():
                    tail.append(''.join([key, '?']))
                    data.append(val)
            q.append(' AND '.join(tail))
        else:
            q.append('1')
        query = ' '.join(q)
        #print query
        try:
            self.cur.execute(query, data)
            self.conn.commit()
        except sqlite3.OperationalError as ex:
            logger.error(str(ex))
            success = False
        finally:
            if close:
                self._dbclose()
        return success


    '''
    update data in the table
    @param table table name
    @param data dictionary with data {field_name: value}
    @param conditions a list of dictionaries with query terms (what’s after where)
    '''
    def _update(self, table, data, conditions=None, close=False):
        self._dbopen()
        success = True
        values = data.values()
        sets = []
        for fld, val in data.items():
            sets.append(''.join([fld, '=?']))
        sets = ','.join(sets)
        q = ['UPDATE', table, 'SET', sets, 'WHERE']
        tail = []
        cond_data = []
        if conditions is not None:
            if type(conditions) is list:
                for condition in conditions:
                    key, val = condition.items()[0]
                    tail.append(''.join([key, '?']))
                    cond_data.append(val)
            elif type(conditions) is dict:
                for key, val in conditions.items():
                    tail.append(''.join([key, '?']))
                    cond_data.append(val)
            q.append(' AND '.join(tail))
        else:
            q.append('1')
        query = ' '.join(q)
        #print query
        try:
            self.cur.execute(query, list(values) + cond_data)
            self.conn.commit()
        except sqlite3.OperationalError as ex:
            success = False
            logger.error(str(ex))
        finally:
            if close:
                self._dbclose()
        return success

    def _get_fld_list(self, table):
        sql = "PRAGMA table_info({table})".format(table=table)
        res = self._query(sql)
        if res:
            return list(map(lambda i: i[1], res))
        return None


class CronJob(SqLite):

    table_name = 'jobs'
    table_name2 = 'errors'
    create_table_sql ='''CREATE TABLE IF NOT EXISTS {table}
                      (
                        slug PRIMARY KEY,
                        m TEXT NOT NULL,
                        h TEXT NOT NULL,
                        dom TEXT NOT NULL,
                        mon TEXT NOT NULL,
                        dow TEXT NOT NULL,
                        description TEXT DEFAULT 'Job description',
                        start_time DEFAULT NULL,
                        max_execute_time INTEGER DEFAULT 300,
                        last_ping DEFAULT CURRENT_TIMESTAMP NOT NULL,
                        enabled INTEGER DEFAULT 1,
                        last_notification DEFAULT CURRENT_TIMESTAMP NOT NULL
                       )'''.format(table=table_name)

    create_table_sql2 = '''CREATE TABLE IF NOT EXISTS {table}
                          (
                            id INTEGER PRIMARY KEY ASC,
                            slug TEXT NOT NULL,
                            job_description TEXT DEFAULT 'Job description',
                            error_message TEXT NOT NULL,
                            created_at DEFAULT CURRENT_TIMESTAMP NOT NULL,
                            notification_sended INTEGER DEFAULT 0
                           )'''.format(table=table_name2)

    def __init__(self):
        super(self.__class__, self).__init__([self.create_table_sql, self.create_table_sql2])


    def get_job(self, slug=None):
        cond = None
        if slug is not None:
            cond = {'slug=': slug}
        jobs = self._get(self.table_name, cond)
        return jobs

    def update_job(self, job):
        fld_lst = self._get_fld_list('jobs')
        if type(job) is dict:
            for key in job.keys():
                if key not in fld_lst:
                    raise Exception('Field {fld} is not allowed!'.format(fld=key))
            return self._update('jobs', job, {'slug=': job['slug']})
        else:
            raise ('Type of job must be dict!')

    def delete_job(self, slug):
        return self._delete('jobs', {'slug=': slug})

    def new_job(self, job):
        fld_lst = self._get_fld_list('jobs')
        if type(job) is dict:
            for key in job.keys():
                if key not in fld_lst:
                    raise Exception('Field {fld} is not allowed!'.format(fld=key))
            return self._insert('jobs', [job])
        else:
            raise ('Type of job must be dict!')

    def new_error(self, error):
        fld_lst = self._get_fld_list('errors')
        if type(error) is dict:
            for key in error.keys():
                if key not in fld_lst:
                    raise Exception('Field {fld} is not allowed!'.format(fld=key))
            return self._insert('errors', [error])
        else:
            raise ('Type of job must be dict!')

    def get_errors(self):
        fld_list = self._get_fld_list(self.table_name2)
        return self._getraw("SELECT * FROM {table} ORDER BY created_at".format(table=self.table_name2), fld_list)

    def delete_error(self, id):
        id = int(id.strip())
        return self._delete(self.table_name2, {'id=': id})




def test():
    cj = CronJob()
    #print(cj.new_job({'slug': ''.join(['command', str(int(time.time()))]), 'max_interval': 3600}))
    #print(cj._get('jobs'))
    #print(cj._getraw("select strftime('%s','now') - strftime('%s',last_run) as diff from jobs", ['diff']))
    #print(cj.delete_job('command1'))
    #print(cj.update_job({'slug': 'command1550087789', 'description': 'Job description2', 'enabled': 0}))
    #print(cj.get_job())
    print(cj.get_job('command1550087789'))


def get_jobs(slug=None):
    cj = CronJob()
    return cj.get_job(slug)

def update_job(slug, formdata):
    cj = CronJob()
    jobs = cj.get_job(slug)
    if len(jobs) > 0:
        job = jobs[0]
        for key,val in job.items():
            if key in formdata:
                job[key] = formdata[key]
        if job['enabled'] == 'on':
            job['enabled'] = 1
        job['max_execute_time'] = int(float(job['max_execute_time']))
        job['slug'] = job['slug'].strip()
        job = fix_formdata(job)
        cj.update_job(job)

def delete_job(slug):
    cj = CronJob()
    return cj.delete_job(slug)

def create_job(job):
    cj = CronJob()
    if job['enabled'] == 'on':
        job['enabled'] = 1
    job['max_execute_time'] = int(float(job['max_execute_time']))
    job['slug'] = job['slug'].strip()
    job['last_notification'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
    return cj.new_job(job)

def pingjob(job):
    cj = CronJob()
    jobs = cj.get_job(job['slug'])
    if len(jobs) > 0:
        job = jobs[0]
        #print(job)
        job['last_ping'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        job['start_time'] = None
        return cj.update_job(job)
    return False

def get_job_list():
    cj = CronJob()
    sql = "SELECT j.*, (strftime('%s','now') - strftime('%s',last_ping)) AS diff, (CASE WHEN  start_time NOT NULL AND ABS(strftime('%s','now') - strftime('%s',start_time)) > max_execute_time THEN 1 ELSE 0 END) AS warning FROM jobs j"
    fld_lst = cj._get_fld_list(cj.table_name)
    fld_lst.append('diff')
    fld_lst.append('warning')
    return cj._getraw(sql, fld_lst)

def get_died_jobs():
    cj = CronJob()
    sql = '''SELECT j.*, (strftime('%s','now') - strftime('%s',last_ping)) AS diff, (CASE WHEN  start_time NOT NULL AND (strftime('%s','now') - strftime('%s',start_time)) > max_execute_time THEN 1 ELSE 0 END) AS warning FROM jobs j
            WHERE enabled=1 AND start_time NOT NULL AND (strftime('%s','now') - strftime('%s',start_time)) > max_execute_time'''
    fld_lst = cj._get_fld_list(cj.table_name)
    fld_lst.append('diff')
    fld_lst.append('warning')
    return cj._getraw(sql, fld_lst)

def get_died_jobs_for_notify():
    cj = CronJob()
    sql = '''SELECT j.*, (strftime('%s','now') - strftime('%s',last_ping)) AS diff, (CASE WHEN  start_time NOT NULL AND (strftime('%s','now') - strftime('%s',start_time)) > max_execute_time THEN 1 ELSE 0 END) AS warning FROM jobs j
                WHERE enabled=1 AND start_time NOT NULL AND (strftime('%s','now') - strftime('%s',start_time)) > max_execute_time'''
    fld_lst = cj._get_fld_list(cj.table_name)
    fld_lst.append('diff')
    fld_lst.append('warning')
    return cj._getraw(sql, fld_lst)

def new_job_error(job, error_message):
    cj = CronJob()
    error = {}
    error['slug'] = job['slug']
    error['job_description'] = job['description']
    error['error_message'] = error_message.strip()
    return cj.new_error(error)

def get_errors():
    cj = CronJob()
    return cj.get_errors()

def delete_error(id):
    cj = CronJob()
    return cj.delete_error(id)

def get_errors_for_notify():
    cj = CronJob()
    fld_lst = cj._get_fld_list(cj.table_name2)
    sql = "SELECT * FROM errors WHERE notification_sended = 0"
    return cj._getraw(sql, fld_lst)

def set_notification_flag(error):
    cj = CronJob()
    return cj._update(cj.table_name2, {'notification_sended': 1}, {'id=': error['id']})

def get_jobs_fld_list():
    cj = CronJob()
    return cj._get_fld_list(cj.table_name)

def fix_formdata(formdata):
    schedule_fld = 'm,h,dom,mon,dow'.split(',')
    for fld, val in formdata.items():
        if fld not in schedule_fld:
            continue
        if not re.match('^[1234567890\/\*]+$', val):
            formdata[fld] = '*'
    return formdata



#test()





