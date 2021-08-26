import psycopg2
import json
import sqlite3
import logging
from ConfigParser import *
import os
import time

class Geoname:

    conn = None
    cur = None
    dbname1 = '/home/user1/geoname.sqlite'
    dbname2 = '/home/ubuntu/geoname.sqlite'
    dbname3 = '/home/root/geoname.sqlite'
    cache = {} #cache for mapping city_name -> region, department
    country_code = 'FR'
    city = None
    cache2 = {}  # cache for mapping country_code -> country_info


    def __init__(self, country_code='FR'):
        self._dbopen()
        self.country_code = country_code
        self.cur.execute('CREATE TABLE IF NOT EXISTS cache(city PRIMARY KEY, data)')
        self.loadCache()

    def _dbopen(self):
        if self.conn is None:
            try:
                self.conn = sqlite3.connect(self.dbname1)
            except Exception, ex:
                try:
                    self.conn = sqlite3.connect(self.dbname2)
                except Exception, ex:
                    try:
                        self.conn = sqlite3.connect(self.dbname3)
                    except Exception, ex:

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

    def _get(self, table, field_list, where='', data=None, dbclose=True):
        self._dbopen()
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
        if dbclose:
            self._dbclose()
        if len(res) > 0:
            return res
        else:
            return None

    def saveCache(self, dbclose=True):
        self._dbopen()
        for city, data in self.cache.items():
            try:
                self.cur.execute('INSERT OR REPLACE INTO cache (city , data) VALUES (?,?)', [city, json.dumps(data)])
            except Exception, ex:
                city = unicode(city, 'utf-8')
                self.cur.execute('INSERT OR REPLACE INTO cache (city , data) VALUES (?,?)', [city, json.dumps(data)])
        self.conn.commit()
        if dbclose:
            self._dbclose()

    def loadCache(self, dbclose=True):
        self._dbopen()
        self.cur.execute('SELECT city, data FROM cache')
        res = self.cur.fetchall()
        if res is not None:
            for row in res:
                city = row[0]
                self.cache[city] = json.loads(row[1])
        if dbclose:
            self._dbclose()

    def push_cache(self, data):
        self.cache[self.city] = data

    def pop_cache(self):
        return self.cache.get(self.city, None)

    def _searchParent(self, res):
        res2 = self._get('admin1CodesASCII', ['name', 'name_ascii'],
                         "code LIKE '%s.%i%%'" % (self.country_code, int(res[0]['admin1_code'])))
        if res2 is not None:
            res3 = self._get('admin2Codes', ['name', 'name_ascii'], "concatenated_codes LIKE '%s.%i.%i%%'" % (
            self.country_code, int(res[0]['admin1_code']), int(res[0]['admin2_code'])))
            if res3 is not None:
                result = {'subdiv1': res2[0], 'subdiv2': res3[0]}
                self.push_cache(result)
                return result
        return None

    def _searchParent2(self, res):
        res2 = self._get('admin1CodesASCII', ['name', 'name_ascii'],
                         "code LIKE '%s.%i%%'" % (self.country_code, int(res[0]['concatenated_codes'].split('.')[1])))
        if res2 is not None:
            result = {'subdiv1': res2[0], 'subdiv2': res[0]}
            self.push_cache(result)
            return result
        return None

    def city2location(self, city):
        city = city.replace(' ', '')
        self.city = city
        result  = self.pop_cache()
        if result is not None:
            return result
        #search from cities
        for table in ['cities1000', 'cities5000', 'cities15000']:
            try:
                res = self._get(table, ['admin1_code', 'admin2_code'], 'country_code=? AND name=?', [self.country_code, city])
            except Exception, ex:
                city = unicode(city, 'utf-8')
                res = self._get(table, ['admin1_code', 'admin2_code'], 'country_code=? AND name=?', [self.country_code, city])
            if res is not None:
                return self._searchParent(res)
        #serach in departments
        res = self._get('admin2Codes', ['concatenated_codes', 'name', 'name_ascii'], "concatenated_codes LIKE '%s%%' AND name LIKE '%% %s'" % (self.country_code, city))
        if res is not None:
            return self._searchParent2(res)
        res = self._get('admin2Codes', ['concatenated_codes', 'name', 'name_ascii'], "concatenated_codes LIKE '%s%%' AND name LIKE '%%-%s'" % (self.country_code, city))
        if res is not None:
            return self._searchParent2(res)
        res = self._get('admin2Codes', ['concatenated_codes', 'name', 'name_ascii'], "concatenated_codes LIKE '%s%%' AND name LIKE '%%%s'" % (self.country_code, city))
        if res is not None:
            return self._searchParent2(res)
        return None

    def isocode2countryinfo(self, code):
        code = code.strip().upper()
        if code in self.cache2:
            return self.cache2[code]
        fld_list = 'ISO,ISO3,ISO_Numeric,fips,Country,Capital,Area,Population,Continent'.split(',')
        countryinfo = self._get('countryInfo', fld_list, 'ISO=? OR ISO3=? OR ISO_Numeric=?', [code, code, code], False)
        if countryinfo is not None:
            self.cache2[code] = countryinfo[0]
            return countryinfo[0]
        else:
            return None



class PgSQLStore(object):
    settings = None
    conn = None
    cur = None

    #schema = 'jobs'
    schema = 'public'
    jobs_table = 'jobs'
    employers_table = 'employers'
    employer_feed_settings_table = 'employer_feed_settings'
    job_boards_table = 'job_boards'
    job_board_feed_settings_table = 'job_board_feed_settings'
    employer_job_boards_table = 'employer_job_boards'

    dbname = None
    dbhost = None
    dbport = None
    dbuser = None
    dbpass = None

    employer_id = None

    job_status = {
        'INACTIVE': {'code': 0, 'desc': 'Inactive'},
        'ACTIVE': {'code': 1, 'desc': 'Active'},
        'EXPIRED': {'code': 2, 'desc': 'Expired'},
        'MANUALLY_ACTIVE': {'code': 3, 'desc': 'Manually active'},
        'MANUALLY_INACTIVE': {'code': 4, 'desc': 'Manually inactive'}
    }

    def __init__(self, config_file=None, log_is_enabled=False):
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

        self.dbname3 = self.conf.get('slave_prod', 'dbname')
        self.dbhost3 = self.conf.get('slave_prod', 'dbhost')
        self.dbport3 = self.conf.get('slave_prod', 'dbport')
        self.dbuser3 = self.conf.get('slave_prod', 'dbuser')
        self.dbpass3 = self.conf.get('slave_prod', 'dbpass')

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

    def dbopen_slave(self):
        if self.conn is not None:
            self.dbclose()
        self.conn = psycopg2.connect(dbname=self.dbname3, user=self.dbuser3, password=self.dbpass3, host=self.dbhost3, port=self.dbport3)
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

    def _setdb(self, db=None):
        self.dbname_old = self.dbname
        self.dbhost_old = self.dbhost
        self.dbport_old = self.dbport
        self.dbuser_old = self.dbuser
        self.dbpass_old = self.dbpass
        self.dbname = self.dbname2
        self.dbhost = self.dbhost2
        self.dbport = self.dbport2
        self.dbuser = self.dbuser2
        self.dbpass = self.dbpass2

    def _resetdb(self):
        self.dbname = self.dbname_old
        self.dbhost = self.dbhost_old
        self.dbport = self.dbport_old
        self.dbuser = self.dbuser_old
        self.dbpass = self.dbpass_old

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


class PgSQLStoreFeedExport(PgSQLStore):

    cacheCpc = {}
    cpc1 = None
    cpc2 = None
    job_job_groups = {}
    active_jobs = {}
    jobs = []
    employers_logo_cache = None

    def __init__(self, config_file=None, log_is_enabled=False):
        super(self.__class__, self).__init__(config_file=None, log_is_enabled=False)
        self.fld_lst_jobs = self._get_fld_list('jobs')

    def getJobsOld(self, employers_id, fld_list=None):
        if type(employers_id) is not list or len(employers_id) == 0:
            return []
        table = '.'.join([self.schema, self.jobs_table])
        where = ''.join(['employer_id IN (', ','.join(map(lambda i: str(i), employers_id)),') AND status IN (', str(self.job_status['ACTIVE']['code']), ',', str(self.job_status['MANUALLY_ACTIVE']['code']), ')'])
        jobs = self._get(table, field_list=fld_list, where=where)
        if jobs is None:
            return []
        return jobs

    def getJobs(self, board_id, limit=1000000, offset=0):
        #based on employer_bod_boards and job_group_job_boards
        sql = '''SELECT * FROM
                  (SELECT *
                   FROM jobs
                   WHERE
                     jobs.employer_id in (
                       SELECT employer_id
                        FROM employer_job_boards INNER JOIN employers ON employer_job_boards.employer_id = employers.id
                        WHERE
                          employer_job_boards.enabled is TRUE
                          AND employers.enabled is TRUE
                          AND job_board_id = %s GROUP BY employer_id)
                    AND status IN (1,3)
                    AND jobs.job_group_id ISNULL
                UNION
                SELECT j.*
                  FROM jobs j INNER JOIN employers e ON j.employer_id=e.id
                      WHERE e.enabled is TRUE
                            AND j.job_group_id in (
                                SELECT job_group_id
                                  FROM job_group_job_boards INNER JOIN job_groups ON  job_group_job_boards.job_group_id = job_groups.id
                                  WHERE  job_group_job_boards.enabled is TRUE
                                      AND job_groups.enabled is TRUE
                                      AND job_board_id = %s GROUP BY job_group_id)
                      AND status IN (1,3)) t ORDER BY created_at'''

        sql = 'select j.* from jobs j inner join v_jobs_for_feeds v on j.id=v.job_id where v.job_board_id=%s'

        sql = ' '.join([sql, 'LIMIT', str(limit), 'OFFSET', str(offset)])

        self.dbopen_slave()
        fld_list = self._get_fld_list('jobs')
        self.jobs = self._getraw(sql, field_list=fld_list, data=[board_id])
        self.dbclose()
        return self.jobs

    def countJobs(self, board_id):
        sql = '''SELECT count(*) AS count FROM
                  (SELECT *
                   FROM jobs
                   WHERE
                     jobs.employer_id in (
                       SELECT employer_id
                       FROM employer_job_boards INNER JOIN employers ON employer_job_boards.employer_id = employers.id
                       WHERE
                         employer_job_boards.enabled is TRUE
                         AND employers.enabled is TRUE
                         AND job_board_id = %s GROUP BY employer_id)
                     AND status IN (1,3)
                     AND jobs.job_group_id ISNULL
                   UNION
                   SELECT j.*
                   FROM jobs j INNER JOIN employers e ON j.employer_id=e.id
                   WHERE e.enabled is TRUE
                         AND j.job_group_id in (
                     SELECT job_group_id
                     FROM job_group_job_boards INNER JOIN job_groups ON  job_group_job_boards.job_group_id = job_groups.id
                     WHERE  job_group_job_boards.enabled is TRUE
                            AND job_groups.enabled is TRUE
                            AND job_board_id = %s GROUP BY job_group_id)
                         AND status IN (1,3)) t'''

        sql = 'select count(j.id) as count from jobs j inner join v_jobs_for_feeds v on j.id=v.job_id where v.job_board_id=%s'
        fld_list = ['count']
        self.dbopen_slave()
        res = self._getraw(sql, field_list=fld_list, data=[board_id])
        self.dbclose()
        if len(res) > 0 and 'count' in res[0]:
            return res[0]['count']
        return None

    #check which jobs for feed are present in jobs_`actions
    def checkHistory(self, jobs, board_id):
        if len(jobs) == 0:
            return False
        sql = '''SELECT id, job_id, job_board_id, created_at, runtime, deleted_at
                  FROM jobs_actions
                  WHERE job_board_id=%s
                    AND deleted_at ISNULL
                    AND runtime=(
                      SELECT max(runtime)
                        FROM jobs_actions
                        WHERE job_board_id=%s
                        )'''
        res = self._getraw(sql, ['id', 'job_id', 'job_board_id', 'created_at', 'runtime', 'deleted_at'], [board_id, board_id])

        curr_runtime = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        for row in res:
            self.active_jobs[row['job_id']] = row
        runFlag = False
        for job in jobs:
            if job['id'] in self.active_jobs.keys():
                self.active_jobs[job['id']]['runtime'] = curr_runtime
            else:
                runFlag = True
                self.active_jobs[job['id']] = {'id': None, 'job_id': job['id'], 'job_board_id': board_id, 'created_at': curr_runtime, 'runtime': curr_runtime, 'deleted_at': None}
        for job_id, active_job in self.active_jobs.items():
            if self.active_jobs[job_id]['runtime'] != curr_runtime:
                self.active_jobs[job_id]['deleted_at'] = curr_runtime
                runFlag = True
        return runFlag

    #update jobs_actions
    def saveHistory(self):
        self.dbopen()
        table = 'jobs_actions'
        for active_job in self.active_jobs.values():
            if active_job['id'] is None:
                sets = []
                for key, val in active_job.items():
                    sets.append(key + '= %s')
                del active_job['id']
                sql = ' '.join(['INSERT INTO', table, '(', ','.join(active_job.keys()), ') VALUES (', ','.join(['%s' for i in active_job.keys()]), ');'])
                self.cur.execute(sql, active_job.values())
            else:
                sets = ['runtime=%s', 'deleted_at=%s']
                sql = ' '.join(['UPDATE', table, 'SET', ','.join(sets), 'WHERE id=%s', ';'])
                self.cur.execute(sql, [active_job['runtime'], active_job['deleted_at'], active_job['id']])
        self.conn.commit()
        self.dbclose()

    def getBoardById(self, board_id, fld_list=None):
        table = '.'.join([self.schema, self.job_boards_table])
        boards = self._get(table, field_list=fld_list, where='id=%s', data=[int(board_id)])
        if boards is not None:
            return boards[0]
        return None

    def getBoardByName(self, board_name, fld_list=None):
        table = '.'.join([self.schema, self.job_boards_table])
        boards = self._get(table, field_list=fld_list, where='name=%s', data=[board_name])
        if boards is not None:
            return boards[0]
        return None

    def getEmployerIds(self, board_id=None):
        self.dbopen()
        table = '.'.join([self.schema, self.employer_job_boards_table])
        employers_table = '.'.join([self.schema, self.employers_table])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        if board_id is not None:
            sql = ' '.join(
                ['SELECT employer_id FROM', table, 'INNER JOIN', employers_table, 'ON', table, '.employer_id = ',
                 employers_table, '.id WHERE', table, '.enabled is TRUE AND', employers_table,'.enabled is TRUE',
                 'AND',  employers_table, ".name NOT ILIKE '%demo%' AND", employers_table, ".name NOT ILIKE '%test%'", #exclude demo employers from feeds
                 'AND job_board_id = ', str(board_id), 'GROUP BY employer_id'])
        else:
            sql = ' '.join(
                ['SELECT employer_id FROM', jobs_table, 'INNER JOIN', employers_table, 'ON', jobs_table, '.employer_id = ',
                 employers_table, '.id WHERE',
                 employers_table, ".name NOT ILIKE '%demo%' AND", employers_table, ".name NOT ILIKE '%test%' AND", # exclude demo employers from feeds
                 employers_table, '.enabled is TRUE GROUP BY employer_id'])
        self.cur.execute(sql)
        res = self.cur.fetchall()
        ids = []
        if res is not None:
            for row in res:
                ids.append(row[0])
            return ids
        return None

    def getBoardFeedSetting(self, board_id, fld_list=None):
        table = '.'.join([self.schema, self.job_board_feed_settings_table])
        board_feed_settings = self._get(table, field_list=fld_list, where='job_board_id=%s', data=(board_id,))
        if len(board_feed_settings) > 0:
            return board_feed_settings[0]
        else:
            return None
    #
    # def getCpc(self, job_id):
    #     if job_id in self.cacheCpc:
    #         return self.cacheCpc[job_id]
    #     sql = '''SELECT mg.target_cpc as cpc from jobs j INNER JOIN job_groups jg ON j.job_group_id=jg.id INNER JOIN meta_groups mg ON jg.meta_group_id=mg.id where j.id=%s'''
    #     fld_list = ['cpc']
    #     res = self._getraw(sql, field_list=fld_list, data=[job_id])
    #     if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
    #         cpc = float(res[0]['cpc'])
    #         self.cacheCpc[job_id] = cpc
    #         return cpc

    def getCpcOld(self, job_id, board_id):
        return 0.4
        #if job_id in self.cacheCpc:
        #    return self.cacheCpc[job_id]
        sql = '''SELECT mg.target_cpc as cpc from jobs j INNER JOIN job_groups jg ON j.job_group_id=jg.id INNER JOIN meta_groups mg ON jg.meta_group_id=mg.id where j.id=%s'''
        fld_list = ['cpc']
        res = self._getraw(sql, field_list=fld_list, data=[job_id])
        if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
            cpc = float(res[0]['cpc'])
            self.cacheCpc[job_id] = cpc
            return cpc
        else:
            sql = '''SELECT jj.cpc as cpc from jobs j INNER JOIN job_groups jg ON j.job_group_id=jg.id INNER JOIN job_group_job_boards jj ON jg.id=jj.job_group_id where j.id=%s and jj.job_board_id = %s'''
            fld_list = ['cpc']
            res = self._getraw(sql, field_list=fld_list, data=[job_id, board_id])
            cpc = float(res[0]['cpc'])
            self.cacheCpc[job_id] = cpc
            return cpc

    def getCpc(self, job, board_id):
        #if job_id in self.cacheCpc:
        #    return self.cacheCpc[job_id]
        if self.cpc1 is None:
            sql = '''SELECT j.id, mg.target_cpc as cpc from jobs j INNER JOIN job_groups jg ON j.job_group_id=jg.id INNER JOIN meta_groups mg ON jg.meta_group_id=mg.id'''
            fld_list = ['id', 'cpc']
            res = self._getraw(sql, field_list=fld_list, data=None)
            if res is not None and len(res) > 0:
                self.cpc1 = {}
                for row in res:
                    key = row['id']
                    self.cpc1[key] = row

        if job['id'] in self.cpc1:
            return self.cpc1[job['id']]['cpc']

        if self.cpc2 is None:
            sql = 'SELECT jg.id, jj.job_board_id, jj.cpc AS cpc FROM job_groups jg INNER JOIN job_group_job_boards jj ON jg.id=jj.job_group_id WHERE jg.enabled=true AND jj.enabled=true'
            fld_list = ['id', 'job_board_id', 'cpc']
            res = self._getraw(sql, field_list=fld_list, data=None)
            if res is not None and len(res) > 0:
                self.cpc2 = {}
                for row in res:
                    key = '_'.join([str(row['id']), str(row['job_board_id'])])
                    self.cpc2[key] = row
        job_group_id = job['job_group_id']
        key = '_'.join([str(job_group_id), str(board_id)])
        if key in self.cpc2:
            return self.cpc2[key]['cpc']
        return None

    '''
    def getCpc(self, employer_id, board_id):
        key = '_'.join([str(employer_id), str(board_id)])
        if key in self.cacheCpc:
            return self.cacheCpc[key]
        table1 = '.'.join([self.schema, self.employer_job_boards_table])
        table2 = '.'.join([self.schema, self.employers_table])
        fld_list = ['cpc']
        res = self._get(table1, fld_list, 'employer_id=%s AND job_board_id=%s', [employer_id, board_id])
        if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
            cpc = float(res[0]['cpc'])
            self.cacheCpc[key] = cpc
            return cpc
        res = self._get(table2, fld_list, 'id=%s', [employer_id])
        if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
            cpc = float(res[0]['cpc'])
            self.cacheCpc[key] = cpc
            return cpc
        return None
    '''

    '''
    def getCpc(self, employer_id, board_id):
        key = '_'.join([str(employer_id), str(board_id)])
        if key in self.cacheCpc:
            return self.cacheCpc[key]
        table1 = '.'.join([self.schema, self.job_board_feed_settings_table])
        table2 = '.'.join([self.schema, self.employers_table])
        fld_list = ['cpc']
        res = self._get(table1, fld_list, 'job_board_id=%s', [board_id])
        if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
            k = float(res[0]['cpc'])
        else:
            k = 1.0
        res = self._get(table2, fld_list, 'id=%s', [employer_id])
        if res is not None and len(res) > 0 and res[0]['cpc'] is not None:
            cpc = float(res[0]['cpc']) * k
            self.cacheCpc[key] = cpc
            return cpc
        return None
    '''

    def changeFeedUpdated(self, board_id, utime):
        table = '.'.join([self.schema, self.job_boards_table])
        self.dbopen()
        sql = ' '.join(['UPDATE', table, 'SET feed_updated_at=%s', 'WHERE id=%s'])
        self.cur.execute(sql, (utime, board_id))
        self.conn.commit()
        self.dbclose()

    def getEmployerLogo(self, job):
        if self.employers_logo_cache is not None and job['employer_id'] in self.employers_logo_cache:
            return self.employers_logo_cache[job['employer_id']]['logo']
        else:
            table = '.'.join([self.schema, self.employers_table])
            employers = self._get(table, ['id', 'uid'], where='TRUE')
        if len(employers) > 0:
            self.employers_logo_cache = {}
            for employer in employers:
                self.employers_logo_cache[employer['id']] = {'uid': employer['uid'], 'logo': None}
            self._setdb()
            sql = "SELECT profile->>'logo' AS logo, profile->>'uid' AS uid FROM company WHERE profile->>'uid' NOTNULL AND profile->>'logo' NOTNULL"
            companies = self._getraw(sql, ['logo', 'uid'])
            #print companies
            self._resetdb()
            for employer_id, data in self.employers_logo_cache.items():
                for company in companies:
                    if company['uid'] == self.employers_logo_cache[employer_id]['uid']:
                        self.employers_logo_cache[employer_id]['logo'] = 'https://api.xtramile.io/api/v1/files/%s/download' % company['logo']
                        #print self.employers_logo_cache[employer_id]['logo']
        if self.employers_logo_cache is not None and job['employer_id'] in self.employers_logo_cache:
            return self.employers_logo_cache[job['employer_id']]['logo']
        return None

    def getFacebookBoardIds(self):
        sql = "SELECT id FROM job_boards WHERE params#>>'{fb}' NOTNULL AND (params#>>'{fb}')::INTEGER=1"
        res = self._getraw(sql, ['id'])
        ids = map(lambda i: i['id'], res)
        return ids

    def refresh_view(self):
        self.dbopen()
        self.cur.execute("refresh materialized view v_jobs_for_feeds")
        self.conn.commit()

    def getJobBoardsNeedUpdate(self, force):
        sql = '''select * from (select job_boards.id, job_boards.update_frequency,  job_boards.feed_updated_at,
            (case when (current_timestamp-job_boards.feed_updated_at) > (interval '1 day' / job_boards.update_frequency) then 1 else 0 end)
            as need_run, feed, name, uuid, (select count(job_id) from v_jobs_for_feeds  v where v.job_board_id=job_boards.id) as number_jobs,
            FALSE AS done, FALSE AS azure_done
             from job_boards where job_boards.update_frequency > 0) as t  where t.need_run=1'''

        if force:
            sql = '''select * from (select job_boards.id, job_boards.update_frequency,  job_boards.feed_updated_at, 1
                        as need_run, feed, name, uuid, (select count(job_id) from v_jobs_for_feeds  v where v.job_board_id=job_boards.id) as number_jobs,
                        FALSE AS done, FALSE AS azure_done
                         from job_boards where job_boards.update_frequency > 0) as t  where t.need_run=1'''
        fld_list = 'id,update_frequency,feed_updated_at,need_run,feed,name,uuid,number_jobs,done,azure_done'.split(',')
        return self._getraw(sql, fld_list)

    def getBoards(self):
        table = '.'.join([self.schema, self.job_boards_table])
        boards = self._get(table, field_list=None, where='TRUE')
        if boards is not None:
            return boards
        return None

    def get_job_ids(self):
        sql = 'select job_id, array(select job_board_id from v_jobs_for_feeds where job_id=t1.job_id) as boards from v_jobs_for_feeds t1 GROUP BY job_id'
        job_ids = self._getraw(sql, ['job_id', 'boards'])
        return job_ids

    def get_jobs(self, job_ids, start, end):
        sql = 'select * FROM jobs WHERE id IN (%s)' % ','.join(map(lambda i: str(i['job_id']), job_ids[start:end]))
        self.jobs = self._getraw(sql, field_list=self.fld_lst_jobs)
        return self.jobs

    def getBoardFeedSettingAll(self):
        table = '.'.join([self.schema, self.job_board_feed_settings_table])
        res = self._get(table, field_list=None, where='TRUE')
        board_feed_settings = {}
        if res > 0:
            for row in res:
                board_feed_settings[row['job_board_id']] = row
            return board_feed_settings
        else:
            return None





