#!/usr/bin/env python
#coding=utf-8

import psycopg2
import logging
from pprint import pprint
import getopt
import sys
from ConfigParser import *
import requests
import json
from base64 import b64encode
import inspect
import time


class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'

    dbname = 'dbname'
    dbhost = 'dbhost'
    dbport = 0000
    dbuser = 'dbuser'
    dbpass = 'dbpass'

    def loadDbParams(self, conf):
        if conf.has_option('database', 'dbname'):
            self.dbname = conf.get('database', 'dbname')
        if conf.has_option('database', 'dbhost'):
            self.dbhost = conf.get('database', 'dbhost')
        if conf.has_option('database', 'dbport'):
            self.dbport = conf.get('database', 'dbport')
        if conf.has_option('database', 'dbuser'):
            self.dbuser = conf.get('database', 'dbuser')
        if conf.has_option('database', 'dbpass'):
            self.dbpass = conf.get('database', 'dbpass')

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


class PgSQLStoreAutorun(PgSQLStore):

    def getEmployerMetadata(self, employerId):
        table = 'employers'
        res = self._get(table, field_list=['metadata'], where='id=%s', data=[employerId])
        if len(res) > 0:
            return res[0]['metadata']
        else:
            return None

    def getEmployersNeedRunScraper(self):
        sql = '''select * from (select employers.id, employers.update_frequency, employers.feed_updated_at,
                 (case when (current_timestamp-employers.feed_updated_at) > (interval '1 day' / employers.update_frequency)
                   then 1 else 0 end) as need_run  from employers where employers.update_frequency > 0) as t  where t.need_run=1'''

        fld_list = 'id,update_frequency,feed_updated_at,need_run'.split(',')
        return self._getraw(sql, fld_list)

    def getEmployersNeedJobimport(self):
        sql = '''select * from (select  jdb.employer_id, feed_updated_at, jdb.update_jobs, (case when feed_updated_at > jdb.update_jobs then 1 else 0 end) as need_run
               from employers inner join (select employer_id, (case when max(updated_at) > max(created_at) then max(updated_at) else  max(created_at) end) as update_jobs
              from jobs  GROUP BY employer_id) as jdb on employers.id=jdb.employer_id where employers.enabled = TRUE) as t where t.need_run=1'''

        fld_list = 'employer_id,update_jobs,feed_updated_at,need_run'.split(',')
        return self._getraw(sql, fld_list)

    def getJobBoardsNeedUpdate(self):
        sql = '''select * from (select job_boards.id, job_boards.update_frequency,  job_boards.feed_updated_at,
            (case when (current_timestamp-job_boards.feed_updated_at) > (interval '1 day' / job_boards.update_frequency) then 1 else 0 end)
            as need_run  from job_boards where job_boards.update_frequency > 0) as t  where t.need_run=1'''

        fld_list = 'id,update_frequency,feed_updated_at,need_run'.split(',')
        return self._getraw(sql, fld_list)

    def setStatusEmployerJobBoards(self, k, logger):
        logger.debug('Running %s' % inspect.stack()[0][3])
        #sql = '''select employer_job_boards.id, employer_job_boards.job_board_id, job_boards.name as job_board, employer_job_boards.employer_id, employers.name as employer
        #                from employer_job_boards INNER JOIN job_boards on employer_job_boards.job_board_id = job_boards.id INNER JOIN
        #                employers on employer_job_boards.employer_id=employers.id where employer_job_boards.monthly_budget_spent < employer_job_boards.monthly_budget * %s
        #                AND employer_job_boards.enabled=FALSE'''

        #fld_list = 'id,job_board_id,job_board,employer_id,employer'.split(',')
        #res = self._getraw(sql, fld_list, [k])
        #logger.debug('res=%s' % str(res))
        #if len(res) > 0:
        #    sql = '''UPDATE employer_job_boards SET enabled=TRUE WHERE
        #    employer_job_boards.monthly_budget_spent < employer_job_boards.monthly_budget * %s AND
        #    employer_job_boards.enabled=FALSE'''
        #    self._exec(sql, [k])
        #    for row in res:
        #        logger.info('Status employerJobBoards was set to ENABLED for employer "%s" and job_board "%s"' % (row['employer'], row['job_board']))

        sql = '''select employer_job_boards.id, employer_job_boards.job_board_id, job_boards.name as job_board, employer_job_boards.employer_id, employers.name,
                 employers.id as employer_id
                from employer_job_boards INNER JOIN job_boards on employer_job_boards.job_board_id = job_boards.id INNER JOIN
                employers on employer_job_boards.employer_id=employers.id where employer_job_boards.monthly_budget_spent >= employer_job_boards.monthly_budget * %s
                AND employer_job_boards.enabled=TRUE'''

        fld_list = 'id,job_board_id,job_board,employer_id,employer, employer_id'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE employer_job_boards SET enabled=FALSE WHERE
                employer_job_boards.monthly_budget_spent >= employer_job_boards.monthly_budget * %s AND
                employer_job_boards.enabled=TRUE'''
            self._exec(sql, [k])
            for row in res:
                msg = 'Status employerJobBoards was set to DISABLED for employer "%s" and job_board "%s" because monthly_budget_spent >= monthly_budget * %f' % (
                row['employer'], row['job_board'], k)
                logger.info(msg)
                self.addRecordToBudgeActions(row['employer_id'], msg, 0, 'EmployerJobBoard', 'EmployerJobBoard', 'disabled')

    def setStatusEmployers(self, k, logger, positive_balance_limit):
        logger.debug('Running %s' % inspect.stack()[0][3])
        #sql = '''select employers.id, employers.name as employer from employers where
        #        employers.monthly_budget_spent < employers.monthly_budget * %s AND employers.enabled=FALSE'''

        #fld_list = 'id,employer'.split(',')
        #res = self._getraw(sql, fld_list, [k])
        #logger.debug('res=%s' % str(res))
        #if len(res) > 0:
        #    sql = '''UPDATE employers SET enabled=TRUE WHERE
        #    employers.monthly_budget_spent < employers.monthly_budget * %s AND employers.enabled=FALSE'''
        #    self._exec(sql, [k])
        #    for row in res:
        #        logger.info('Status employer "%s" was set to ENABLED' % row['employer'])

        sql = '''select employers.id, employers.name as employer from employers where
                employers.monthly_budget_spent >= employers.monthly_budget * %s AND employers.enabled=TRUE'''

        fld_list = 'id,employer'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE employers SET enabled=FALSE WHERE
            employers.monthly_budget_spent >= employers.monthly_budget * %s AND employers.enabled=TRUE'''
            self._exec(sql, [k])
            for row in res:
                logger.info('Status employer "%s" was set to DISABLED' % row['employer'])
                self.addRecordToBudgeActions(row['id'], 'Status employer "%s" was set to DISABLED' % row['employer'], 0, 'Employer', 'Employer', 'disabled')

        #enable/disable employer by totalDeposit
        sql = '''SELECT id, name as employer FROM employers WHERE enabled=TRUE AND (total_deposit-(total_spent+employers.monthly_budget_spent)) < %s'''

        fld_list = 'id,employer'.split(',')
        res = self._getraw(sql, fld_list, [positive_balance_limit])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE employers SET enabled=FALSE WHERE
            enabled=TRUE AND (total_deposit-(total_spent+employers.monthly_budget_spent)) < %s'''
            self._exec(sql, [positive_balance_limit])
            for row in res:
                logger.info('Status employer "%s" was set to DISABLED' % row['employer'])
                self.addRecordToBudgeActions(row['id'], 'Positive Balance Limit of %s is reached' % positive_balance_limit, 0, 'Employer', 'Employer', 'positive_balance_limit')


        #sql = '''SELECT id, name as employer FROM employers WHERE enabled=FALSE AND (total_deposit-(total_spent+employers.monthly_budget_spent)) >= %s'''

        #fld_list = 'id,employer'.split(',')
        #res = self._getraw(sql, fld_list, [positive_balance_limit])
        #logger.debug('res=%s' % str(res))
        #if len(res) > 0:
        #    sql = '''UPDATE employers SET enabled=TRUE WHERE
        #    enabled=FALSE AND (total_deposit-(total_spent+employers.monthly_budget_spent)) >= %s'''
        #    self._exec(sql, [positive_balance_limit])
        #    for row in res:
        #        logger.info('Status employer "%s" was set to ENABLED' % row['employer'])
        #        self.addRecordToBudgeActions(row['id'], 'Positive Balance Limit of %s is present' % positive_balance_limit)


    def setStatusJobGroupJobBoards(self, k, logger):
        logger.debug('Running %s' % inspect.stack()[0][3])
        #sql = '''select job_group_job_boards.id, job_group_job_boards.job_board_id, job_boards.name as job_board, job_group_job_boards.job_group_id, job_groups.name as job_group
        #            from job_group_job_boards INNER JOIN job_boards on job_group_job_boards.job_board_id = job_boards.id INNER JOIN
        #            job_groups on job_group_job_boards.job_group_id=job_groups.id where job_group_job_boards.monthly_budget_spent < job_group_job_boards.monthly_budget * %s
        #            AND job_group_job_boards.enabled=FALSE'''

        #fld_list = 'id,job_board_id,job_board,job_group_id,job_group'.split(',')
        #res = self._getraw(sql, fld_list, [k])
        #logger.debug('res=%s' % str(res))
        #if len(res) > 0:
        #    sql = '''UPDATE job_group_job_boards SET enabled=TRUE WHERE
        #    job_group_job_boards.monthly_budget_spent < job_group_job_boards.monthly_budget * %s AND
        #    job_group_job_boards.enabled=FALSE'''
        #    self._exec(sql, [k])
        #    for row in res:
        #        logger.info('Status jobGroupJobBoards was set to ENABLED for job_group "%s" and job_board "%s"' % (
        #        row['job_group'], row['job_board']))

        sql = '''select job_group_job_boards.id, job_group_job_boards.job_board_id, job_boards.name as job_board, job_group_job_boards.job_group_id, job_groups.name as job_group
                    from job_group_job_boards INNER JOIN job_boards on job_group_job_boards.job_board_id = job_boards.id INNER JOIN
                    job_groups on job_group_job_boards.job_group_id=job_groups.id where job_group_job_boards.monthly_budget_spent >= job_group_job_boards.monthly_budget * %s
                    AND job_group_job_boards.enabled=TRUE'''

        fld_list = 'id,job_board_id,job_board,job_group_id,job_group'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE job_group_job_boards SET enabled=FALSE WHERE
            job_group_job_boards.monthly_budget_spent >= job_group_job_boards.monthly_budget * %s AND
            job_group_job_boards.enabled=TRUE'''
            self._exec(sql, [k])
            for row in res:
                logger.info('Status jobGroupJobBoards was set to DISABLED for job_group "%s" and job_board "%s"' % (
                    row['job_group'], row['job_board']))

    def setStatusJobs(self, k, logger, status):
        logger.debug('Running %s' % inspect.stack()[0][3])
        sql = '''select jobs.id, jobs.employer_id, employers.name as employer from jobs INNER JOIN employers on jobs.employer_id=employers.id
                where jobs.budget_spent < jobs.budget * %s AND jobs.status not in (0,1,2,3,4)'''

        fld_list = 'id,employer_id,employer'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE jobs SET status=%s
                where jobs.budget_spent < jobs.budget * %s AND jobs.status not in (0,1,2,3,4)'''
            self._exec(sql, [k, status])
            for row in res:
                msg = 'Status job id= "%i" and employer "%s" was set to %i, because budget_spent < budget * %f' % (row['id'], row['employer'], status, k)
                logger.info(msg)
                self.addRecordToBudgeActions(row['employer_id'], msg, amount=0, rectype='Job', type_code='Job', comment_code=None)

    def setStatusJobGroups(self, k, logger):
        logger.debug('Running %s' % inspect.stack()[0][3])
        sql = '''select job_groups.id, job_groups.name as job_group from job_groups where
                job_groups.monthly_budget_progress < job_groups.monthly_budget * %s AND job_groups.enabled=FALSE'''

        fld_list = 'id,job_group'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE job_groups SET enabled=TRUE WHERE
            job_groups.monthly_budget_progress < job_groups.monthly_budget * %s AND job_groups.enabled=FALSE'''
            self._exec(sql, [k])
            for row in res:
                logger.info('Status Job Group "%s" was set to ENABLED' % row['job_group'])

        sql = '''select job_groups.id, job_groups.name as job_group from job_groups where
                job_groups.monthly_budget_progress >= job_groups.monthly_budget * %s AND job_groups.enabled=TRUE'''

        fld_list = 'id,job_group'.split(',')
        res = self._getraw(sql, fld_list, [k])
        logger.debug('res=%s' % str(res))
        if len(res) > 0:
            sql = '''UPDATE job_groups SET enabled=FALSE WHERE
            job_groups.monthly_budget_progress >= job_groups.monthly_budget * %s AND job_groups.enabled=TRUE'''
            self._exec(sql, [k])
            for row in res:
                logger.info('Status Job Group "%s" was set to DISABLED' % row['job_group'])


    def addRecordToBudgeActions(self, employerId, comment, amount=0, rectype=None, type_code='Employer', comment_code=None):
        if rectype is None:
            rectype = 'Employer'
        now = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        row = {'budgetable_type': rectype, 'budgetable_id': employerId, 'comment': comment, 'created_at': now, 'updated_at': now, 'amount': amount, 'budgetable_type_enum': type_code, 'comment_enum': comment_code}
        self._insert('budget_actions', [row])


class Autorun:
    logger = None
    store = None
    conf = None
    loggerStatus = None
    option = 'default'
    args = None
    run_spider='myproject,myspider'

    def __init__(self, store):
        self.store = store
        self._loadConfig()
        self._loadLogger()
        self._loadLoggerStatus()
        self.store.loadDbParams(self.conf)

    def _loadLogger(self):
        self.logger = logging.getLogger('ScrapyAutorun')
        logfile = self.conf.get('logger', 'logfile')
        try:
            fh = logging.FileHandler(logfile, 'a', 'utf8')
        except Exception, ex:
            fh = logging.FileHandler(logfile.split('/').pop(), 'a', 'utf8')
        fh2 = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        fh2.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(fh2)
        lvl = self.conf.getint('logger', 'logger_level')
        self.logger.setLevel(lvl)

    def _loadLoggerStatus(self):
        self.loggerStatus = logging.getLogger('StatusLogger')
        statusLogfile = self.conf.get('logger', 'status_logfile')
        try:
            fh = logging.FileHandler(statusLogfile, 'a', 'utf8')
        except Exception, ex:
            fh = logging.FileHandler(statusLogfile.split('/').pop(), 'a', 'utf8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        self.loggerStatus.addHandler(fh)
        lvl = self.conf.getint('logger', 'status_logger_level')
        self.loggerStatus.setLevel(lvl)

    def _loadConfig(self):
        try:
            optlist, args = getopt.getopt(sys.argv[1:], 'c:t', ['config-file=', 'test', 'jobs-lp', 'company-lp', 'job-apply-form', 'job-apply', 'import-jobleads', 'import-jobmonitor', 'import-dejobmonitor', 'run-spider=', 'args='])
            if '-c' in map(lambda item: item[0], optlist):
                config_file = filter(lambda item: item[0] == '-c', optlist)[0][1]
            elif '--config-file' in map(lambda item: item[0], optlist):
                config_file = filter(lambda item: item[0] == '--config-file', optlist)[0][1]
            else:
                raise Exception('config file expected!')
            self.conf = ConfigParser()
            self.conf.read(config_file)
            if '-t' in map(lambda item: item[0], optlist):
                self.option = 'test'
            elif '--test' in map(lambda item: item[0], optlist):
                self.option = 'test'
            if '--jobs-lp' in map(lambda item: item[0], optlist):
                self.option = 'jobs-lp'
            if '--company-lp' in map(lambda item: item[0], optlist):
                self.option = 'company-lp'
            if '--job-apply-form' in map(lambda item: item[0], optlist):
                self.option = 'job-apply-form'
            if '--job-apply' in map(lambda item: item[0], optlist):
                self.option = 'job-apply'
            if '--import-jobleads' in map(lambda item: item[0], optlist):
                self.option = 'import-jobleads'
            if '--import-jobmonitor' in map(lambda item: item[0], optlist):
                self.option = 'import-jobmonitor'
            if '--import-dejobmonitor' in map(lambda item: item[0], optlist):
                self.option = 'import-dejobmonitor'
            if '--run-spider' in map(lambda item: item[0], optlist):
                self.run_spider = filter(lambda item: item[0] == '--run-spider', optlist)[0][1]
                self.option = 'run-spider'
                if '--args' in map(lambda item: item[0], optlist):
                    self.args = filter(lambda item: item[0] == '--args', optlist)[0][1]

        except Exception, ex:
            print 'Autorun scrapers script'
            print 'Usage: %s options' % sys.argv[0]
            print '''Options:
                            -c <filename>  or --config-file=<filename>  - Filename config. Required.
                            Optional:
                            -t or --test - Run test code.
                            --jobs-lp - Run check jobs landing pages
                            --company-lp - Run check companies landing pages
                            --job-apply-form - Run check apply form landing pages
                            --job-apply - Run check apply candidate
                            --import-jobleads - Run import from JobLeads feeds
                            --import-jobmonitor - Run import from JobMonitor feeds
                            --import-dejobmonitor - Run import from DE JobMonitor feeds
                    '''
            print ex.message
            exit(1)

    def run(self):
        self.logger.debug('Start autorun')
        coef = self.conf.getfloat('status', 'k')
        positive_balance_limit = self.conf.getfloat('status', 'positive_balance_limit')
        self.store.setStatusEmployerJobBoards(coef, self.loggerStatus)
        self.store.setStatusEmployers(coef, self.loggerStatus, positive_balance_limit)
        self.store.setStatusJobGroupJobBoards(coef, self.loggerStatus)
        status = self.conf.getint('status', 'status')
        #self.store.setStatusJobs(coef, self.loggerStatus, status)

        if self.limitJobsExceeded():
            self.logger.warning('Max jobs number running')
            exit(0)

        #employersNeedRunScraper = self.store.getEmployersNeedRunScraper()
        #self.logger.debug('employersNeedRunScraper=%s' % str(employersNeedRunScraper))
        #for employer in employersNeedRunScraper:
        #    self.runJobScrapper(employer['id'])

        #employersNeedJobimport = self.store.getEmployersNeedJobimport()
        #self.logger.debug('employersNeedJobimport=%s' % str(employersNeedJobimport))
        #for employer in employersNeedJobimport:
        #    self.runJobImporters(employer['employer_id'])

        if not self.feedgeneratorAlreadyRunned():
            jobBoardsNeedUpdate = self.store.getJobBoardsNeedUpdate()
            self.logger.debug('jobBoardsNeedUpdate=%s' % str(jobBoardsNeedUpdate))
            #for jobBoard in jobBoardsNeedUpdate:
            #    self.runFeedGenerator(jobBoard['id'])
            #    self.runFeedGeneratorJobInTree(jobBoard['id'])
            if len(jobBoardsNeedUpdate) > 0:
                self.runFeedGenerator()
        else:
            self.logger.debug('feedgeneratorAlreadyRunned')
        self.logger.debug('End autorun')

    def getListSpiders(self):
        if self.conf.has_option('scrapyd', 'api_listspiders'):
            server = self.conf.get('scrapyd', 'server')
            url = server + self.conf.get('scrapyd', 'api_listspiders')
            res = requests.get(url)
            if res.status_code == 200:
                result = json.loads(res.text)
                if result['status'] == 'ok':
                    return result['spiders']
        return None

    def getListJobs(self):
        if self.conf.has_option('scrapyd', 'api_listjobs'):
            server = self.conf.get('scrapyd', 'server')
            url = server + self.conf.get('scrapyd', 'api_listjobs')
            res = requests.get(url)
            if res.status_code == 200:
                result = json.loads(res.text)
                if result['status'] == 'ok':
                    return {'running': result['running'], 'pending': result['pending'], 'finished': result['finished']}
        return None

    def limitJobsExceeded(self):
        maxRunJobs = int(self.conf.get('scrapyd', 'max_run_jobs'))
        listJobs = self.getListJobs()
        if len(listJobs['running']) >= maxRunJobs:
            return True
        return False

    def feedgeneratorAlreadyRunned(self):
        if self.conf.has_option('scrapyd', 'api_listjobs'):
            server = self.conf.get('scrapyd', 'server')
            url = server + 'listjobs.json?project=feedgenerator'
            res = requests.get(url)
            if res.status_code == 200:
                result = json.loads(res.text)
                if result['status'] == 'ok':
                    if 'running' in result and len(result['running']) == 0 and 'pending' in result and len(result['pending']) == 0:
                        return False
        return True

    def spiderAlreadyRunned(self, project_name, spider_name):
        if self.conf.has_option('scrapyd', 'api_listjobs'):
            server = self.conf.get('scrapyd', 'server')
            url = server + 'listjobs.json?project=%s' % project_name
            res = requests.get(url)
            if res.status_code == 200:
                result = json.loads(res.text)
                if result['status'] == 'ok':
                    if 'running' in result and len(result['running']) > 0:
                        for item in result['running']:
                            if item['spider'] == spider_name:
                                return True
                    if 'pending' in result and len(result['pending']) > 0:
                        for item in result['pending']:
                            if item['spider'] == spider_name:
                                return True

        return False

    def runSpider(self, project_name, spider_name, args):
        #disable check if spider in parallel mode
        if 'worker' not in args:
            if self.spiderAlreadyRunned(project_name, spider_name):
                self.logger.info('Spiger "%s" of project "%s" already runned or pending!' % (spider_name, project_name))
                return False
        self.logger.debug('running spider "%s", args: %s' % (spider_name, args))
        projectId = self.getProjectIdByName(project_name)
        spiderId = self.getSpiderIdByName(projectId, spider_name)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': args}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % (project_name, spider_name, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def getBasicAuthHeader(self):
        username = self.conf.get('spiderkeeper', 'username')
        password = self.conf.get('spiderkeeper', 'password')
        userAndPass = b64encode(b'%s:%s' % (username, password)).decode('ascii')
        headers = {'Authorization': 'Basic %s' % userAndPass}
        return headers


    def spiderAlreadyRun(self, projectId, spiderName):
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_job_list') % projectId, '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_job_list') % projectId])
        print url
        res = requests.get(url, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            result = json.loads(res.text)
            for item in result['RUNNING']:
                if item['job_instance']['spider_name'] == spiderName:
                    return True
            for item in result['PENDING']:
                if item['job_instance']['spider_name'] == spiderName:
                    return True
        return False


    def getProjectIdByName(self, name):
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_projects'), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_projects')])
        res = requests.get(url, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            result = json.loads(res.text)
            for item in result:
                if item['project_name'] == name:
                    return item['project_id']
        return None

    def getSpiderIdByName(self, projectId, name):
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spiders') % str(projectId), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spiders') % str(projectId)])
        res = requests.get(url, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            result = json.loads(res.text)
            for item in result:
                if item['spider_name'] == name:
                    return item['spider_instance_id']
        return None

    def runJobScrapper(self, employerId):
        self.logger.debug('running runJobScrapper, employer_id=%i' % employerId)
        projectId = self.getProjectIdByName('jobscrapers')
        scrapersRunParams = self.store.getEmployerMetadata(employerId)
        if scrapersRunParams is None:
            self.logger.debug('Employer %i has not metadata' % int(employerId))
            return False
        if 'spider' not in scrapersRunParams:
            self.logger.debug('Employer id=%i have not spider' % employerId)
            return False
        spiderName = scrapersRunParams['spider']
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        if self.spiderAlreadyRun(projectId, spiderName):
            self.logger.debug('spiderAlreadyRun')
            return False
        if self.limitJobsExceeded():
            self.logger.debug('limitJobsExceeded')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = None
        self.logger.debug('scrapersRunParams=%s' % str(scrapersRunParams))
        if 'employer_id' in scrapersRunParams:
            data = {'spider_arguments': 'employer_id=%i' % int(scrapersRunParams['employer_id'])}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('jobscrapers', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runJobImporters(self, employerId, spiderName=None):
        self.logger.debug('running runJobImporters, employer_id=%i' % employerId)
        projectId = self.getProjectIdByName('jobimporters')
        scrapersRunParams = self.store.getEmployerMetadata(employerId)
        if spiderName is None:
            if 'spider' not in scrapersRunParams:
                self.logger.debug('Employer id=%i have not spider' % employerId)
                spiderName = 'general'
            else:
                spiderName = scrapersRunParams['spider']
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        #if self.spiderAlreadyRun(projectId, spiderName):
        #    self.logger.debug('spiderAlreadyRun')
        #    return False
        if self.limitJobsExceeded():
            self.logger.debug('limitJobsExceeded')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'employer_id=%i' % int(employerId)}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('jobimporters', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runFeedGenerator_Old(self, jobBoardId):
        self.logger.debug('running runFeedGenerator, job_board_id=%i' % jobBoardId)
        projectId = self.getProjectIdByName('feedgenerator')
        spiderName = 'general3'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        #if self.spiderAlreadyRun(projectId, spiderName):
        #    self.logger.debug('spiderAlreadyRun')
        #    return False
        if self.limitJobsExceeded():
            self.logger.debug('limitJobsExceeded')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'board_id=%i,azure=true' % int(jobBoardId)}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('feedgenerator', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runFeedGenerator(self):
        self.logger.debug('running runFeedGenerator')
        projectId = self.getProjectIdByName('feedgenerator')
        spiderName = 'general4'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        #if self.spiderAlreadyRun(projectId, spiderName):
        #    self.logger.debug('spiderAlreadyRun')
        #    return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'azure=true,port=6800'}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('feedgenerator', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runFeedGeneratorJobInTree(self, jobBoardId):
        if jobBoardId not in [9, 12, 15]:
            return True
        self.logger.debug('running runFeedGeneratorJobInTree, job_board_id=%i' % jobBoardId)
        projectId = self.getProjectIdByName('feedgenerator')
        spiderName = 'jobintree2'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        # if self.spiderAlreadyRun(projectId, spiderName):
        #    self.logger.debug('spiderAlreadyRun')
        #    return False
        if self.limitJobsExceeded():
            self.logger.debug('limitJobsExceeded')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join(
                [server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=',
                 api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'board_id=%i,azure=true' % int(jobBoardId)}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('feedgenerator', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False


    def runFeedGeneratorIndeed(self):
        self.logger.debug('running runFeedGeneratorIndeed')
        projectId = self.getProjectIdByName('feedgenerator')
        spiderName = 'indeed'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        #if self.spiderAlreadyRun(projectId, spiderName):
        #    self.logger.debug('spiderAlreadyRun')
        #    return False
        if self.limitJobsExceeded():
            self.logger.debug('limitJobsExceeded')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=', api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = None
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s' % ('feedgenerator', spiderName))
            return True
        self.logger.error(res.text)
        return False

    def runJobLandingMonitor(self):
        self.logger.debug('running runJobLandingMonitor')
        projectId = self.getProjectIdByName('monitor')
        spiderName = 'joblp'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join(
                [server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=',
                 api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'env=prod'}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('monitor', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runCompanyLandingMonitor(self):
        self.logger.debug('running runCompanyLandingMonitor')
        projectId = self.getProjectIdByName('monitor')
        spiderName = 'companylp'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join(
                [server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=',
                 api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'env=prod'}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('monitor', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runApplyFormMonitor(self):
        self.logger.debug('running runApplyFormMonitor')
        projectId = self.getProjectIdByName('monitor')
        spiderName = 'jobapplyform'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join(
                [server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=',
                 api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'env=prod'}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('monitor', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def runApplyMonitor(self):
        self.logger.debug('running runApplyMonitor')
        projectId = self.getProjectIdByName('monitor')
        spiderName = 'jobapply'
        spiderId = self.getSpiderIdByName(projectId, spiderName)
        if spiderId is None:
            self.logger.debug('spiderId is None')
            return False
        server = self.conf.get('spiderkeeper', 'server')
        if self.conf.has_option('spiderkeeper', 'api_key'):
            api_key = self.conf.get('spiderkeeper', 'api_key')
            url = ''.join(
                [server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId)), '?api_key=',
                 api_key])
        else:
            url = ''.join([server, self.conf.get('spiderkeeper', 'api_spider_run') % (str(projectId), str(spiderId))])
        data = {'spider_arguments': 'env=prod'}
        self.logger.debug('data=%s' % str(data))
        res = requests.put(url, data=data, headers=self.getBasicAuthHeader())
        if res.status_code == 200:
            self.logger.info('Run spider: project=%s, spider=%s, params=%s' % ('monitor', spiderName, str(data)))
            return True
        self.logger.error(res.text)
        return False

    def run_test(self):
        print 'test mode'
        #pprint(self.store.getEmployerMetadata(14))
        #pprint(self.spiderAlreadyRun(1, 'societegenerale'))
        #scrapersRunParams = self.store.getEmployerMetadata(14)
        #if scrapersRunParams is None:
        #    self.logger.debug('Employer %i has not metadata' % int(14))
        #    return False
        #spiderName = scrapersRunParams['spider']
        #pprint(scrapersRunParams)
        #pprint(spiderName)
        #print self.getListJobs()
        #print self.feedgeneratorAlreadyRunned()

autorun = Autorun(PgSQLStoreAutorun())
if autorun.option == 'default':
    autorun.run()
elif autorun.option == 'test':
    autorun.run_test()
elif autorun.option == 'jobs-lp':
    autorun.runJobLandingMonitor()
elif autorun.option == 'company-lp':
    autorun.runCompanyLandingMonitor()
elif autorun.option == 'job-apply-form':
    autorun.runApplyFormMonitor()
elif autorun.option == 'job-apply':
    autorun.runApplyMonitor()
elif autorun.option == 'import-jobleads':
    autorun.runJobImporters(101, 'jobleads')
elif autorun.option == 'import-jobmonitor':
    autorun.runJobImporters(104, 'jobmonitor')
elif autorun.option == 'import-dejobmonitor':
    autorun.runJobImporters(119, 'dejobmonitor')
elif autorun.option == 'run-spider':
    autorun.runSpider(autorun.run_spider.split(',')[0], autorun.run_spider.split(',')[1], autorun.args)











