#!/usr/bin/env python
#coding=utf-8

import psycopg2
import time
import random
import logging
from dbacc import *


class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'

    dbname = 'dbname'
    dbhost = 'dbport'
    dbport = 0000
    dbuser = 'dbuser'
    dbpass = 'dbpass'


    def __init__(self, db):
        self.dbname = db.get('dbname')
        self.dbhost = db.get('dbhost')
        self.dbport = db.get('dbport')
        self.dbuser = db.get('dbuser')
        self.dbpass = db.get('dbpass')

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

    def execute(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        self.dbclose()
        return res



action_data = '''3.08	0	0	0	5
4.08	0	0	0	7
5.08	0	0	0	4
6.08	0	0	0	8
7.08	0	0	0	3
8.08	0	0	0	6
9.08	0	0	0	8
10.08	0	0	0	9
11.08	0	0	0	9
12.08	0	0	0	4
13.08	0	0	0	5
14.08	0	0	0	6
15.08	0	0	0	2
16.08	0	0	0	6
17.08	0	0	0	5
18.08	0	0	0	3
19.08	0	0	0	4
20.08	0	0	0	8
21.08	0	0	0	0
22.08	0	0	0	8
23.08	0	0	0	6
24.08	0	0	0	12
25.08	0	0	0	3
26.08	0	0	0	5
27.08	0	0	0	14
28.08	0	0	0	9
3.09	0	0	0	10
4.09	0	0	0	11
5.09	0	0	0	15
6.09	0	0	0	6
7.09	0	0	0	8
8.09	0	0	0	4
9.09	0	0	0	5
10.09	0	0	0	3
11.09	0	0	0	9
12.09	0	0	0	2
13.09	0	0	0	5
14.09	0	0	0	10
15.09	1	0	0	5
16.09	3	0	0	4
17.09	1	0	0	7
18.09	2	0	0	6'''

action_data = action_data.replace("\t", ' ')

cpc = 1.4
cpc_origin = 1.0

data = {}
data['employer_id'] = 44
data['job_board_id'] = 16
data['job_id'] = 'c442b924-820c-49de-9b06-942f907b40a7_44'     #here jobs.exteranl_unuque_id
data['user_ip'] = '197.214.13.10'
data['user_agent'] = 'FAKE'
data['url_referrer'] = 'FAKE'
data['url'] = 'https://cesi.jobs.xtramile.io/responsable-performance-industrielle-et-innovation-titre-professionnel--de-niveau-bac4-(hf)'
data['cpc_origin'] = cpc_origin
data['cpc'] = cpc
data['duplicate'] = False
data['duplicate_id'] = None
data['user_token'] = 'fde06d1a82b3318b96c6412f2eea2a63'

rows = action_data.split("\n")
actions = []
for row in rows:
    items = filter(lambda s: len(s) > 0, map(lambda s: s.strip(), row.strip().split(' ')))
    actions.append(items)

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def getDate(date_str):
    h = random.randrange(0, 23)
    m = random.randrange(0, 59)
    s = random.randrange(0, 59)
    timestamp = int(time.mktime((2017, int(date_str.split('.')[1]), int(date_str.split('.')[0]), h, m, s, 0, 0, 0)))
    date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(timestamp))
    return date


def insertData(db, data):
    sets = []
    table = 'stats'
    for key, val in data.items():
        sets.append(key + '= %s')
    sql = ' '.join(['INSERT INTO', table, '(', ','.join(data.keys()), ') VALUES (', ','.join(['%s' for i in data.keys()]), ');'])
    db.cur.execute(sql, data.values())


def run(db, actions, data):
    db = PgSQLStore(db)
    db.dbopen()
    for action in actions:
        print 'Inserting stat for date %s ...' % action[0]
        for i in range(4):
            print '...Inserting stat for action %i ...' % i
            for j in range(int(action[i+1])):
                print '......insert record %i/%s ... ' % (j+1, action[i+1])
                date = getDate(action[0])
                data['created_at'] = date
                data['updated_at'] = date
                data['action'] = i
                if i == 0:
                    data['cpc'] = cpc
                    data['cpc_origin'] = cpc_origin
                else:
                    data['cpc'] = 0
                    data['cpc_origin'] = 0
                insertData(db, data)
    db.conn.commit()
    db.dbclose()

if __name__ == '__main__':
    run(test, actions, data)





