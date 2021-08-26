#!/usr/bin/env python
#coding=utf-8


########################################################
## Import geodata from Maxmind file to postgresql table
########################################################
import psycopg2
import logging
from pprint import pprint
import json
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


class Maxmind:

    logger = None
    database = None
    ROWS_PER_ONE = 500
    tablename = 'locations'
    fld_list = 'geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone'.split(',')

    def __init__(self, db):
        self.logger = logging.getLogger('Maxmind')
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.setLevel(1)
        self.logger.info('RUN Maxmind import')
        self.database = PgSQLStore(db)

    def clear_table(self):
        self.logger.info('...сlearing table %s' % self.tablename)
        self.database._clear_table(self.tablename)

    def import_data(self, filenames):
        for filename in filenames:
            self.logger.info('...processing file %s' % filename)
            f = open(filename)
            count = 0
            data = []
            for line in f:
                rec = {}
                row = line.split(',')
                if row[0] == 'geoname_id':
                    continue
                try:
                    for i in range(len(row)):
                        rec[self.fld_list[i]] = row[i]
                except IndexError:
                    self.logger.warning('Error: geoname_id %s skipped' % row[0])
                else:
                    data.append(rec)
                    count += 1
                if ((count % self.ROWS_PER_ONE) == 0):
                    try:
                        self.database._insert(self.tablename, data)
                        self.logger.info('...inserted %i rows' % count)
                    except Exception, ex:
                        self.logger.warning('...error while inserted data: %s' % ex)
                    else:
                        data = []
            try:
                self.database._insert(self.tablename, data)
                self.logger.info('...inserted %i rows' % count)
            except Exception, ex:
                self.logger.warning('...error while inserted data: %s' % ex)
            f.close()
        self.database.dbclose()
        self.logger.info('END Maxmind import')


maxmind = Maxmind(local)
maxmind.clear_table()
maxmind.import_data(['/home/user1/Downloads/Maxmind/bases/GeoLite2-City-Locations-en.csv', '/home/user1/Downloads/Maxmind/bases/GeoLite2-City-Locations-fr.csv'])






