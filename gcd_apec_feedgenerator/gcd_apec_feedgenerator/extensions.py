import psycopg2
import json
import sqlite3
import logging
from ConfigParser import *
import os
import time
import urllib
import requests
import certifi

class Geocode:

    conn = None
    cur = None
    dbname1 = '/home/user1/geobase.sqlite'
    dbname2 = '/home/ubuntu/geobase.sqlite'
    dbname3 = '/home/root/geobase.sqlite'
    gooogleApiUrl = 'https://maps.googleapis.com/maps/api/geocode/json?'
    cache = {} #cache for mapping city_name -> country_code
    cache2 = {} #cache for mapping country_code -> country_info
    cache3 = {} #cache for mapping country -> ISO code
    insee_cache = {} #cache for INSEE codes
    iso2 = [
        'AF', 'AX', 'AL', 'DZ', 'AS', 'AD', 'AO', 'AI', 'AQ', 'AG', 'AR', 'AM', 'AW', 'AU', 'AT', 'AZ', 'BS', 'BH', 'BD',
        'BB', 'BY', 'BE', 'BZ', 'BJ', 'BM', 'BT', 'BA', 'BW', 'BV', 'BR', 'IO', 'BN', 'BG', 'BF', 'BI', 'KH', 'CM', 'CA',
        'CV', 'KY', 'CF', 'TD', 'CL', 'CN', 'CX', 'CC', 'CO', 'KM', 'CG', 'CK', 'CR', 'CI', 'HR', 'CU', 'CW', 'CY', 'CZ',
        'DK', 'DJ', 'DM', 'DO', 'EC', 'EG', 'SV', 'GQ', 'ER', 'EE', 'ET', 'FK', 'FO', 'FJ', 'FI', 'FR', 'GF', 'PF', 'TF',
        'GA', 'GM', 'GE', 'DE', 'GH', 'GI', 'GR', 'GL', 'GD', 'GP', 'GU', 'GT', 'GG', 'GN', 'GW', 'GY', 'HT', 'HM', 'VA',
        'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IQ', 'IE', 'IM', 'IL', 'IT', 'JM', 'JP', 'JE', 'JO', 'KZ', 'KE', 'KI', 'KW',
        'KG', 'LA', 'LV', 'LB', 'LS', 'LR', 'LY', 'LI', 'LT', 'LU', 'MO', 'MG', 'MW', 'MY', 'MV', 'ML', 'MT', 'MH', 'MQ',
        'MR', 'MU', 'YT', 'MX', 'MC', 'MN', 'ME', 'MS', 'MA', 'MZ', 'MM', 'NA', 'NR', 'NP', 'NL', 'NC', 'NZ', 'NI', 'NE',
        'NG', 'NU', 'NF', 'MP', 'NO', 'OM', 'PK', 'PW', 'PA', 'PG', 'PY', 'PE', 'PH', 'PN', 'PL', 'PT', 'PR', 'QA', 'RE',
        'RO', 'RU', 'RW', 'BL', 'KN', 'LC', 'MF', 'PM', 'VC', 'WS', 'SM', 'ST', 'SA', 'SN', 'RS', 'SC', 'SL', 'SG', 'SX',
        'SK', 'SI', 'SB', 'SO', 'ZA', 'GS', 'SS', 'ES', 'LK', 'SD', 'SR', 'SJ', 'SZ', 'SE', 'CH', 'SY', 'TJ', 'TH', 'TL',
        'TG', 'TK', 'TO', 'TT', 'TN', 'TR', 'TM', 'TC', 'TV', 'UG', 'UA', 'AE', 'GB', 'US', 'UM', 'UY', 'UZ', 'VU', 'VN',
        'WF', 'EH', 'YE', 'ZM', 'ZW'
    ]

    def __init__(self):
        self._dbopen()
        self.cur.execute('CREATE TABLE IF NOT EXISTS cache(city PRIMARY KEY, countryCode)')
        self.cur.execute('CREATE TABLE IF NOT EXISTS insee_cache(city PRIMARY KEY, insee_code)')
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
        for city, countryCode in self.cache.items():
            try:
                self.cur.execute('INSERT OR REPLACE INTO cache (city , countryCode) VALUES (?,?)', [city, countryCode])
            except Exception, ex:
                city = unicode(city, 'utf-8')
                self.cur.execute('INSERT OR REPLACE INTO cache (city , countryCode) VALUES (?,?)', [city, countryCode])
        self.conn.commit()
        for city, insee_code in self.insee_cache.items():
            try:
                self.cur.execute('INSERT OR REPLACE INTO insee_cache (city , insee_code) VALUES (?,?)', [city, insee_code])
            except Exception, ex:
                city = unicode(city, 'utf-8')
                self.cur.execute('INSERT OR REPLACE INTO insee_cache (city , countryCode) VALUES (?,?)', [city, insee_code])
        self.conn.commit()
        if dbclose:
            self._dbclose()

    def loadCache(self, dbclose=True):
        self._dbopen()
        self.cur.execute('SELECT city, countryCode FROM cache')
        res = self.cur.fetchall()
        if res is not None:
            for row in res:
                city = row[0]
                countryCode = row[1]
                self.cache[city] = countryCode
        self.cur.execute('SELECT city, insee_code FROM insee_cache')
        res = self.cur.fetchall()
        if res is not None:
            for row in res:
                city = row[0]
                insee_code = row[1]
                self.insee_cache[city] = insee_code

        if dbclose:
            self._dbclose()

    def city2CountryGoogle(self, city):
        try:
            city = city.strip().encode("utf-8")
        except UnicodeDecodeError, ex:
            city = city.strip()
        try:
            param = urllib.urlencode({'address': city})
            resp = requests.get(self.gooogleApiUrl + param)
            respd = json.loads(resp.text)
            if respd['status'] == 'OK':
                code = None
                for component in respd['results'][0]['address_components']:
                    if component['types'][0] == 'country':
                        code = component['short_name']
                self.cache[city] = code
                return code
            else:
                return None
        except Exception, ex:
            return None

    def city2CountryGeobase(self, city):
        city = city.strip()
        self._dbopen()
        try:
            sql = ''.join(
                ['SELECT country_code FROM allCountries WHERE name=? OR asciiname=? OR alternatenames LIKE "%', city,'%"'])
            self.cur.execute(sql, [city, city])
        except Exception, ex:
            city = unicode(city, 'utf8')
            sql = ''.join(
                ['SELECT country_code FROM allCountries WHERE name=? OR asciiname=? OR alternatenames LIKE "%', city,'%"'])
            self.cur.execute(sql, [city, city])
        res = self.cur.fetchone()
        if res is not None:
            self.cache[city] = res[0]
            return res[0]
        else:
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

    def city2countryinfo(self, city):
        if city is None:
            return None
        city = city.strip()
        if city in self.cache:
            code = self.cache[city]
            return self.isocode2countryinfo(code)
        else:
            code = self.city2CountryGoogle(city)
            #logging.warning('code city: %s from Google: %s' % (city, str(code)))
            if code is not None:
                return self.isocode2countryinfo(code)
            else:
                code = self.city2CountryGeobase(city)
                #logging.warning('code city: %s from DB: %s' % (city, str(code)))
                if code is not None:
                    return self.isocode2countryinfo(code)
                else:
                    return None

    def country2iso(self, country):
        country = country.strip().lower()
        if country not in self.cache3:
            fld_list = ['ISO']
            res = self._get('countryInfo', fld_list, 'lower(Country)=?', (country,))
            if res is not None:
                code = res[0]['ISO']
                self.cache3[country] = code
                return code
            return None
        else:
            return self.cache3[country]

    def city2insee_code(self, city):
        city = city.lower()
        if city in self.insee_cache:
            return self.insee_cache[city]
        res = self.get_insee_code_from_service(city)
        if 'error' in res:
            return None
        code = None
        match = False
        if 'results' in res and type(res['results']) is list:
            for row in res['results']:
                if not match:
                    if city == row[1].lower():
                        code = row[0]
                        match = True
                    if city in row[0].lower():
                        code = row[0]
                        match = True
                self.insee_cache[row[1].lower()] = row[0]
            return code
        return None

    def get_insee_code_from_service(self, city):
        cities = [city]
        url = 'https://www.dcode.fr/api/'
        data = {'tool': 'insee-french-city-code', 'list': '\n'.join(cities), 'cities': True}
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
            'referer': 'https://www.dcode.fr/insee-french-city-code',
            'x-requested-with': 'XMLHttpRequest'
        }
        try:
            res = requests.post(url, data=data, headers=headers, verify=certifi.where())
            if res.status_code == 200:
                d = json.loads(res.text)
                if 'results' in d:
                    return {'results': d['results']}
            else:
                return {'error': res.text}
        except Exception, ex:
            return {'error': ex.message}


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

    def _serialise_dict(self, val):
        if type(val) is dict:
            return json.dumps(val)
        else:
            return val




class PgSQLStoreGcd(PgSQLStore):

    company_description_cache = {}

    def __init__(self, config_file=None, log_is_enabled=False):
        super(self.__class__, self).__init__(config_file=None, log_is_enabled=False)

    def get_jobs(self, job_board_id, offset=0, limit=1000):
        sql = """SELECT jj.* FROM v_jobs_for_feeds j
                INNER JOIN jobs jj on j.job_id=jj.id
                INNER join employers e ON jj.employer_id=e.id
              WHERE j.job_board_id = {google_job_board_id}
                AND e.id = jj.employer_id ORDER BY jj.id OFFSET {offset} LIMIT {limit}
              """.format(google_job_board_id=job_board_id, offset=offset, limit=limit)
        field_list = self._get_fld_list('jobs')
        return self._getraw(sql, field_list, None)

    def count_jobs(self, job_board_id):
        sql = """SELECT count(jj.id) AS count FROM v_jobs_for_feeds j
                INNER JOIN jobs jj on j.job_id=jj.id
                INNER join employers e ON jj.employer_id=e.id
              WHERE j.job_board_id = {google_job_board_id}
                AND e.id = jj.employer_id
              """.format(google_job_board_id=job_board_id)
        return self._getraw(sql, ['count'], None)[0]['count']

    def refresh_view(self):
        self.dbopen()
        self.cur.execute("refresh materialized view v_jobs_for_feeds")
        self.conn.commit()

    def get_jobs_for_delete(self, job_board_id, offset=0, limit=1000):
        sql = """SELECT * FROM jobs where attributes->>'google_cd_job_name' NOTNULL
                AND jobs.id NOT IN (SELECT job_id FROM v_jobs_for_feeds WHERE job_board_id={google_job_board_id})
                ORDER BY id OFFSET {offset} LIMIT {limit}""".format(google_job_board_id=job_board_id, offset=offset, limit=limit)
        field_list = self._get_fld_list('jobs')
        return self._getraw(sql, field_list, None)

    def count_jobs_for_delete(self, job_board_id):
        sql = """SELECT count(*) AS count FROM jobs where attributes->>'google_cd_job_name' NOTNULL
                AND jobs.id NOT IN (SELECT job_id FROM v_jobs_for_feeds WHERE job_board_id={google_job_board_id})""".format(google_job_board_id=job_board_id)
        return self._getraw(sql, ['count'], None)[0]['count']

    def getEmployers(self, employer_id=None):
        table = '.'.join([self.schema, self.employers_table])
        if employer_id is not None:
            employers = self._get(table, field_list=None, where='id=%s', data=(employer_id,))
        else:
            employers = self._get(table, field_list=None, where='TRUE', data=None)
        return employers

    def save_gcd_company_name(self, employer, name):
        sql = 'UPDATE employers SET metadata=%s WHERE id=%s'
        employer['metadata']['google_cd_name'] = name
        self.execute(sql, [self._serialise_dict(employer['metadata']), employer['id']])
        return employer['metadata']

    def save_gcd_job_name(self, job, name):
        sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
        job['attributes']['google_cd_job_name'] = name
        self.execute(sql, [self._serialise_dict(job['attributes']), job['id']])
        return job['attributes']

    def rm_gcd_job_name(self, job):
        sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
        del job['attributes']['google_cd_job_name']
        self.execute(sql, [self._serialise_dict(job['attributes']), job['id']])
        return job['attributes']

    def save_gcd_job_name_batch(self, jobs, attributes):
        self.dbopen()
        for job in jobs:
            job_id = int(job[u'requisitionId'])
            attributes[job_id]['google_cd_job_name'] = job[u'name']
            sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
            self.cur.execute(sql, [self._serialise_dict(attributes[job_id]), job_id])
        self.conn.commit()

    def rm_gcd_job_name_batch(self, jobs, attributes):
        self.dbopen()
        for job in jobs:
            job_id = int(job['requisitionId'])
            del attributes[job_id]['google_cd_job_name']
            sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
            self.cur.execute(sql, [self._serialise_dict(attributes[job_id]), job_id])
        self.conn.commit()

    def get_job_board(self, board_id, fld_list=None):
        table = '.'.join([self.schema, self.job_boards_table])
        boards = self._get(table, field_list=fld_list, where='id=%s', data=[int(board_id)])
        if boards is not None and len(boards) > 0:
            return boards[0]
        return None

    def get_company_description(self, employer):
        if employer['id'] in self.company_description_cache:
            return self.company_description_cache[employer['id']]
        self._setdb()
        self.dbopen()
        table = '.'.join([self.schema, 'company'])
        res = self._get(table, ['description'], "profile->>'uid'=%s", [employer['uid']])
        self.dbclose()
        self._resetdb()
        if res:
            description = res[0]['description']
            self.company_description_cache[employer['id']] = description
            return description
        return None

    def update_jobs_attributes(self, attributes):
        self.dbopen()
        for job_id in attributes.keys():
            sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
            self.cur.execute(sql, [self._serialise_dict(attributes[job_id]), job_id])
        self.conn.commit()

    def get_jobs_for_remove(self, job_board_id, offset=0, limit=1000):
        sql = """SELECT * FROM jobs where attributes->>'google_index_api' NOTNULL
                AND jobs.id NOT IN (SELECT job_id FROM v_jobs_for_feeds WHERE job_board_id={google_job_board_id})
                ORDER BY id OFFSET {offset} LIMIT {limit}""".format(google_job_board_id=job_board_id, offset=offset, limit=limit)
        field_list = self._get_fld_list('jobs')
        return self._getraw(sql, field_list, None)

    def count_jobs_for_remove(self, job_board_id):
        sql = """SELECT count(*) AS count FROM jobs where attributes->>'google_index_api' NOTNULL
                AND jobs.id NOT IN (SELECT job_id FROM v_jobs_for_feeds WHERE job_board_id={google_job_board_id})""".format(google_job_board_id=job_board_id)
        return self._getraw(sql, ['count'], None)[0]['count']

    def count_jobs_in_groups(self, job_groups):
        sql = """SELECT count(*) AS count FROM jobs WHERE job_group_id IN ({job_groups})""".format(job_groups=','.join(map(lambda i: str(i), job_groups)))
        return self._getraw(sql, ['count'], None)[0]['count']

    def get_jobs_in_groups(self, job_groups, offset, limit):
        sql = """SELECT * FROM jobs WHERE job_group_id IN ({job_groups})
                 ORDER BY id OFFSET {offset} LIMIT {limit}""".format(job_groups=','.join(map(lambda i: str(i), job_groups)), offset=offset, limit=limit)
        field_list = self._get_fld_list('jobs')
        return self._getraw(sql, field_list, None)



