import psycopg2
from ConfigParser import *
import json
from pprint import pprint
from transliterate import translit
import requests
import sqlite3
import urllib
import os
import sys
import io
import logging
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


class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    #schema = 'jobs'
    schema = 'public'
    jobs_table = 'jobs'
    employers_table = 'employers'
    job_groups_table = 'job_groups'
    employer_feed_settings_table = 'employer_feed_settings'
    dbname = None
    dbhost = None
    dbport = None
    dbuser = None
    dbpass = None

    pkey = 'external_unique_id' #primary key for jobs_table
    employer_id = None
    job_status = {
        'INACTIVE': {'code': 0, 'desc': 'Inactive'},
        'ACTIVE': {'code': 1, 'desc': 'Active'},
        'EXPIRED': {'code': 2, 'desc': 'Expired'},
        'MANUALLY_ACTIVE': {'code': 3, 'desc': 'Manually active'},
        'MANUALLY_INACTIVE': {'code': 4, 'desc': 'Manually inactive'}
    }

    def __init__(self, config_file=None, log_is_enabled=False):
        config_file = None
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
        print(sections)
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

        self.logger = logging.getLogger(__name__)
        ch = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
        self.logger.info('DB_HOST: %s' % self.dbhost)

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
        #print sql
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
        return res

    def _serialise_dict(self, val):
        if type(val) is dict:
            return json.dumps(val)
        else:
            return val

    def _getraw(self, sql, field_list, data=None, dbclose=False):
        self.dbopen()
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
            if len(field_list) != len(row):
                raise Exception('Number fields in fields list no match number columns in result!')
            d = {}
            for i in range(len(row)):
                d[field_list[i]] = row[i]
            res.append(d)
        if dbclose:
            self.dbclose()
        return res

    def _encode(self, value, encode):
        if hasattr(value, 'encode'):
            value = value.encode(encode)
        return value


class PgSQLStoreImport(PgSQLStore):

    slugs = {}
    expired_jobs_cache = {}
    job_groups_cache = {}

    def setPkey(self, pkey):
        self.pkey = pkey

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

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception, ex:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        if len(res) > 0:
            return res[0]
        else:
            return None

    def transliterate(self, str):
        try:
            str = translit(str.strip().lower(), reversed=True)
        except Exception, ex:
            str = str.strip().lower()
        return str

    def removeNonValidChars(self, str):
        c = []
        valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-()'
        for ch in str:
            if ch in valid:
                c.append(ch)
            else:
                c.append('-')
        return ''.join(c)

    def getJobSlug(self, title):
        return self.removeNonValidChars(self.transliterate(title))

    def getJobsSlugs(self, employer_id):
        self.logger.info('start get job slugs')
        self.slugs = {}
        job_slugs = self.getEmployerJobsSlugs(employer_id)
        if job_slugs:
            for row in job_slugs:
                key = row['slug']
                external_unique_id = row['external_unique_id'].strip().replace(' ', '').replace('\n', '').replace('\t', '')
                self.slugs[key] = external_unique_id
                if row['status'] != self.job_status['EXPIRED']['code']:
                    self.expired_jobs_cache[external_unique_id] = int(row['id'])
        self.logger.info('finish get job slugs')
        self.logger.info('lenght of expired_jobs_cache=%i' % len(self.expired_jobs_cache))
        self.logger.info('size of expired_jobs_cache=%s' % str(sys.getsizeof(self.expired_jobs_cache)))
        return self.slugs

    def getCompanySlug(self, employer_id):
        table = '.'.join([self.schema, self.employers_table])
        res = self._get(table, ['uid'], 'id=%s', [employer_id])
        if len(res) > 0:
            uid = res[0]['uid']
            conn = psycopg2.connect(dbname=self.dbname, user=self.dbuser, password=self.dbpass, host=self.dbhost, port=self.dbport)
            cur = conn.cursor()
            cur.execute("SELECT slug FROM company WHERE profile->>'uid' = %s", [uid])
            res = cur.fetchone()
            cur.close()
            conn.close()
            if len(res) > 0 and res[0] is not None:
                return res[0]
        return None

    def getEmployerJobsSlugs(self, employerId):
        table = '.'.join([self.schema, self.jobs_table])
        res = self._get(table, field_list=['external_unique_id', 'slug', 'status', 'id'], where='employer_id=%s', data=(employerId,))
        return res

    def getJobGroupId(self, employerId, categoryId, country):
        sql = 'SELECT id FROM job_groups WHERE employer_id=%s AND category_id=%s AND country=%s'
        res = self._getraw(sql, ['id'], [employerId, categoryId, country])
        if len(res) > 0:
            return res[0]['id']
        return None

    def getGroups(self, employerId):
        sql = 'SELECT id, category_id FROM job_groups WHERE employer_id=%s'
        res = self._getraw(sql, ['id', 'category_id'], [employerId])
        return res

    def getMapCategories(self, employerId):
        sql = 'SELECT employer_category, category_id FROM job_category_mappings WHERE employer_id=%s AND active=TRUE'
        res = self._getraw(sql, ['employer_category', 'category_id'], [employerId])
        return res

    def updateJobGroupsFields(self):
        sql = '''UPDATE job_groups SET jobs_amount=(SELECT jobs_amount FROM (SELECT jg.id, (CASE WHEN j.jobs_amount ISNULL THEN 0 ELSE j.jobs_amount END) FROM job_groups jg LEFT JOIN
                (SELECT job_group_id,  count(*) AS jobs_amount FROM jobs WHERE job_group_id NOTNULL GROUP BY job_group_id) AS j ON j.job_group_id=jg.id) t1 WHERE job_groups.id=t1.id),
                cv_total=(SELECT cv_total FROM (SELECT jg.id, (CASE WHEN j.cv_total ISNULL THEN 0 ELSE j.cv_total END) FROM job_groups jg LEFT JOIN
                  (SELECT job_group_id,  sum(cv) AS cv_total FROM jobs WHERE job_group_id NOTNULL GROUP BY job_group_id) AS j ON j.job_group_id=jg.id) t2 WHERE job_groups.id=t2.id),
                jobs = (SELECT array(SELECT id::VARCHAR FROM jobs WHERE job_group_id=job_groups.id)), job_budget=(SELECT (CASE WHEN sum(budget) NOTNULL THEN sum(budget) ELSE 0 END)
                FROM jobs WHERE jobs.job_group_id=job_groups.id AND jobs.budget NOTNULL)'''
        self.dbopen()
        self.cur.execute(sql)
        self.conn.commit()
        self.dbclose()

    def createTemporaryTable(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        self.dropTemporaryTable(temporaryTable)
        self.cur.execute(' '.join(['CREATE TABLE IF NOT EXISTS', temp_table, 'AS SELECT * FROM ', jobs_table, 'LIMIT 1;']))
        self.cur.execute(' '.join(['DELETE FROM', temp_table, ';']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN created_at SET DEFAULT now()']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN updated_at SET DEFAULT now()']))
        self.createTemporaryKeyTable(temporaryTable)

    def createTemporaryKeyTable(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable + 'key'])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        self.cur.execute(
            ' '.join(['CREATE TABLE IF NOT EXISTS', temp_table, 'AS SELECT ', self.pkey, ' FROM ', jobs_table, 'LIMIT 1;']))
        self.cur.execute(' '.join(['DELETE FROM', temp_table, ';']))

    def insertItemToTemporaryTable(self, item, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        #table = '.'.join([self.schema, self.jobs_table])
        sql = ' '.join(['INSERT INTO', table, '(', ','.join(item.keys()), ') VALUES (', ','.join(['%s' for i in item.keys()]), ');'])
        values = map(lambda val: self._serialise_dict(val), item.values())
        try:
            self.cur.execute(sql, values)
        except UnicodeEncodeError, ex:
            values = map(lambda val: self._encode(self._serialise_dict(val), 'utf-8'), item.values())
            self.cur.execute(sql, values)
        self.insertKeyToTemporaryTable(item, temporaryTable)

    def insertKeyToTemporaryTable(self, item, temporaryTable):
        table = '.'.join([self.schema, temporaryTable+'key'])
        #table = '.'.join([self.schema, self.jobs_table])
        sql = ' '.join(['INSERT INTO', table, '(', self.pkey, ') VALUES (%s);'])
        values = [item[self.pkey]]
        self.cur.execute(sql, values)

    def mergeTables(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        temp_table_key = '.'.join([self.schema, temporaryTable+'key'])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        pkey = self.pkey

        #adding new
        fld_list = 'uid,external_id,external_unique_id,employer_id,job_group_id,url,title,city,state,country,description,job_type,company,category,posted_at,expire_date,status,budget,budget_spent,created_at,updated_at,slug,company_slug,attributes'.split(',')
        self.cur.execute(' '.join(["SELECT setval('", 'jobs_id_seq', "', (SELECT max(id) FROM", jobs_table, '));']))
        try:
            sql = ' '.join(['INSERT INTO', jobs_table, '(', ','.join(fld_list), ') SELECT', ','.join(fld_list), 'FROM', temp_table, 'WHERE', pkey, 'IN (SELECT', pkey, 'FROM', temp_table, 'EXCEPT SELECT', pkey, 'FROM', jobs_table, ');'])
            self.cur.execute(sql)
        except Exception, ex:
            sql = ' '.join(['INSERT INTO', jobs_table, '(', ','.join(fld_list), ') SELECT', ','.join(fld_list), 'FROM', temp_table, 'WHERE', pkey, 'IN (SELECT', pkey, 'FROM', temp_table, 'EXCEPT SELECT', pkey, 'FROM', jobs_table, ') AND slug NOT IN (SELECT slug FROM',jobs_table,'WHERE employer_id=',str(self.employer_id), ');'])
            self.cur.execute(sql)

        #updating job that already exists as expired
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,expire_date,updated_at,status'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.',pkey, ' LIMIT 1)']))
        sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM',jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')','AND employer_id=', str(self.employer_id), 'AND status=', str(self.job_status['EXPIRED']['code']), ';'])
        # 'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey) AND jobs_table.status=2'
        self.cur.execute(sql)

        #updating existing
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,expire_date,updated_at'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.', pkey, ' LIMIT 1)']))
        sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM', jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', 'AND employer_id=', str(self.employer_id), ';'])
        #'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey)'
        self.cur.execute(sql)

    def cleanTemporaryTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        self.cur.execute(' '.join(['DELETE FROM ', table]))

    def dropTemporaryTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        self.cur.execute(' '.join(['DROP TABLE IF EXISTS', table]))
        self.dropTemporaryKeyTable(temporaryTable)

    def dropTemporaryKeyTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable + 'key'])
        self.cur.execute(' '.join(['DROP TABLE IF EXISTS', table]))

    def recalculateJobGroupBudgets(self, k, employer_id):
        sql = "UPDATE job_groups jg SET monthly_budget=(SELECT %s * count(*) FROM jobs j WHERE j.status IN (1,3) AND j.job_group_id=jg.id) WHERE employer_id =%s"
        self.dbopen()
        self.cur.execute(sql, (k, employer_id))
        self.conn.commit()

    def set_expired(self, temporaryTable):
        jobs_table = '.'.join([self.schema, self.jobs_table])
        sql = "CREATE TEMP TABLE {table} (id BIGINT NOT NULL, status INTEGER, CONSTRAINT {table}_pkey PRIMARY KEY (id))".format(table=temporaryTable)
        self.dbopen()
        self.cur.execute(sql)
        self.conn.commit()
        buffer = io.StringIO()
        #pprint(self.jobs_status_cache)
        lines = []
        for external_unique_id in self.expired_jobs_cache.keys():
            if self.expired_jobs_cache[external_unique_id] > 0:
                line = ','.join([unicode(self.expired_jobs_cache[external_unique_id]), unicode(self.job_status['EXPIRED']['code'])])
                lines.append(line)
        if len(lines) > 0:
            lenght = len(lines) - 1
            for i in range(0, lenght+1):
                if i < lenght:
                    buffer.write(lines[i]+os.linesep)
                else:
                    buffer.write(lines[i])
            buffer.seek(0)
            self.cur.copy_from(buffer, temporaryTable, sep=',', columns=['id', 'status'])
            self.cur.execute("ALTER TABLE {table} DISABLE TRIGGER ALL".format(table=jobs_table))
            self.conn.commit()
            sql = "UPDATE {jobs_table} a SET status=b.status FROM {temporaryTable} b WHERE a.id=b.id".format(jobs_table=jobs_table,temporaryTable=temporaryTable)
            self.cur.execute(sql)
            self.conn.commit()
            self.cur.execute("ALTER TABLE {table} ENABLE TRIGGER ALL".format(table=jobs_table))
            self.conn.commit()
            #self.cur.execute("TRUNCATE TABLE {temporaryTable}".format(temporaryTable=temporaryTable))
            #self.conn.commit()
            #self.conn.autocommit = True
            #self.cur.execute("VACUUM ANALYZE {jobs_table}".format(jobs_table=self.jobs_table))
            #self.conn.commit()

    def create_job_group(self, employer_id,  name, category_id, country_code):
        key = '-'.join([str(employer_id), name, str(category_id), country_code])
        if key in self.job_groups_cache:
            return self.job_groups_cache[key]
        body = {
            'appLimit': "max",
            'applications': 0,
            'applyTotal': 0,
            'categoryId': category_id,
            'country': country_code,
            'cpaMax': 0,
            'cpaTarget': 0,
            'cvTotal': 0,
            'defaultJobBudget': "100",
            'employer': "/api/employers/{employer_id}".format(employer_id=employer_id),
            'jobBoards': True,
            'jobBudget': 0,
            'jobs': [],
            'jobsAmount': 0,
            'monthlyBudget': "1000",
            'monthlyBudgetProgress': "0",
            'name': name,
            'objectives': "application",
            'programmatic': True,
            'socialMedia': True,
            'enabled': False
        }
        table = '.'.join([self.schema, 'job_groups'])
        res = self._get(table=table, field_list=None, where='employer_id=%s AND name=%s AND category_id=%s AND country=%s', data=[employer_id,  name, category_id, country_code])
        if len(res) > 0:
            id = res[0]['id']
            self.job_groups_cache[key] = id
            return id
        else:
            token = 'reJwHFHwH1dxl3CS01oMIUQDKfaGzCb1NUZ0KqLGYpUvoezpRZxsIDIsa4kkHWHd'
            payload = body
            headers = {'Authorization': token, 'Content-Type': 'application/json'}
            if self.dbname == 'xtramile_prod':
                domain = 'io'
            elif self.dbname == 'xtramile_dev':
                domain = 'tech'
            url = 'https://service.xtramile.{domain}/api/jobGroups'.format(domain=domain)
            res = requests.post(url, json=payload, headers=headers, verify=certifi.where())
            if res.status_code == 201:
                id = json.loads(res.text)['id']
                if type(id) is int:
                    self.job_groups_cache[key] = id
                    return id
                else:
                    pprint(id)
                    return None
            else:
                pprint(res.text)
                return None




class PgSQLStoreJobsUpdate(PgSQLStore):

    company_id = None

    def setPkey(self, pkey):
        self.pkey = pkey

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception, ex:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        if len(res) > 0:
            return res[0]
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
        values = map(lambda val: self._serialise_dict(val), item.values())
        self.cur.execute(sql, values)

    def mergeTables(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        jobs_table = '.'.join([self.schema, self.jobs_table])
        pkey = self.pkey

        #adding new
        fld_list = 'uid,external_id,external_unique_id,employer_id,job_group_id,url,title,city,state,country,description,job_type,company,category,posted_at,expire_date,status,budget,budget_spent,created_at,updated_at,slug,company_slug,keywords,attributes,is_editable'.split(',')
        sql = ' '.join(['INSERT INTO', jobs_table, '(', ','.join(fld_list), ') SELECT', ','.join(fld_list), 'FROM', temp_table, 'WHERE', pkey, 'IN (SELECT', pkey, 'FROM', temp_table, 'EXCEPT SELECT', pkey, 'FROM', jobs_table, ');'])
        self.cur.execute(' '.join(["SELECT setval('", 'jobs_id_seq', "', (SELECT max(id) FROM", jobs_table, '));']))
        self.cur.execute(sql)

        #marking deleted as archive
        if self.company_id is not None:
            sql = ' '.join(['UPDATE', jobs_table, "SET status=", str(self.job_status['EXPIRED']['code']), "WHERE", pkey, 'IN (SELECT', pkey, 'FROM', jobs_table, 'EXCEPT SELECT', pkey, 'FROM', temp_table, ') AND employer_id=', str(self.employer_id), ';'])
        else:
            sql = ' '.join(['UPDATE', jobs_table, "SET status=", str(self.job_status['EXPIRED']['code']), "WHERE", pkey, 'IN (SELECT',pkey, 'FROM', jobs_table, 'EXCEPT SELECT', pkey, 'FROM', temp_table, ") AND url like '%jobs.xtramile.io%';"])
        self.cur.execute(sql)

        #updating job that already exists as expired
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,updated_at,status,slug,company_slug,keywords,attributes,is_editable'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.',pkey, ')']))
        if self.company_id is None:
            sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM',jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')',"AND url like '%jobs.xtramile.io%'", 'AND status=', str(self.job_status['EXPIRED']['code']), ';'])
        else:
            sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM',jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', 'AND employer_id=', str(self.employer_id), 'AND status=', str(self.job_status['EXPIRED']['code']), ';'])
        # 'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey) AND jobs_table.status=2'
        self.cur.execute(sql)

        #updating existing
        upd_fld_list = 'employer_id,external_unique_id,url,title,city,state,country,description,job_type,company,category,posted_at,updated_at,status,slug,company_slug,keywords,attributes,is_editable,attributes'.split(',')
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', jobs_table, '.', pkey, ')']))
        if self.company_id is None:
            sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM',jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', "AND url like '%jobs.xtramile.io%'", ';'])
        else:
            sql = ' '.join(['UPDATE', jobs_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM', jobs_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', 'AND employer_id=', str(self.employer_id), ';'])
        #'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey)'
        self.cur.execute(sql)

    def dropTemporaryTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        self.cur.execute(' '.join(['DROP TABLE IF EXISTS', table]))



class PgSQLStoreCandidates(PgSQLStore):

    table = ''
    fld_list = []
    upd_fld_list = []

    def setPkey(self, pkey):
        self.pkey = pkey

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception, ex:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        if len(res) > 0:
            return res[0]
        else:
            return None

    def getJobByExternalUniqueid(self, id):
        table = '.'.join([self.schema, self.jobs_table])
        self.dbopen()
        res = self._get(table=table, field_list=None, where='external_unique_id=%s', data=[id])
        if len(res) > 0:
            return res[0]
        return None

    def createTemporaryTable(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        src_table = '.'.join([self.schema, self.table])
        self.dropTemporaryTable(temporaryTable)
        self.cur.execute(' '.join(['CREATE TABLE IF NOT EXISTS', temp_table, 'AS SELECT * FROM ', src_table, 'LIMIT 1;']))
        self.cur.execute(' '.join(['DELETE FROM', temp_table, ';']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN created_at SET DEFAULT now()']))
        self.cur.execute(' '.join(['ALTER TABLE', temp_table, 'ALTER COLUMN updated_at SET DEFAULT now()']))

    def insertItemToTemporaryTable(self, item, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        #table = '.'.join([self.schema, self.jobs_table])
        sql = ' '.join(['INSERT INTO', table, '(', ','.join(item.keys()), ') VALUES (', ','.join(['%s' for i in item.keys()]), ');'])
        values = map(lambda val: self._serialise_dict(val), item.values())
        self.cur.execute(sql, values)

    def mergeTables(self, temporaryTable):
        temp_table = '.'.join([self.schema, temporaryTable])
        src_table = '.'.join([self.schema, self.table])
        pkey = self.pkey

        #adding new
        fld_list = self.fld_list
        sql = ' '.join(['INSERT INTO', src_table, '(', ','.join(fld_list), ') SELECT', ','.join(fld_list), 'FROM', temp_table, 'WHERE', pkey, 'IN (SELECT', pkey, 'FROM', temp_table, 'EXCEPT SELECT', pkey, 'FROM', src_table, ');'])
        self.cur.execute(' '.join(["SELECT setval('", 'jobs_id_seq', "', (SELECT max(id) FROM", src_table, '));']))
        self.cur.execute(sql)

        #deletind if not exist
        #sql = ' '.join(['DELETE FROM', src_table, "WHERE", pkey, 'IN (SELECT', pkey, 'FROM', src_table, 'EXCEPT SELECT', pkey, 'FROM', temp_table, ');'])
        #self.cur.execute(sql)

        #updating existing
        upd_fld_list = self.fld_list
        set_list = []
        for fld in upd_fld_list:
            set_list.append(' '.join([fld, '=(', 'SELECT', fld, 'FROM', temp_table, 'WHERE', temp_table, '.', pkey, '=', src_table, '.', pkey, ')']))
        sql = ' '.join(['UPDATE', src_table, 'SET', ','.join(set_list), 'WHERE', pkey, 'IN (SELECT old.', pkey, 'FROM', src_table, 'AS old,', temp_table, 'WHERE old.', pkey, '=', temp_table, '.', pkey, ')', ';'])
        #'UPDATE jobs_table SET title = (SELECT title FROM temp_table WHERE temp_table.pkey = jobs_table.pkey) WHERE pkey IN(SELECT old.pkey FROM jobs_table AS old, temp_table WHERE old.pkey = temp_table.pkey)'
        self.cur.execute(sql)

    def dropTemporaryTable(self, temporaryTable):
        table = '.'.join([self.schema, temporaryTable])
        self.cur.execute(' '.join(['DROP TABLE IF EXISTS', table]))


