#coding=utf-8
import requests
import json
import sqlite3
import urllib
import psycopg2
from jobscrapers.dbacc import db
import logging
import json

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
                        raise Exception(str(self.__class__) + 'Can\'t open database file!')
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
        country = country.strip().lower().capitalize()
        if country not in self.cache3:
            fld_list = ['ISO']
            res = self._get('countryInfo', fld_list, 'Country=?', (country,))
            if res is not None:
                code = res[0]['ISO']
                self.cache3[country] = code
                return code
            return None
        else:
            return self.cache3[country]

    def get_countries(self):
        fld_list = ['Country']
        res = self._get('countryInfo', fld_list, '1')
        return map(lambda i: i['Country'].lower(), res)



class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    #schema = 'jobs'
    schema = 'public'
    jobs_table = 'jobs'
    employers_table = 'employers'
    employer_feed_settings_table = 'employer_feed_settings'
    employers_table = 'employers'
    dbname = db['dbname']
    dbhost = db['dbhost']
    dbport = db['dbport']
    dbuser = db['dbuser']
    dbpass = db['dbpass']

    def __init__(self, log_is_enabled=False):PgSQLStore
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


class PgSQLStoreScrapers(PgSQLStore):

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception, ex:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        self.dbclose()
        if len(res) > 0:
            return res[0]
        else:
            return None

    def changeFeedUpdated(self, employer, utime):
        if employer is None:
            return
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        sql = ' '.join(['UPDATE', table, 'SET feed_updated_at=%s', 'WHERE id=%s'])
        self.cur.execute(sql, (utime, employer['id']))
        self.conn.commit()
        self.dbclose()

    def updateFeedUrl(self, employer, url):
        if employer is None:
            return
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        sql = ' '.join(['UPDATE', table, 'SET feed_url=%s', 'WHERE id=%s'])
        self.cur.execute(sql, (url, employer['id']))
        self.conn.commit()
        self.dbclose()

    def getEmployerdByName(self, employer_name, fld_list=None):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=fld_list, where='name=%s', data=[employer_name])
        if len(employers) > 0:
            return employers[0]
        return None




