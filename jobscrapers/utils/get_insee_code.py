#!/usr/bin/env python
#coding=utf-8

######################################################
# Get INSEE code for France cities
######################################################

import requests
import certifi
from pprint import pprint
import json
import urllib
import sqlite3


'''
cities = ['Paris', 'Metz', 'Caen', 'Touluse']
cities = ['metz']
url = 'https://www.dcode.fr/api/'

data = {'tool': 'insee-french-city-code', 'list': '\n'.join(cities), 'cities': True}
headers = {
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
    'referer': 'https://www.dcode.fr/insee-french-city-code',
    'x-requested-with': 'XMLHttpRequest'
}

res = requests.post(url, data=data, headers=headers, verify=certifi.where())
d = json.loads(res.text)
pprint(d)
'''

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
        pprint(res)
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


geocode = Geocode()
geocode.loadCache()

code = geocode.city2insee_code('Berlin')
pprint(code)

geocode.saveCache()



