#!/usr/bin/env python
#coding=utf-8

import time


import requests
import json
import sqlite3
import urllib

from pprint import pprint

class Geocode:

    conn = None
    cur = None
    dbname1 = '/home/user1/geobase.sqlite'
    dbname2 = '/home/ubuntu/geobase.sqlite'
    gooogleApiUrl = 'https://maps.googleapis.com/maps/api/geocode/json?'
    cache = {} #cache for mapping city_name -> country_code
    cache2 = {} #cache for mapping country_code -> country_info


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
            if code is not None:
                return self.isocode2countryinfo(code)
            else:
                code = self.city2CountryGeobase(city)
                if code is not None:
                    return self.isocode2countryinfo(code)
                else:
                    return None

geocode = Geocode()


cities = '''Hauts-de-Seine,Val-de-Marne,Luxembourg,Hong Kong,Luxembourg,Frankfurt am Main,Hauts-de-Seine,
Praha,Val-de-Marne,London,Hong Kong,Val-de-Marne,Praha,Prague,Bas-Rhin,Val-de-Marne,
Yvelines,Côte-d\'Or,Finistère,Casablanca et régions,Praha 5 - Stodůlky',Casablanca et régions,České Budějovice - Krajinská,
Praha 5 - Stodůlky,Praha 7 - U Průhonu 32,Praha 9 - Náměstí OSN,Praha 5 - Stodůlky,Ostrava-Poruba - Náměstí V. Vacka,
Praha 9 - Náměstí OSN,Praha 9 - Náměstí OSN,Praha 9 - Náměstí OSN,Praha 9 - Náměstí OSN,Praha 1 - Václavské nám,
Praha 1 - Václavské nám,Praha 1 - Václavské nám,Praha 9 - Náměstí OSN,Praha 1 - Na Příkopě,Praha 1 - Na Příkopě,
Karlovy Vary - Bělehradská,Praha 9 - Náměstí OSN,Praha 5 - Stodůlky,Casablanca et régions,Casablanca et régions,
Praha 5 - Stodůlky,Praha 1 - Václavské nám.,Casablanca et régions,Praha 5 - Stodůlky,Praha 1 - Václavské nám.,
Praha 1 - Václavské nám.,Benešov u Prahy - Tyršova,Říčany u Prahy - 17. listopadu,Praha 1 - Václavské nám.,
Praha 5 - Stodůlky,Praha 1 - Václavské nám.,Praha 5 - Stodůlky,Praha 5 - Stodůlky,Praha 5 - Stodůlky,Praha 9 - Vysočany - Freyova,
Praha 5 - Stodůlky,Náchod - Palackého,Plzeň - Goethova,Praha 1 - Václavské nám.,Jablonec nad Nisou - Jehlářská,
Praha 5 - Stodůlky,Praha 5 - Stodůlky,Casablanca et régions,Praha 5 - Stodůlky,Benešov u Prahy - Tyršova,Rokycany - Jiráskova
'''
cities = map(lambda s: s.strip(), cities.split(','))

for city in cities:
    print city
    start = time.clock()
    print geocode.city2countryinfo(city)
    stop = time.clock()
    print 'spent time: %f' % ((stop - start)*1000)
    print '-' * 60

geocode.saveCache()


