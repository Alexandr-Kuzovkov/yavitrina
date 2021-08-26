#!/usr/bin/env python
#coding=utf-8

######################################################
# Create sqlite database from files from geoname.org
#####################################################

from pprint import pprint
import sqlite3
import logging
import os


######################################################################################################################
#http://download.geonames.org/export/dump/
#The main 'geoname' table has the following fields :
#---------------------------------------------------
#
# geonameid         : integer id of record in geonames database
# name              : name of geographical point (utf8) varchar(200)
# asciiname         : name of geographical point in plain ascii characters, varchar(200)
# alternatenames    : alternatenames, comma separated, ascii names automatically transliterated, convenience attribute from alternatename table, varchar(10000)
# latitude          : latitude in decimal degrees (wgs84)
# longitude         : longitude in decimal degrees (wgs84)
# feature class     : see http://www.geonames.org/export/codes.html, char(1)
# feature code      : see http://www.geonames.org/export/codes.html, varchar(10)
# country code      : ISO-3166 2-letter country code, 2 characters
# cc2               : alternate country codes, comma separated, ISO-3166 2-letter country code, 200 characters
# admin1 code       : fipscode (subject to change to iso code), see exceptions below, see file admin1Codes.txt for display names of this code; varchar(20)
# admin2 code       : code for the second administrative division, a county in the US, see file admin2Codes.txt; varchar(80)
# admin3 code       : code for third level administrative division, varchar(20)
# admin4 code       : code for fourth level administrative division, varchar(20)
# population        : bigint (8 byte int)
# elevation         : in meters, integer
# dem               : digital elevation model, srtm3 or gtopo30, average elevation of 3''x3'' (ca 90mx90m) or 30''x30'' (ca 900mx900m) area in meters, integer. srtm processed by cgiar/ciat.
# timezone          : the iana timezone id (see file timeZone.txt) varchar(40)
# modification date : date of last modification in yyyy-MM-dd format

# The table 'alternate names' :
# -----------------------------
# alternateNameId   : the id of this alternate name, int
# geonameid         : geonameId referring to id in table 'geoname', int
# isolanguage       : iso 639 language code 2- or 3-characters; 4-characters 'post' for postal codes and 'iata','icao' and faac for airport codes, fr_1793 for French Revolution names,  abbr for abbreviation, link for a website, varchar(7)
# alternate name    : alternate name or name variant, varchar(400)
# isPreferredName   : '1', if this alternate name is an official/preferred name
# isShortName       : '1', if this is a short name like 'California' for 'State of California'
# isColloquial      : '1', if this alternate name is a colloquial or slang term
# isHistoric        : '1', if this alternate name is historic and was used in the past

#####################################################################################################################

source_dir = '/home/user1/Downloads/geoname/'
allCountries = source_dir + 'allCountries.txt'
alternateNames = source_dir + 'alternateNames.txt'
countryInfo = source_dir + 'countryInfo.txt'
cities1000 = source_dir + 'cities1000.txt'        #: all cities with a population > 1000 or seats of adm div (ca 150.000), see 'geoname' table for columns
cities5000 = source_dir + 'cities5000.txt'         #: all cities with a population > 5000 or PPLA (ca 50.000), see 'geoname' table for columns
cities15000 = source_dir + 'cities15000.txt'       #: all cities with a population > 15000 or capitals (ca 25.000), see 'geoname' table for columns
admin1CodesASCII =  source_dir + 'admin1CodesASCII.txt'     #: names in English for admin divisions. Columns: code, name, name ascii, geonameid
admin2Codes  =  source_dir + 'admin2Codes.txt'       #: names for administrative subdivision 'admin2 code' (UTF8), Format : concatenated codes <tab>name <tab> asciiname <tab> geonameId


logger = logging.getLogger('make geobase')
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logger.setLevel(1)


dbname = '/home/user1/geobase.sqlite'
try:
    logger.info('removing file %s' % dbname)
    os.remove(dbname)
except Exception, ex:
    print ex
conn = sqlite3.connect(dbname)
cur = conn.cursor()

def get_lines(file):
    fh = open(file)
    num_lines = sum(1 for line in fh)
    fh.close()
    return num_lines


cur.execute('DROP TABLE IF EXISTS allCountries')
cur.execute('CREATE TABLE IF NOT EXISTS allCountries(geonameid INTEGER, name, asciiname, alternatenames, latitude REAL, longitude REAL, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population INTEGER, elevation INTEGER, dem, timezone, modification_date)')
logger.info('processing "allCountries"')

count = 0
num_lines = get_lines(allCountries)
f = open(allCountries, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO allCountries (geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS alternateNames')
cur.execute('CREATE TABLE IF NOT EXISTS alternateNames(alternateNameId INTEGER, geonameid INTEGER, isolanguage, alternate_name, isPreferredName, isShortName, isColloquial, isHistoric)')
logger.info('processing "alternateNames"')

count = 0
num_lines = get_lines(alternateNames)
f = open(alternateNames, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO alternateNames(alternateNameId, geonameid, isolanguage, alternate_name, isPreferredName, isShortName, isColloquial, isHistoric) VALUES(?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS countryInfo')
cur.execute('CREATE TABLE IF NOT EXISTS countryInfo(ISO, ISO3, ISO_Numeric INTEGER, fips, Country, Capital, Area INTEGER, Population INTEGER, Continent, tld, CurrencyCode, CurrencyName, Phone, Postal_Code_Format, Postal_Code_Regex, Languages, geonameid INTEGER, neighbours, EquivalentFipsCode)')
logger.info('processing "countryInfo"')

count = 0
num_lines = get_lines(countryInfo)
f = open(countryInfo, 'r')
for line in f:
    line = unicode(line, 'utf8')
    if line[0] == '#':
        continue
    row = line.split('\t')
    sql = 'INSERT INTO countryInfo(ISO, ISO3, ISO_Numeric, fips, Country, Capital, Area, Population, Continent, tld, CurrencyCode, CurrencyName, Phone, Postal_Code_Format, Postal_Code_Regex, Languages, geonameid, neighbours, EquivalentFipsCode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS cities1000')
cur.execute('CREATE TABLE IF NOT EXISTS cities1000(geonameid INTEGER, name, asciiname, alternatenames, latitude REAL, longitude REAL, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population INTEGER, elevation INTEGER, dem, timezone, modification_date)')
logger.info('processing "cities1000"')

count = 0
num_lines = get_lines(cities1000)
f = open(cities1000, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO cities1000 (geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS cities5000')
cur.execute('CREATE TABLE IF NOT EXISTS cities5000(geonameid INTEGER, name, asciiname, alternatenames, latitude REAL, longitude REAL, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population INTEGER, elevation INTEGER, dem, timezone, modification_date)')
logger.info('processing "cities5000"')

count = 0
num_lines = get_lines(cities5000)
f = open(cities5000, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO cities5000 (geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS cities15000')
cur.execute('CREATE TABLE IF NOT EXISTS cities15000(geonameid INTEGER, name, asciiname, alternatenames, latitude REAL, longitude REAL, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population INTEGER, elevation INTEGER, dem, timezone, modification_date)')
logger.info('processing "cities15000"')

count = 0
num_lines = get_lines(cities15000)
f = open(cities15000, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO cities15000 (geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS admin1CodesASCII')
cur.execute('CREATE TABLE IF NOT EXISTS admin1CodesASCII(code, name, name_ascii, geonameid INTEGER)')
logger.info('processing "admin1CodesASCII"')

count = 0
num_lines = get_lines(admin1CodesASCII)
f = open(admin1CodesASCII, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO admin1CodesASCII (code, name, name_ascii, geonameid) VALUES (?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 1000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()


cur.execute('DROP TABLE IF EXISTS admin2Codes')
cur.execute('CREATE TABLE IF NOT EXISTS admin2Codes(concatenated_codes, name, name_ascii, geonameid INTEGER)')
logger.info('processing "admin2Codes"')

count = 0
num_lines = get_lines(admin2Codes)
f = open(admin2Codes, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO admin2Codes (concatenated_codes, name, name_ascii, geonameid) VALUES (?,?,?,?)'
    cur.execute(sql, row)
    count += 1
    if count % 10000 == 0:
        logger.info('... line %i from %i' % (count, num_lines))
conn.commit()
f.close()


cur.close()
conn.close()




