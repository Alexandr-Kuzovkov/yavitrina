#!/usr/bin/env python
#coding=utf-8


from pprint import pprint
import sqlite3


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

allCountries = '/home/user1/Downloads/allCountries.txt'
alternateNames = '/home/user1/Downloads/alternateNames.txt'
countryInfo = '/home/user1/Downloads/countryInfo.txt'


dbname = '/home/user1/geobase.sqlite'
conn = sqlite3.connect(dbname)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS allCountries')
cur.execute('CREATE TABLE IF NOT EXISTS allCountries(geonameid INTEGER, name, asciiname, alternatenames, latitude REAL, longitude REAL, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population INTEGER, elevation INTEGER, dem, timezone, modification_date)')

f = open(allCountries, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO allCountries (geonameid, name, asciiname, alternatenames, latitude, longitude, feature_class, feature_code, country_code, cc2, admin1_code, admin2_code, admin3_code, admin4_code, population, elevation, dem, timezone, modification_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS alternateNames')
cur.execute('CREATE TABLE IF NOT EXISTS alternateNames(alternateNameId INTEGER, geonameid INTEGER, isolanguage, alternate_name, isPreferredName, isShortName, isColloquial, isHistoric)')

f = open(alternateNames, 'r')
for line in f:
    line = unicode(line, 'utf8')
    row = line.split('\t')
    sql = 'INSERT INTO alternateNames(alternateNameId, geonameid, isolanguage, alternate_name, isPreferredName, isShortName, isColloquial, isHistoric) VALUES(?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
conn.commit()
f.close()

cur.execute('DROP TABLE IF EXISTS countryInfo')
cur.execute('CREATE TABLE IF NOT EXISTS countryInfo(ISO, ISO3, ISO_Numeric INTEGER, fips, Country, Capital, Area INTEGER, Population INTEGER, Continent, tld, CurrencyCode, CurrencyName, Phone, Postal_Code_Format, Postal_Code_Regex, Languages, geonameid INTEGER, neighbours, EquivalentFipsCode)')

f = open(countryInfo, 'r')
for line in f:
    line = unicode(line, 'utf8')
    if line[0] == '#':
        continue
    row = line.split('\t')
    sql = 'INSERT INTO countryInfo(ISO, ISO3, ISO_Numeric, fips, Country, Capital, Area, Population, Continent, tld, CurrencyCode, CurrencyName, Phone, Postal_Code_Format, Postal_Code_Regex, Languages, geonameid, neighbours, EquivalentFipsCode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
    cur.execute(sql, row)
conn.commit()
f.close()
cur.close()
conn.close()




