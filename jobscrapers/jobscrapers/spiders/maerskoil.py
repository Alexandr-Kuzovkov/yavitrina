# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
from pprint import pprint
from scrapy_splash import SplashRequest
import json
from jobscrapers.extensions import Geocode
from jobscrapers.items import JobItem
import pkgutil

class MaerskoilSpider(scrapy.Spider):

    name = "maerskoil"
    publisher = "MaerksOil"
    publisherurl = 'http://www.maerskoil.com/'
    #lua_src = pkgutil.get_data('jobscrapers', 'lua/swissre.lua')
    geocode = Geocode()

    def start_requests(self):
        allowed_domains = ["http://www.maerskoil.com/", "https://jobsearch.maersk.com/"]
        urls = [
            'http://www.maerskoil.com/Career/Vacancies/Pages/vacancies.aspx'
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.getJson)


    def getJson(self, response):
        url = 'https://jobsearch.maersk.com/vacancies/jobsearch_json?component=UNREGISTERED2POSTING&xml=%3CSC%3E%3Ci%3E%3CSE%3EZ_FRO_POST_INFO_COMPANY_LB%3C%2FSE%3E%3CSM%3E0021%3C%2FSM%3E%3CQ%3E%3Ci%3E%3CO%3E%3C%2FO%3E%3CL%3E00000011%3C%2FL%3E%3C%2Fi%3E%3C%2FQ%3E%3C%2Fi%3E%3C%2FSC%3E'
        yield scrapy.Request(url=url, callback=self.getJobLinks)


    def getJobLinks(self, response):
        s = response.text
        s = s[s.find('hit')+7:s.find('updated')-5]
        #text = '''[{'PH' : 'Technology &amp; Innovation Portfolio Manager','PG' : '005056A569991ED784E4786D318E5332','ES' : '','PB' : '2017-03-27','FA' : 'Commercial/Sales/Business Development','CO' : 'Denmark','CI' : 'Copenhagen','LO' : '12.590473','LA' : '55.68819'},{'PH' : 'SAP Business Process Manager (FI)','PG' : '005056A569991EE787D5B463350F9D9B','ES' : '','PB' : '2017-04-11','FA' : 'Finance/Accounting','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Senior Production Technologist','PG' : '005056A52F591ED786C025941315A84B','ES' : '','PB' : '2017-04-05','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Production Technologist','PG' : '005056A52F591ED786C03D08B38D0851','ES' : '','PB' : '2017-04-05','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Senior Production Technologist','PG' : '005056A52F591ED786C05BD658718859','ES' : '','PB' : '2017-04-05','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Production Technologist','PG' : '005056A52F591ED786C07236D35B4860','ES' : '','PB' : '2017-04-05','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Well Services Engineer – be the rig focal point of expertise','PG' : '005056A569991EE786D4DA56ACB4A9E6','ES' : '','PB' : '2017-04-06','FA' : 'Wells','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Production Operator','PG' : '005056A52F591ED786D5EED53F20D24F','ES' : '','PB' : '2017-04-06','FA' : 'Oil Operations/Production','CO' : 'Kazakhstan','CI' : 'Aktau','LO' : '51.17123','LA' : '43.635609'},{'PH' : 'Asset Wells Manager - take part in the development of technical/operational excellence and innovation ','PG' : '005056A52F591ED786D7CD157568533A','ES' : '','PB' : '2017-04-06','FA' : 'Wells','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Maintenance Campaign Coordinator','PG' : '005056A52F591EE787BA89746F7B1CB9','ES' : '','PB' : '2017-04-10','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Head of Integrated Solutions','PG' : '005056A569991EE786ED89F0C73799C7','ES' : '','PB' : '2017-04-07','FA' : 'Oil Operations/Production','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Legal Counsel','PG' : '005056A5088A1ED786F2AE0387215DE8','ES' : '','PB' : '2017-04-07','FA' : 'Legal','CO' : 'United Kingdom','CI' : 'Aberdeen','LO' : '-2.083679','LA' : '57.111278'},{'PH' : 'Engineering Manager','PG' : '005056A5088A1EE787B99B7278EC9CBE','ES' : '','PB' : '2017-04-10','FA' : 'Engineering/Facilities','CO' : 'Kazakhstan','CI' : 'Aktau','LO' : '51.17123','LA' : '43.635609'},{'PH' : 'Performance Analyst','PG' : '005056A5088A1EE7899FB07688905266','ES' : '','PB' : '2017-04-19','FA' : 'Project/Process/Performance Management','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Information Security Manager','PG' : '005056A52F591EE789B38045896F8EA3','ES' : '','PB' : '2017-04-20','FA' : 'IT','CO' : 'Denmark','CI' : 'Esbjerg','LO' : '8.448109','LA' : '55.462245'},{'PH' : 'Enterprise Integration Architect','PG' : '005056A5088A1ED787F2800C65D49EE2','ES' : '','PB' : '2017-04-12','FA' : 'IT','CO' : 'Denmark','CI' : 'Copenhagen','LO' : '12.590473','LA' : '55.68819'},{'PH' : 'Senior Piping and Mechanical Engineer TA- 2','PG' : '005056A52F591ED786E8BD8B3EBE5A29','ES' : '','PB' : '2017-04-07','FA' : 'Engineering/Facilities','CO' : 'Kazakhstan','CI' : 'Aktau','LO' : '51.17123','LA' : '43.635609'},{'PH' : 'Piping Engineer (Temporary)','PG' : '005056A5088A1ED787E8E8E7EC1B5479','ES' : '','PB' : '2017-04-12','FA' : 'Engineering/Facilities','CO' : 'Kazakhstan','CI' : 'Aktau','LO' : '51.17123','LA' : '43.635609'}]'''
        s = s.replace("'", '"')
        jobs_data = json.loads(s)
        for job_data in jobs_data:
            url = 'https://jobsearch.maersk.com/vacancies/publication?pinst=' + job_data['PG']
            request = scrapy.Request(url=url, callback=self.parseJob)
            request.meta['date'] = job_data['PB']
            request.meta['title'] = job_data['PH']
            request.meta['category'] = job_data['FA']
            request.meta['country'] = job_data['CO']
            request.meta['location'] = job_data['CI']
            yield request


    def parseJob(self, response):
        l = ItemLoader(item=JobItem(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.meta['title'])
        l.add_value('location', response.meta['location'])
        country = self.geocode.city2countryinfo(response.meta['country'])['ISO']
        if country is not None:
            l.add_value('country', country)
        else:
            country = self.geocode.city2countryinfo(response.meta['location'])['ISO']
            l.add_value('country', country)
        l.add_value('referencenumber', response.css('.erecpub_right_align').xpath('text()').extract()[0][4:].strip())
        l.add_value('date', self.getPublishDate(response.meta['date']))
        l.add_value('category', response.meta['category'])
        l.add_value('description', self.getDescription(response))
        yield l.load_item()


    def getPublishDate(self, date_string):
        year, month, day = map(lambda i: i.strip(), date_string.strip().split('-'))
        day = int(day)
        month = int(month)
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def getDescription(self, response):
        text1 = response.css('#erecpub_info_row_2').extract()[0]
        text2 = response.css('.erecpub_master_content_column').extract()[0]
        return '<br/>'.join([text1, text2])



