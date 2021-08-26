# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import MonsterItem
from jobscrapers.items import annotations_list
import time
import pkgutil
from scrapy_splash import SplashRequest
import math
import re
from jobscrapers.extensions import Geocode
import os
from jobscrapers.pipelines import clear_folder
from bs4 import BeautifulSoup

class MonsterSpider(scrapy.Spider):

    name = "monster"
    publisher = "Monster"
    publisherurl = 'https://www.monster.co.uk'
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    lua_src = pkgutil.get_data('jobscrapers', 'lua/monster.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/monster2.lua')
    lua_src_html = pkgutil.get_data('jobscrapers', 'lua/html.lua')
    url_index = None
    dirname = 'monster'
    limit = False
    drain = False
    rundebug = False
    country = 'uk' #country
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    reannotate_only = False
    order = [
        'name',
        'title',
        'position',
        'company',
        'city',
        'postal_code',
        'desc',
        'salary_l',
        'salary',
        'summary_l',
        'summary',
        'location_l',
        'city',
        'postal_code2',
        'job_type_l',
        'position_scheduled',
        'contrat_type',
        'experience_l',
        'experience',
        'posted_l',
        'posted',
        'industries_l',
        'industries',
        'education_l',
        'education',
        'contactname_l',
        'contactname',
        'phone_l',
        'phone',
        'fax_l',
        'fax'
        'referencecode_l',
        'referencecode'
    ]
    min_item_len = 7
    jobs_per_page = {'uk': 27, 'ca': 26}
    
    def __init__(self, limit=False, drain=False, country='uk', debug=False, ra=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.country = country
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        if ra:
            self.reannotate_only = True

    def start_requests(self):
        allowed_domains = ["https://www.monster.co.uk", "https://job-openings.monster.co.uk"]
        url = 'https://www.monster.co.uk/jobs/search/?saltyp=1&cy=uk'
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://www.monster.co.uk/jobs/search/?saltyp=1&cy=uk&jobid=206579589'
            splash_args = {'wait': 2.5, 'lua_source': self.lua_src_html, 'timeout': 3600}
            if self.limit:
                splash_args['limit'] = self.limit
            request = SplashRequest(url, self.parse_job, endpoint='execute', args=splash_args)
            request.meta['name'] = '206579589'
            request.meta['industry'] = 'UK'
            #request = scrapy.Request(url)
            yield request
        elif self.reannotate_only:
            url = 'http://localhost'
            yield scrapy.Request(url, callback=self.reannotate)
        else:
            requests = []
            url = 'https://www.monster.co.uk/jobs/search/?saltyp=1&cy={country}'.format(country=self.country)
            if self.country == 'ca':
                url = 'https://www.monster.ca/jobs/search/'
            splash_args = {'wait': 2.5, 'lua_source': self.lua_src, 'timeout': 3600}
            if self.limit:
                splash_args['limit'] = self.limit
            industry = self.country
            files_dir = self.settings.get('FILES_DIR', '')
            clear_folder(os.path.sep.join([files_dir, self.dirname, industry]))
            clear_folder(os.path.sep.join([files_dir, self.dirname, 'src', industry]))
            request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args=splash_args)
            request.meta['industry'] = self.country
            requests.append(request)
            if not self.limit or self.limit > self.jobs_per_page[self.country] * 10:
                if not self.limit:
                    self.limit = 10000
                for page in range(11, int(math.ceil((self.limit-self.jobs_per_page[self.country] * 10)/self.jobs_per_page[self.country]))+1):
                    url = 'https://www.monster.co.uk/jobs/search/?saltyp=1&cy={country}&stpage=1&page={page}'.format(country=self.country, page=page)
                    if self.country == 'ca':
                        url = 'https://www.monster.ca/jobs/search/?stpage=1&page={page}'.format(page=page)
                    splash_args['lua_source'] = self.lua_src2
                    request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args=splash_args)
                    request.meta['industry'] = self.country
                    requests.append(request)
            for request in requests:
                yield request

    def get_jobs_list(self, response):
        data = json.loads(response.text)
        links = data[0]
        criterie = data[1]
        self.logger.info('Fetched: {count} items'.format(count=len(links)))
        uris = links.values()
        #pprint(uris)
        for uri in uris:
            job_id = uri.split('/').pop()
            url = 'https://www.monster.co.uk/jobs/search/?saltyp=1&cy={country}&jobid={job_id}'.format(country=self.country, job_id=job_id)
            if self.country == 'ca':
                url = 'https://www.monster.ca/jobs/search/?jobid={job_id}'.format(job_id=job_id)
            request = SplashRequest(url, callback=self.parse_job, endpoint='execute', args={'wait': 2.5, 'lua_source': self.lua_src_html, 'timeout': 3600})
            request.meta['name'] = job_id
            request.meta['industry'] = response.meta['industry']
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=MonsterItem())
        #data = json.loads(response.text)
        l.add_value('name', response.meta['name'])
        l.add_value('industry', response.meta['industry'])
        #response = data[0]
        #png = data[1]
        #l.add_value('png', png)
        try:
            title = response.css('div#JobViewHeader h1.title').xpath('text()').extract()[0]
            position = '-'.join(title.split('-')[:-1]).strip()
            l.add_value('position', position)
            l.add_value('position', 'position')
            company = title.split('-').pop().strip()
            l.add_value('company', company)
            l.add_value('company', 'company')
        except Exception, ex:
            pass
        try:
            subtitle = response.css('div#JobViewHeader h2.subtitle').xpath('text()').extract()[0]
            city = subtitle.split(',')[0].strip()
            l.add_value('city', city)
            l.add_value('city', 'city')
            postal_code = subtitle.split(',')[1].strip()
            l.add_value('postal_code', postal_code)
            if len(filter(lambda i: str(i) in postal_code, range(0, 10))) > 0:
                l.add_value('postal_code', 'postal_code')
            elif postal_code.lower() in self.cities:
                l.add_value('postal_code', 'city')
            else:
                l.add_value('postal_code', 'O')
            l.add_value('postal_code', 'postal_code')
        except Exception, ex:
            pass
        try:
            desc = response.css('div#JobDescription').extract()[0]
            desc = desc.replace('</p>', ' ').replace('<br>', ' ').replace('</li>', ' ').replace('</strong>', ' ')
            soup = BeautifulSoup(desc)
            desc = soup.get_text()
            l.add_value('desc', desc)
            l.add_value('desc', 'O')
        except Exception, ex:
            pass
        try:
            salary = response.css('div.mux-job-cards section header + div').xpath('text()').extract()[0].strip()
            l.add_value('salary_l', 'Salary')
            l.add_value('salary_l', 'O')
            l.add_value('salary', salary)
            if len(filter(lambda i: str(i) in salary, range(0, 10))) > 0:
                l.add_value('salary', 'salary')
            else:
                l.add_value('salary', 'O')
        except Exception, ex:
            pass

        try:
            summary_sections = response.css('div#JobSummary section')
            l.add_value('summary', 'job summary')
            l.add_value('summary', 'O')
            summary = {}
            for section in summary_sections:
                key = section.css('dt.key').xpath('text()').extract()[0].strip()
                val = section.css('dd.value').xpath('text()').extract()[0].strip()
                summary[key] = val
            if 'Location' in summary:
                l.add_value('location_l', 'Location')
                l.add_value('location_l', 'O')
                city = summary['Location'].split(',')[0]
                l.add_value('city', city)
                if city.lower() in self.cities:
                    l.add_value('city', 'city')
                else:
                    l.add_value('city', 'O')
                try:
                    postal_code = summary['Location'].split(',')[1]
                    l.add_value('postal_code2', postal_code)
                    if len(filter(lambda i: str(i) in postal_code, range(0, 10))) > 0:
                        l.add_value('postal_code2', 'postal_code')
                    elif postal_code.lower() in self.cities:
                        l.add_value('postal_code2', 'city')
                    else:
                        l.add_value('postal_code2', 'O')
                except Exception:
                    pass

            if 'Job type' in summary:
                l.add_value('job_type_l', 'Job type')
                l.add_value('job_type_l', 'O')
                position_scheduled = summary['Job type'].split(',')[0]
                l.add_value('position_scheduled', position_scheduled)
                l.add_value('position_scheduled', 'position_scheduled')
                try:
                    contrat_type = summary['Job type'].split(',')[1]
                    l.add_value('contrat_type', contrat_type)
                    l.add_value('contrat_type', 'contrat_type')
                except Exception:
                    pass

            if 'Career level' in summary:
                l.add_value('experience_l', 'Career level')
                l.add_value('experience_l', 'O')
                experience = summary['Career level']
                l.add_value('experience', experience)
                l.add_value('experience', 'experience')
                
            if 'Education level' in summary:
                l.add_value('education_l', 'Education level')
                l.add_value('education_l', 'O')
                education = summary['Education level']
                l.add_value('education', education)
                l.add_value('education', 'education')

            for key, val in summary.items():
                if key in ['Location', 'Job type', 'Career level', 'Education level']:
                    continue
                field = key.lower().replace(' ', '')
                l.add_value(field+'_l', key)
                l.add_value(field+'_l', 'O')
                l.add_value(field, summary[key])
                l.add_value(field, 'O')

        except Exception, ex:
            pass

        try:
            keys = response.css('div#ContactInfo dt')
            vals = response.css('div#ContactInfo dd')
            contacts = {}
            for i in range(len(keys)):
                key = keys[i].xpath('text()').extract()[0].strip()
                val = vals[i].xpath('text()').extract()[0].strip()
                contacts[key] = val
            for key, val in contacts.items():
                field = key.lower().replace(' ', '')
                l.add_value(field+'_l', key)
                l.add_value(field+'_l', 'O')
                l.add_value(field, contacts[key])
                l.add_value(field, 'O')
        except Exception, ex:
            raise Exception(ex)
        yield l.load_item()

    def debug(self, response):
        data = json.loads(response.text)
        links = data[0]
        criterie = data[1]
        pprint(links)
        pprint(criterie)
        pprint(len(links))

    def debug2(self, response):
        div = response.css('div#JobDescription')
        pprint(div.extract()[0])
        salary = response.css('div.mux-job-cards section header + div').xpath('text()').extract()[0]
        pprint(salary)

        sections = response.css('div#JobSummary section')
        data = {}
        for section in sections:
            key = section.css('dt.key').xpath('text()').extract()[0].strip()
            val = section.css('dd.value').xpath('text()').extract()[0].strip()
            data[key] = val
        keys = response.css('div#ContactInfo dt')
        vals = response.css('div#ContactInfo dd')
        for i in range(len(keys)):
            key = keys[i].xpath('text()').extract()[0].strip()
            val = vals[i].xpath('text()').extract()[0].strip()
            data[key] = val
        pprint(data)

    def reannotate(self, response):
        self.logger.info('RUN REANNOTATE ONLY!!!')
        files_dir = self.settings.get('FILES_DIR', '')
        source_dir = os.path.sep.join([files_dir, self.dirname, 'src', self.country])
        self.logger.info('Source dir: "%s"' % source_dir)
        list_of_files = os.listdir(source_dir)
        self.logger.info('%i files will reannotate' % len(list_of_files))
        for name in list_of_files:
            with open(os.path.sep.join([source_dir, name]), 'r') as fi:
                item = json.load(fi, 'utf-8')
                item['name'] = [name[:-4]]
                yield item

    def cut_tags(self, text):
        allowed_tags = []
        all_tags_re = re.compile('<.*?>')
        all_tags = all_tags_re.findall(text)
        # pprint(all_tags)
        all_tags = map(lambda i: i.split(' ')[0].replace('<', '').replace('>', '').replace('/', ''), all_tags)
        # pprint(list(set(all_tags)))
        for tag in all_tags:
            if tag not in allowed_tags:
                if tag in ['table', 'tbody', 'thead', 'header', 'footer', 'nav', 'section', 'article', 'aside',
                           'address', 'figure', 'td', 'th', 'tr', 'img']:
                    text = re.sub("""<%s.*?>""" % (tag,), '', text)
                    text = re.sub("""<\/%s>""" % (tag,), '', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        return text



