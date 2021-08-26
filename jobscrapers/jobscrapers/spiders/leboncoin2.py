# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import LeboncoinItem
from jobscrapers.items import PlainItem
from jobscrapers.items import annotations_list
import time
import pkgutil
from scrapy_splash import SplashRequest
import scrapy_splash
import math
import re
from base64 import b64encode

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class Leboncoin2Spider(scrapy.Spider):

    name = "leboncoin2"
    handle_httpstatus_list = [403]
    publisher = "Leboncoin"
    publisherurl = 'https://www.leboncoin.fr'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/leboncoin3.lua')
    url_index = None
    dirname = 'leboncoin2'
    limit = False
    drain = False
    rundebug = False
    min_item_len = 5
    min_len = 50
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    cities = []
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    orders = {}
    
    def __init__(self, limit=False, drain=False, debug=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))

    def start_requests(self):
        allowed_domains = ["https://www.leboncoin.fr"]
        count_job = 10069
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&text=%22bois%22&i'
        if self.rundebug:
            self.logger.info('Debug!!!')
            #url = 'https://2ip.ru/'
            url = 'https://www.leboncoin.fr/offres_d_emploi/1633562515.htm/'
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = url[:-1].split('/').pop()
            pprint(url)
            yield request
        else:
            request = SplashRequest(url, self.get_count_jobs, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            yield request

    def get_count_jobs(self, response):
        #pprint(response.text)
        count_job = int(response.css('label[for="result_pro"] span span').xpath('text()').extract()[0].replace(' ', ''))
        self.logger.info('COUNT JOB: {count_job}'.format(count_job=count_job))
        max_page = int(math.ceil(count_job / 40.0))
        url = 'https://www.leboncoin.fr/recherche/?category=33&text=%22bois%22'
        if self.limit:
            max_page = int(math.ceil(self.limit / 40.0))
        for page in range(1, max_page + 1):
            if page > 1:
                url = 'https://www.leboncoin.fr/recherche/?category=33&text=%22bois%22&page={page}'.format(page=page)
            request = SplashRequest(url, self.get_jobs_list, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            yield request

    def get_jobs_list(self, response):
        uris = response.css('a[class="clearfix trackable"]').xpath('@href').extract()
        #pprint(uris)
        for uri in uris:
            url = ''.join(['https://www.leboncoin.fr', uri])
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = uri[:-1].split('/').pop()
            yield request

    def parse_job(self, response):
        try:
            data = json.loads(response.text)
        except Exception, ex:
            self.logger.error(response.text)
            raise Exception(ex)
        l1 = ItemLoader(item=LeboncoinItem())
        name = response.meta['name']
        l1.add_value('name', name)
        l1.add_value('itemtype', 'annotation')
        self.add_element_from_data(l1, data, name, 'title', 'position', html=False)
        self.add_element_from_data(l1, data, name, 'salary', 'salary', html=False)
        self.add_element_from_data(l1, data, name, 'company', 'company', html=False)
        if 'desc' in data and data['desc'] is not None:
            self.add_element(l1, 'Description', name, 'desc_l', 'O', html=False)
            self.add_element(l1, data['desc'].replace('<br>', ' ').replace('&nbsp;', ''), name, 'desc', 'O', html=False)
            self.add_element(l1, u'Critères', name, 'criteries', 'O', html=False)
        if 'contract_type' in data and data['contract_type'] is not None:
            self.add_element(l1, u'TYPE DE CONTRAT', name, 'contrat_type_l', 'O', html=False)
            self.add_element(l1, data['contract_type'], name, 'contrat_type', 'contrat_type', html=False)
        if 'category' in data and data['category'] is not None:
            self.add_element(l1, u"SECTEUR D'ACTIVITÉ", name, 'category_l', 'O', html=False)
            self.add_element(l1, data['category'], name, 'category', 'O', html=False)
        if 'jobduty' in data and data['jobduty'] is not None:
            self.add_element(l1, u"FONCTION", name, 'jobduty_l', 'O', html=False)
            self.add_element(l1, data['jobduty'], name, 'jobduty', 'experience', html=False)
        if 'experience' in data and data['experience'] is not None:
            self.add_element(l1,  u"EXPÉRIENCE", name, 'experience_duration_l', 'O', html=False)
            self.add_element(l1, data['experience'], name, 'experience_duration', 'experience_duration', html=False)
        if 'education' in data and data['education'] is not None:
            self.add_element(l1,  u"NIVEAU D'ÉTUDES", name, 'education_l', 'O', html=False)
            self.add_element(l1, data['education'], name, 'education', 'education', html=False)
        if 'job_type' in data and data['job_type'] is not None:
            self.add_element(l1,  u"NTRAVAIL À", name, 'position_scheduled_l', 'O', html=False)
            self.add_element(l1, data['job_type'], name, 'position_scheduled', 'position_scheduled', html=False)
        if 'location' in data and data['location'] is not None:
            self.add_element(l1,  u"Localisation", name, 'location_l', 'O', html=False)
            self.add_element(l1, data['location'], name, 'location', 'city', html=False)
        yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            self.get_element(data, 'raw1'),
            self.get_element(data, 'raw2'),
            self.get_element(data, 'raw3'),
            self.get_element(data, 'raw4')
        ])
        l2.add_value('text', html)
        yield l2.load_item()

    def rm_spaces(self, text):
        text = text.replace('\n', ' ').replace('&nbsp;', ' ')
        while not text.find('  ') == -1:
            text = text.replace('  ', ' ')
        return text

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
                           'address', 'figure', 'td', 'th', 'tr', 'img', 'div', 'br', 'strong', 'span', 'section',
                            'li', 'ul', 'ol', 'p'
                           ]:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), ' ', text)
        return text

    def add_order(self, name, key):
        if name in self.orders:
            self.orders[name].append(key)
        else:
            self.orders[name] = [key]

    def add_element_from_data(self, l, data, name, key, annotation, html=False):
        if key in data:
            value = data[key]
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def add_element(self, l, text, name, key, annotation, html=False):
            value = text
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def get_element(self, data, key):
        if key in data:
            return data[key]
        return ''




