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
from jobscrapers.items import PlainItem
from jobscrapers.items import annotations_list
import time
import pkgutil
from scrapy_splash import SplashRequest
import math
import re
from jobscrapers.extensions import Geocode
import os
from jobscrapers.pipelines import clear_folder
from jobscrapers.items import categories2
import requests

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class PoleEmploiApiSpider(scrapy.Spider):

    name = "pole-emploi-api"
    publisher = "Pole-emploi"
    publisherurl = 'https://www.emploi-store-dev.fr'
    dirname = 'pole-emploi-api'
    limit = False
    drain = False
    rundebug = False
    token = None
    LIMIT = 100
    MAX = 1000
    env_content = pkgutil.get_data('jobscrapers', 'data/.env')
    application_id = None
    client_secret = None
    secteurActivite = [2, 16, 31]

    def __init__(self, limit=False, drain=False, debug=False, dirname=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if dirname:
            self.dirname = dirname
        self.application_id = map(lambda line: line.replace('APPLICATION_ID=', '').strip(),filter(lambda line: line.startswith('APPLICATION_ID'), self.env_content.split('\n')))[0]
        self.client_secret = map(lambda line: line.replace('CLIENT_SECRET=', '').strip(), filter(lambda line: line.startswith('CLIENT_SECRET'), self.env_content.split('\n')))[0]

    def start_requests(self):
        allowed_domains = ["https://api.emploi-store.fr"]
        urls = []
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://candidat.pole-emploi.fr/offres/recherche/detail/089LSHY'
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['name'] = '089LSHY'
            request.meta['industry'] = '16'
            yield request
        else:
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request(url=url, callback=self.fetch_jobs)
            yield request

    def fetch_jobs(self, response):
        offsets = range(0, self.MAX, self.LIMIT)
        for offset in offsets:
            jobs = self.get_jobs(offset, offset+self.LIMIT-1, '02,16')
            time.sleep(2)
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            url = '?'.join([url, 'offset={offset}'.format(offset=offset)])
            request = scrapy.Request(url, callback=self.get_job_page)
            request.meta['jobs'] = jobs
            #pprint(len(jobs))
            yield request
        for offset in offsets:
            jobs = self.get_jobs(offset, offset+self.LIMIT-1, '31')
            time.sleep(2)
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            url = '?'.join([url, 'offset=v2{offset}'.format(offset=offset)])
            request = scrapy.Request(url, callback=self.get_job_page)
            request.meta['jobs'] = jobs
            #pprint(len(jobs))
            yield request

    def get_token(self):
        url = 'https://entreprise.pole-emploi.fr/connexion/oauth2/access_token'
        scope = 'application_{application_id} api_offresdemploiv2 o2dsoffre '.format(application_id=self.application_id)
        data = {
            'realm': '/partenaire',
            'grant_type': 'client_credentials',
            'client_id': self.application_id,
            'client_secret': self.client_secret,
            'scope': scope
        }
        res = requests.post(url=url, data=data)
        if res.status_code == 200:
            #pprint(res.json())
            self.token = {
                'token': res.json()['access_token'],
                'expire': int(time.time()) + res.json()['expires_in']
            }
        else:
            self.logger.error(str(res.json()))

    def get_headers(self):
        if not self.token or int(time.time()) >= self.token['expire']:
            self.get_token()
        headers = {'Authorization': 'Bearer {token}'.format(token=self.token['token']), 'Content-Type': 'application/json'}
        #pprint(headers)
        return headers

    def get_jobs(self, start, end, sector):
            url = 'https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/search?range={p}-{d}&secteurActivite={sector}'.format(p=start, d=end, sector=sector)
            res = requests.get(url=url, headers=self.get_headers())
            if res.status_code < 300:
                return res.json()['resultats']
            self.logger.error('HTTP_CODE: %i' % res.status_code)
            self.logger.error('BODY: %s' % res.text)
            return None

    def get_job(self, id):
        url = 'https://api.emploi-store.fr/partenaire/offresdemploi/v2/offres/{id}'.format(id=id)
        res = requests.get(url=url, headers=self.get_headers())
        if res.status_code < 300:
            return res.json()
        self.logger.error('HTTP_CODE: %i' % res.status_code)
        self.logger.error('BODY: %s' % res.text)
        return None

    def get_job_page(self, response):
        jobs = response.meta['jobs']
        pprint(len(jobs))
        for job in jobs:
            url = job['origineOffre']['urlOrigine']
            request = scrapy.Request(url, callback=self.parse_job)
            request.meta['industry'] = job['secteurActivite']
            request.meta['name'] = job['id']
            yield request

    def parse_job(self, response):
        l1 = ItemLoader(item=MonsterItem())
        name = response.meta['name']
        industry = response.meta['industry']
        l1.add_value('name', name)
        l1.add_value('industry', industry)
        l1.add_value('itemtype', 'annotation')
        html = ' '.join(response.css('div[itemtype="http://schema.org/JobPosting"]').extract()).replace(' '.join(response.css(
            'div[itemtype="http://schema.org/JobPosting"] div[class="block-other-offers with-header"]').extract()), '')
        text = self.rm_spaces(self.cut_tags(html))
        l1.add_value('text', text)
        l1.add_value('text', 'O')
        yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('industry', industry)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
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
                            'li', 'ul', 'ol', 'p', 'dd', 'dl', 'hr', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a'
                           ]:
                    text = re.sub("""<%s.*?>""" % (tag,), ' ', text)
                    text = re.sub("""<\/%s>""" % (tag,), ' ', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), ' ', text)
        return text

    def debug(self):
        pass

