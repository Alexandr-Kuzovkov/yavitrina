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
import math
import re
import os
import pkgutil
from jobscrapers.extensions import Geocode
from jobscrapers.items import wood_jobs
from scrapy_splash import SplashRequest

class ApecSpider(scrapy.Spider):

    name = "apec"
    publisher = "apec"
    publisherurl = 'https://cadres.apec.fr'
    url_index = None
    dirname = 'apec'
    limit = False
    drain = False
    orders = {}
    min_len = 50
    cities = []
    rundebug = False
    annotation = False

    def __init__(self, limit=False, drain=False, debug=None, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        if annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["https://cadres.apec.fr"]
        if self.rundebug:
            url = 'https://cadres.apec.fr/offres-emploi-cadres/0_0_0_164360508W__________offre-d-emploi-cuisines-raison-cuisiniste-agenceur-directeur-territoire-f-h.html?numIdOffre=164360508W&totalCount=31&selectedElement=0&sortsType=SCORE&sortsDirection=DESCENDING&nbParPage=20&page=0&motsCles=Agenceur&distance=0&xtmc=agenceur&xtnp=1&xtcr=1&retour=%2Fhome%2Fmes-offres%2Frecherche-des-offres-demploi%2Fliste-des-offres-demploi.html%3FsortsType%3DSCORE%26sortsDirection%3DDESCENDING%26nbParPage%3D20%26page%3D0%26motsCles%3DAgenceur%26distance%3D0'
            request = SplashRequest(url, self.parse_job, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
            request.meta['name'] = 'debug-job-9156890'
            yield request
        else:
            for title in wood_jobs[1:]:
                title = urllib.quote(title.encode('utf-8'))
                url = 'https://cadres.apec.fr/home/mes-offres/recherche-des-offres-demploi/liste-des-offres-demploi.html?sortsType=SCORE&sortsDirection=DESCENDING&nbParPage=100&page=0&motsCles={title}&distance=0'.format(title=title)
                request = SplashRequest(url, self.get_job_list, endpoint='render.html', args={'wait': 0.5, 'timeout': 3600})
                request.meta['page'] = 0
                yield request

    def get_job_list(self, response):
        links = response.css('h2[class="offre-title"] a[target="_self"]').xpath('@href').extract()
        links = list(set(links))
        for link in links:
            url = ''.join(['https://cadres.apec.fr', link])
            request = SplashRequest(url, self.parse_job, endpoint='render.html', args={'wait': 3.0, 'timeout': 3600})
            parsed = urlparse.urlparse(url)
            name = urlparse.parse_qs(parsed.query)['numIdOffre'][0]
            request.meta['name'] = name
            yield request
        next_links = response.css('ul[class="pagination pagination-sm pull-right"] li').xpath(u'//a[text()="Suiv."]').extract()
        if len(next_links) > 0:
            page = response.meta['page']
            next_url = response.url.replace('&page={page}'.format(page=page), '&page={page}'.format(page=page + 1))
            request = SplashRequest(next_url, callback=self.get_job_list, endpoint='render.html', args={'wait': 3.0, 'timeout': 3600})
            request.meta['page'] = page + 1
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        description = '\n'.join(
            response.css('h1[class="ng-scope"]').extract() +
            response.css('div[class="cadre cadre-grey margin-bottom-0"] div[class="row"]').extract() +
            response.css('div[class="cadre cadre-grey margin-bottom-0"] div[class="clearfix border-top-solid"] div[class="pull-left"]').extract() +
            response.css('div[class="cadre cadre-grey"] div[class="row"]').extract() +
            response.css('div[id="descriptif-du-poste"]').extract() +
            response.css('div[id="profil-recherche"]').extract() +
            response.css('div[id="entreprise"]').extract() +
            response.css('div[id="processus-de-recrutement"]').extract()
        )
        if self.annotation:
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            l1.add_value('description', self.rm_spaces(self.cut_tags(description)))
            l1.add_value('description', 'description')
            self.add_order(name, 'description')
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = description
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




