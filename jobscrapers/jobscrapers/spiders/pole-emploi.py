# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import HaysJob
import time
import pkgutil
from scrapy_splash import SplashRequest


class PoleEmploiSpider(scrapy.Spider):

    name = "pole-emploi"
    publisher = "Pole emploi"
    publisherurl = 'https://candidat.pole-emploi.fr/'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/pole-emploi.lua')
    url_index = None
    dirname = 'pole-emploi'
    urls = []


    def __init__(self, url_index=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if url_index is not None:
            self.url_index = int(url_index)

    def start_requests(self):
        allowed_domains = ["https://candidat.pole-emploi.fr"]
        urls = [
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=75D&offresPartenaires=true&range=0-9&rayon=10&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=57463&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=51454&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=13201&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=06088&offresPartenaires=true&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=31555&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=33063&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=59350&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=29019&offresPartenaires=true&range=0-9&rayon=100&tri=0',
            'https://candidat.pole-emploi.fr/offres/recherche?lieux=44109&offresPartenaires=true&range=0-9&rayon=100&tri=0'
        ]
        for index in range(len(urls)):
            if self.url_index is not None and index != self.url_index:
                continue
            url = urls[index]
            request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})
            request.meta['search_url'] = url
            yield request


    def get_jobs_list(self, response):
        search_url = response.meta['search_url']
        self.logger.info('Parsing page %s ...' % search_url)
        data = json.loads(response.text)
        self.logger.info('%i items was fetched' % len(data))
        for key, job_data in data.items():
            url = ''.join(['https://candidat.pole-emploi.fr', job_data['link']])
            self.urls.append(url)
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['title'] = job_data['title']
            request.meta['subtitle'] = job_data['subtitle']
            request.meta['url'] = url
            yield request

    def parse_job(self, response):
        l = ItemLoader(item=HaysJob())
        l.add_value('title', response.meta['title'])
        l.add_value('subtitle', response.meta['subtitle'])
        l.add_value('description', self.get_description(response))
        l.add_value('name', self.get_job_name(response.meta['url']))
        yield l.load_item()

    def get_job_name(self, url):
        name = url.split('/').pop()
        return name

    def get_description(self, response):
        desc = []
        try:
            p1 = response.css('div.description p')[0].xpath('text()').extract()[0]
            desc.append(p1)
        except Exception, ex:
            pass
        try:
            p2 = ' '.join(response.css('dd').xpath('text()').extract())
            desc.append(p2)
        except Exception:
            pass
        try:
            p3 = response.css('h3.subtitle')[0].xpath('text()').extract()[0]
            desc.append(p3)
        except Exception:
            pass
        try:
            p4 = response.css('h4.skill-subtitle')[0].xpath('text()').extract()[0]
            desc.append(p4)
        except Exception:
            pass
        try:
            p5 = ' '.join(response.css('span.skill span').xpath('text()').extract()[0:1])
            desc.append(p5)
        except Exception:
            pass
        try:
            p6 = ' '.join(response.css('ul[class="skill-list list-unstyled"] + h3').xpath('text()').extract())
            desc.append(p6)
        except Exception:
            pass
        try:
            p7 = ' '.join(response.css('ul[class="skill-list list-unstyled"] + h3 + ul li').xpath('text()').extract())
            p8 = ' '.join(response.css('ul[class="skill-list list-unstyled"] + h3 + ul li span').xpath('text()').extract())
            desc.append(p7 + p8)
        except Exception:
            pass
        try:
            p9 = ' '.join(response.css('span[itemprop="hiringOrganization"] + h3.subtitle').xpath('text()').extract())
            desc.append(p9)
        except Exception:
            pass
        try:
            p10 = ' '.join(response.css('span[itemprop="hiringOrganization"] + h3.subtitle + div h4').xpath('text()').extract())
            desc.append(p10)
        except Exception:
            pass
        return '\n'.join(desc).strip()

