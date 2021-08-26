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
import time
import pkgutil
from scrapy_splash import SplashRequest
from jobscrapers.items import wood_jobs
import re


class PoleEmploiWoodSpider(scrapy.Spider):
    name = "pole-emploi-wood"
    publisher = "Pole emploi"
    publisherurl = 'https://candidat.pole-emploi.fr/'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/pole-emploi.lua')
    rundebug = False
    annotation = False
    dirname = 'pole-emploi-wood'
    urls = []


    def __init__(self, debug=False, annotation=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if debug:
            self.rundebug = True
        if annotation:
            self.annotation = True

    def start_requests(self):
        allowed_domains = ["https://candidat.pole-emploi.fr"]
        if self.rundebug:
            self.logger.info('Debug!!!')
            url = 'https://candidat.pole-emploi.fr/offres/recherche/detail/089LSHY'
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['name'] = '089LSHY'
            request.meta['industry'] = '16'
            yield request
        else:
            urls = []
            for title in wood_jobs:
                title = urllib.quote_plus(title.encode('utf-8'))
                url = 'https://candidat.pole-emploi.fr/offres/recherche?motsCles={title}&offresPartenaires=true&range=0-99&rayon=100&tri=0'.format(title=title)
                request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600})
                request.meta['search_url'] = url
                request.meta['industry'] = title
                yield request

    def get_jobs_list(self, response):
        search_url = response.meta['search_url']
        self.logger.info('Parsing page %s ...' % search_url)
        data = json.loads(response.text)
        self.logger.info('%i items was fetched' % len(data))
        for key, job_data in data.items():
            url = ''.join(['https://candidat.pole-emploi.fr', job_data['link']])
            request = scrapy.Request(url=url, callback=self.parse_job)
            request.meta['industry'] = response.meta['industry']
            request.meta['name'] = job_data['link'].split('/').pop()
            yield request

    def parse_job(self, response):
        name = response.meta['name']
        html = ' '.join(response.css('div[itemtype="http://schema.org/JobPosting"]').extract()).replace(
            ' '.join(response.css(
                'div[itemtype="http://schema.org/JobPosting"] div[class="block-other-offers with-header"]').extract()),
            '')
        if self.annotation:
            text = self.rm_spaces(self.cut_tags(html))
            l1 = ItemLoader(item=MonsterItem())
            l1.add_value('name', name)
            l1.add_value('itemtype', 'annotation')
            l1.add_value('text', text)
            l1.add_value('text', 'O')
            yield l1.load_item()
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
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

