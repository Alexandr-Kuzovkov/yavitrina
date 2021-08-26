# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import IndeedItem
from jobscrapers.items import onisep_data_template
from jobscrapers.items import onisep_act_template
from jobscrapers.items import onisep_comp_template
from jobscrapers.items import onisep_education_template
from jobscrapers.items import onisep_formation_template
from jobscrapers.items import annotations_list
import math
import re
import os
import pkgutil
from jobscrapers.extensions import Geocode

r_title = re.compile('^.*F/H')
r_city = re.compile('F/H,.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

class OnisepSpider(scrapy.Spider):

    name = "onisep"
    publisher = "Onisep"
    publisherurl = 'http://www.onisep.fr'
    url_index = None
    dirname = 'onisep'
    limit = False
    drain = False
    reannotate_only = False
    orders = {}
    geocode = Geocode()
    countries = geocode.get_countries()
    cities = []
    file_content = pkgutil.get_data('jobscrapers', 'data/metiers_onisep.csv')

    def __init__(self, limit=False, drain=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        self.links = list(set(filter(lambda s: s.startswith('http://www.onisep.fr'), map(lambda k: k[1].strip(), filter(lambda j: len(j) > 1, map(lambda i: i.split(','), self.file_content.split('\n')))))))

    def start_requests(self):
        allowed_domains = ["http://www.onisep.fr"]
        if self.limit:
            self.links = self.links[:self.limit]
        self.logger.info('Job\'s links found: {count}'.format(count=len(self.links)))
        for url in self.links:
            request = scrapy.Request(url, callback=self.get_job_page)
            yield request

    def get_job_page(self, response):
        uri = ' '.join(response.css('a[id="ideo-version-long"]').xpath('@href').extract())
        url = ''.join(['http://www.onisep.fr', uri])
        request = scrapy.Request(url, callback=self.parse_job)
        yield request

    def parse_job(self, response):
        l = ItemLoader(item=IndeedItem())
        #title#
        url = response.url
        id = url[url.find('content/location')+17:url.find('/version_longue')]
        name = '.'.join([id, 'json'])
        l.add_value('name', name)
        data = onisep_data_template
        data['id'] = int(id)
        data['label'] = ' '.join(response.css('div[id="oni_zoom-block"] h1')[:1].xpath('text()').extract())
        data['description'] = ' '.join(response.css('div[id="oni_zoom-block"] div[class="oni_chapo"] p').xpath('text()').extract())
        try:
            data['salary_init'] = ' '.join(response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li abbr span').xpath(u'//span[starts-with(text(),"Salaire débutant")]')[0].xpath('parent::abbr/parent::li/text()').extract()).strip()
        except IndexError:
            pass
        try:
            data['educ_min'] = ' '.join(response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li span').xpath(u'//span[starts-with(text(),"Niveau minimum")]')[0].xpath('parent::li/text()').extract()).strip()
        except IndexError:
            pass
        try:
            data['alt'] = map(lambda i: i.strip(), ' '.join(response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li span').xpath(u'//span[starts-with(text(),"Synonymes")]')[0].xpath('parent::li/text()').extract()).strip().split(','))
        except IndexError:
            pass
        try:
            status = ' '.join(response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li span').xpath(u'//span[starts-with(text(),"Statut")]')[0].xpath('parent::li/text()').extract()).strip()
            data['status'] = self.rm_spaces(status)
        except IndexError:
            pass
        try:
            data['sector'] = map(lambda i: i.strip(), response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li span').xpath(u'//span[starts-with(text(),"Secteur(s) professionnel(s)")]')[0].xpath('following-sibling::p/a/text()').extract())
        except IndexError:
            pass
        try:
            data['soft_skills'] = map(lambda i: i.strip(), response.css('div[id="oni_zoom-block"] ul[class="oni_last"] li span').xpath(u'//span[starts-with(text(),"Centre(s)")]')[0].xpath('following-sibling::p/a/text()').extract())
        except IndexError:
            pass
        act_titles = response.css('div[id="oni_onglet-1"]').xpath(u'//h3[text()="Compétences requises"]/preceding-sibling::h4/text()').extract()
        act_descrs = response.css('div[id="oni_onglet-1"]').xpath(u'//h3[text()="Compétences requises"]/preceding-sibling::p/text()').extract()

        activities = []
        for i in range(min(len(act_titles), len(act_descrs))):
            activities.append({'act_title': act_titles[i], 'act_description': act_descrs[i]})
        data['activities'] = activities
        comp_titles = response.css('div[id="oni_onglet-1"]').xpath(u'//h3[text()="Compétences requises"]/following-sibling::h4/text()').extract()
        comp_descrs = response.css('div[id="oni_onglet-1"]').xpath(u'//h3[text()="Compétences requises"]/following-sibling::p/text()').extract()
        competencies = []
        for i in range(min(len(comp_titles), len(comp_descrs))):
            competencies.append({'comp_title': comp_titles[i], 'comp_descr': comp_descrs[i]})
        data['competencies'] = competencies

        educations = []
        educ_titles = response.css('div[id="oni_onglet-4"]').xpath(u'p[starts-with(text(), "Niveau")]/text()').extract()
        for i in range(len(educ_titles)):
            educ_labels = response.css('div[id="oni_onglet-4"]').xpath(u'p[starts-with(text(), "Niveau")]')[i].xpath('following-sibling::ul[1]/li/text()').extract()
            educations.append({'educ_level': educ_titles[i], 'educ_labels': educ_labels})
        data['educations'] = educations

        formations = []
        form_levels = response.css('div[id="oni_onglet-5"] h4').xpath('text()').extract()
        for i in range(len(form_levels)):
            form_title = response.css('div[id="oni_onglet-5"] h4')[i].xpath('following-sibling::ul')[0].xpath('li/a/text()').extract()
            formations.append({'form_level': form_levels[i], 'form_title': form_title})
        data['formations'] = formations

        l.add_value('body', json.dumps(data))
        yield l.load_item()


    def rm_spaces(self, text):
        while not text.find('  ') == -1:
            text = text.replace('  ', '')
        return text




