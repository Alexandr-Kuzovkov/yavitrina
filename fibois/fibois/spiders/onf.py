# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
from fibois.items import JobItem
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.keywords import last_posted
from fibois.scrapestack import ScrapestackRequest

class OnfSpider(scrapy.Spider):
    name = 'onf.fr'
    allowed_domains = []
    dirname = 'onf.fr'
    handle_httpstatus_list = [400, 404, 401]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = False
    es_exporter = None
    lua_src = pkgutil.get_data('fibois', 'lua/html-render.lua')

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if delta:
            self.delta = True
        if drain:
            self.drain = True

    def start_requests(self):
        url = 'http://www1.onf.fr/carrieres/sommaire/postuler/postuler/@@index.html'
        if self.use_scrapestack:
            request = ScrapestackRequest(url, callback=self.open_form, access_key=self.scrapestack_access_key)
        else:
            request = scrapy.Request(url, callback=self.open_form, dont_filter=True)
        yield request

    def open_form(self, response):
        url = 'https://www.altays-progiciels.com/clicnjob/RechOffre.php?NoSociete=157&NoSocFille=0&NoModele=97&NoLangue=1&NoSource=1&Autonome=1'
        if self.use_scrapestack:
            request = ScrapestackRequest(url, callback=self.open_job_list, access_key=self.scrapestack_access_key)
        else:
            request = scrapy.Request(url, callback=self.open_job_list, dont_filter=True)
        yield request

    def open_job_list(self, response):
        url = 'https://www.altays-progiciels.com/clicnjob/ListeOffreCand.php?NoTableOffreLieePl=0&NoTypContratl=0&NoNivEtl=0&NoPaysl=0&RefOffrel=&RechPleinTexte='
        if self.use_scrapestack:
            request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
        else:
            request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
        yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.xpath(u'//td[contains(text(), "résultat")]/text()').extract()[0:1])
        self.logger.info('header={header}'.format(header=h1.encode('utf-8')))
        links = response.css('tr[class="offre-titre"] td a').xpath('@href').extract()
        self.logger.debug(links)
        self.logger.debug('!!!links count={count}'.format(count=len(links)))
        page_results = {'job_board': 'onf.fr', 'job_board_url': 'http://www1.onf.fr', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://www.altays-progiciels.com/clicnjob/{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = 'onf_jobboard'
            results_block_title = response.css('tr[class="offre-titre"]').extract()
            results_block_content = response.css('tr[class="offre-contenu"]').extract()
            if len(results_block_title) > i:
                job['html'] = results_block_title[i]
                html = results_block_title[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('a b').xpath('text()').extract()).strip()
            if len(results_block_content) > i:
                job['html'] = ' '.join([job['html'], results_block_content[i]]).strip()
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request

        next_link = ' '.join(response.xpath(u'//b[contains(text(), "Page suivante")]/parent::a/@href').extract()[0:1])
        self.logger.debug('next_link={next_link}'.format(next_link=next_link))
        if len(next_link) > 0:
            url = 'https://www.altays-progiciels.com/clicnjob/{link}'.format(link=next_link)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_list_page, dont_filter=True)
            yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'onf.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('span[class="titre2"]').extract() + response.css('tr[class="critere-liste"]').extract()).strip()
        html_content = ' '.join(response.css('div[id="content-block"]').extract()).strip()
        content = html_content
        content = self.h.handle(content).strip()
        contract_type = ' '.join(response.xpath(u'//b[contains(text(), "Type de contrat")]/parent::div/text()').extract()).strip()
        profile = ' '.join(response.xpath(u'//b[contains(text(), "Niveau Professionnel")]/parent::div/text()').extract()).strip()
        salary = ' '.join(response.xpath(u'//b[contains(text(), "Salaire Annuel Brut")]/parent::div/text()').extract()).strip()
        sector = ' '.join(response.xpath(u'//b[contains(text(), "Domaines métiers")]/parent::div/text()').extract()).strip()
        region = ' '.join(response.xpath(u'//b[contains(text(), "Domaines métiers")]/parent::div/text()').extract()).strip()
        ville = ' '.join(response.xpath(u'//b[contains(text(), "Ville")]/parent::div/text()').extract()).strip()
        education_level = ' '.join(response.xpath(u'//b[contains(text(), "Niveau d\'études")]/parent::div/text()').extract()).strip()
        experience_level = ' '.join(response.xpath(u'//b[contains(text(), "Niveau d\'expérience")]/parent::div/text()').extract()).strip()
        pays = ' '.join(response.xpath(u'//b[contains(text(), "Pays")]/parent::div/text()').extract()).strip()
        department = ' '.join(response.xpath(u'//b[contains(text(), "Département")]/parent::div/text()').extract()).strip()
        publish_date = ' '.join(response.xpath(u'//b[contains(text(), "Date de publication")]/parent::div/text()').extract()).strip()

        if len(salary) > 0:
            l.add_value('salary', salary + u' (K€)')
        if len(region) > 0:
            l.add_value('location', region)
        if len(ville) > 0:
            l.add_value('location', ville)
        if len(pays) > 0:
            l.add_value('location', pays)
        if len(department) > 0:
            l.add_value('location', department)
        if len(contract_type) > 0:
            l.add_value('contract_type', contract_type)
        if len(publish_date) > 0:
            l.add_value('publish_date', publish_date)
        if len(sector) > 0:
            l.add_value('sector', sector)
        if len(profile) > 0:
            l.add_value('profile', profile)
        if len(education_level) > 0:
            l.add_value('profile', education_level)
        if len(experience_level) > 0:
            l.add_value('experience_level', experience_level)
        l.add_value('content', content)
        l.add_value('source', 'scrapy')
        l.add_value('header', header)
        l.add_value('html_content', html_content)
        yield l.load_item()
        job_details = {'html': html_content, 'title': header, 'url': job['url']}
        if not self.drain:
            self.es_exporter.insert_job_details_html(job_details)

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date









