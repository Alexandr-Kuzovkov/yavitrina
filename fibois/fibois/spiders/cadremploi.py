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

class CadremploiSpider(scrapy.Spider):
    name = 'cadremploi'
    allowed_domains = []
    dirname = 'www.cadremploi.fr'
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    keywords = occupations
    #keywords = ["bois"]
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
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
        if keywords_limit:
            self.keywords_limit = int(keywords_limit)
        if keywords:
            if keywords == 'companies':
                self.keywords = companies
                self.keywords_type = 'companies'
        if self.keywords_limit is not None:
            self.keywords = self.keywords[0:self.keywords_limit]

    def start_requests(self):
        for keyword in self.keywords:
            self.pagination[keyword] = {'page': 1, 'pages': None}
        for keyword in self.keywords:
            page = self.pagination[keyword]['page']
            url = 'https://www.cadremploi.fr/emploi/liste_offres?motscles={keyword}&tri=DATE_PUBLICATION'.format(keyword=urllib.quote_plus(keyword))
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['keyword'] = keyword
            yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.css('h1[class="compteur-offres"]').xpath('text()').extract()).strip()
        keyword = response.meta['keyword']
        self.logger.info(h1)
        try:
            count = int(' '.join(response.css('h1[class="compteur-offres"]').xpath('text()').extract()).replace(' ', '').encode('ascii','ignore'))
            self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        except Exception as ex:
            self.logger.warning(ex)
            self.logger.warning('keyword={keyword}'.format(keyword=keyword))
        links = response.css('ul[id="liste-postuler"] li[class="offre-card"] a').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('ul[id="liste-postuler"] li[class="offre-card"]').extract()
        page_results = {'job_board': 'cadremploi.fr', 'job_board_url': 'https://www.cadremploi.fr', 'page_url': response.url, 'offers': []}
        dates = []
        for i in range(0, len(links)):
            url = 'https://www.cadremploi.fr{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = unicode(response.meta['keyword'], 'utf8')
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                title = ' '.join(offer_block.css('h2[class="titre-offre-label"]').xpath('text()').extract()).strip()
                if len(title) == 0:
                    title = ' '.join(offer_block.css('h2[class="ce-link link-title d-inline-block text-left mr-1 mb-0"]').xpath('text()').extract()).strip()
                job['title'] = title
                job['company_name'] = ' '.join(offer_block.css('div[class="infos-offre"] span[class="nom-recruteur be-ellipsis"]').xpath('text()').extract()).strip()
                job['contract_info'] = ' '.join(offer_block.css('div[class="infos-offre"] span[class="js-type-contrat-offre"]').xpath('text()').extract()).strip()
                job['publish_date'] = ' '.join(offer_block.css('time[class="date-publication"]').xpath('text()').extract()).strip()
                job['location'] = ' '.join(offer_block.css('div[class="infos-offre"] span[class="localisation"]').xpath('text()').extract()).strip()
                page_results['offers'].append(job)
                dates.append(job['publish_date'])
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key, dont_filter=True)
            else:
                args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                request = SplashRequest(url, callback=self.parse_job_detail_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
            request.meta['job'] = job
            yield request
        try:
            next_link = ' '.join(response.css('a[id="js-pagination-next"]').xpath('@href').extract())
            if len(next_link) > 0:
                if self.check_published_at(response) or not self.delta:
                    url = 'https://www.cadremploi.fr{next_link}'.format(next_link=next_link)
                    if self.use_scrapestack:
                        request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                    else:
                        args = {'wait': 10.0, 'lua_source': self.lua_src, 'timeout': 3600}
                        request = SplashRequest(url, callback=self.parse_job_list_page, endpoint='execute', args=args, meta={"handle_httpstatus_all": True})
                    request.meta['keyword'] = keyword
                    yield request
        except Exception as ex:
            self.logger.info('pagination error: ' + str(ex))
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'cadremploi.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        header = ' '.join(response.css('div[class="header-wrap container-detail"]').extract()).strip()
        html_content = ' '.join(response.css('div[class="container-detail-content"]').extract()).strip()
        content = header + '<br>' + html_content
        content = self.h.handle(content).strip()
        # content = self.h.handle(content).encode('utf-8').strip()
        company_info = ' '.join(response.css('p[class="desc__p desc_entreprise cache"]').xpath('text()').extract()).strip()
        salary = ' '.join(response.css('div[class="container-detail-content"] span[class="icon-info-sup icon-salaire"] + span').xpath('text()').extract()).strip()
        profile = ' '.join(response.css('div[class="container-detail-content"] p[class="desc__p desc_profil cache"]').xpath('text()').extract()).strip()
        if len(company_info) > 0:
            l.add_value('company_info', company_info)
        if len(salary) > 0:
            l.add_value('salary', salary)
        if len(profile) > 0:
            l.add_value('profile', profile)
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

    def check_published_at(self, response):
        if not self.delta:
            return True
        dates = response.css('ul[id="liste-postuler"] li[class="offre-card"] time[class="date-publication"]').xpath('text()').extract()
        pprint(dates)
        if type(dates) is list:
            for date in dates:
                if date.strip() in [
                    u"Publié aujourd'hui",
                    u"Publié il y a 1 jours",
                    u"Publié hier",
                    u"Publiée il y a 1 jour",
                    u"Publiée il y a moins de 24h"
                ]:
                    return True
        return False









