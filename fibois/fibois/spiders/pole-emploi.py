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
from fibois.scrapestack import ScrapestackRequest

class PoleEmploiSpider(scrapy.Spider):
    name = 'pole-emploi'
    allowed_domains = ['www.pole-emploi.fr', 'candidat.pole-emploi.fr', 'api.scrapestack.com']
    page = 1
    dirname = 'pole-emploi'
    lua_src = pkgutil.get_data('fibois', 'lua/pole-emploi.lua')
    handle_httpstatus_list = [400, 404]
    h = html2text.HTML2Text()
    h.ignore_emphasis = True
    h.ignore_links = True
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    delta = False
    #keywords = occupations[-4:-3]
    keywords = occupations
    keywords_type = 'occupations'
    keywords_limit = None
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = False
    es_exporter = None

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, keywords=False, limit=False, keywords_limit=False, drain=False, delta=False, debug=False, *args, **kwargs):
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
        self.debug = debug

    def start_requests(self):
        if self.debug:
            self.logger.warning('!!!!!!!!!DEBUG!!!!!!!!!')
            url = 'https://candidat.pole-emploi.fr/offres/recherche/detail/9497775'
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page)
            job = {'url': url}
            request.meta['job'] = job
            yield request
        else:
            for keyword in self.keywords:
                self.pagination[keyword] = {'page': 1, 'pages': None}
            for keyword in self.keywords:
                rng = self.get_range(keyword)
                url = 'https://candidat.pole-emploi.fr/offres/recherche?motsCles={keyword}&offresPartenaires=true&range={range}&rayon=10&tri=1'.format(range=rng, keyword=urllib.quote_plus(keyword))
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_list_page)
                request.meta['keyword'] = keyword
                yield request

    def parse_job_list_page(self, response):
        #pprint(response.text)
        h1 = ' '.join(response.css('h1[class="title"]').xpath('text()').extract())
        count = 0
        try:
            count = int(h1.strip().split(' ')[0])
        except Exception as ex:
            self.logger.warning(ex)
        keyword = response.meta['keyword']
        self.logger.info(h1)
        self.logger.info('keyword: "{keyword}" - {count} offers found'.format(keyword=keyword, count=count))
        if self.pagination[keyword]['pages'] is None:
            self.pagination[keyword]['pages'] = int(math.ceil(count/20.0))
        links = response.css('ul[class="result-list list-unstyled"] li a[class="media with-fav"]').xpath('@href').extract()
        self.logger.debug(links)
        results_block = response.css('ul[class="result-list list-unstyled"] li a[class="media with-fav"]').extract()
        page_results = {'job_board': 'pole-emploi.fr', 'job_board_url': 'https://www.pole-emploi.fr', 'page_url': response.url, 'offers': []}
        for i in range(0, len(links)):
            url = 'https://candidat.pole-emploi.fr{link}'.format(link=links[i])
            job = {'url': url}
            job['search_term'] = response.meta['keyword']
            job['keyword_type'] = self.keywords_type
            if len(results_block) > i:
                job['html'] = results_block[i]
                html = results_block[i]
                body = html.encode('utf-8')
                offer_block = response.replace(body=body)
                job['title'] = ' '.join(offer_block.css('h2[class="t4 media-heading"]').xpath('text()').extract())
                job['contract_info'] = ' '.join(offer_block.css('p[class="contrat"]').xpath('text()').extract())
                job['contract_type'] = ' '.join(offer_block.css('p[class="contrat"] span[class="type-contrat"]').xpath('text()').extract())
            page_results['offers'].append(job)
            if self.use_scrapestack:
                request = ScrapestackRequest(url, callback=self.parse_job_detail_page, dont_filter=True, access_key=self.scrapestack_access_key)
            else:
                request = scrapy.Request(url, callback=self.parse_job_detail_page, dont_filter=True)
            request.meta['job'] = job
            yield request
        if self.pagination[keyword]['page'] < self.pagination[keyword]['pages'] and count > 0:
            if self.check_published_at(response) or not self.delta:
                self.pagination[keyword]['page'] += 1
                rng = self.get_range(keyword)
                url = 'https://candidat.pole-emploi.fr/offres/recherche?motsCles={keyword}&offresPartenaires=true&range={range}&rayon=10&tri=1'.format(range=rng, keyword=urllib.quote_plus(keyword))
                if self.use_scrapestack:
                    request = ScrapestackRequest(url, callback=self.parse_job_list_page, access_key=self.scrapestack_access_key)
                else:
                    request = scrapy.Request(url, callback=self.parse_job_list_page)
                request.meta['keyword'] = keyword
                yield request
        if not self.drain:
            self.es_exporter.insert_page_results_html(page_results)

    def parse_job_detail_page(self, response):
        job = response.meta['job']
        self.logger.debug('!!!!!JOB URL: {url}'.format(url=job['url']))
        l = ItemLoader(item=JobItem(), response=response)
        for key, val in job.items():
            l.add_value(key, val)
        l.add_value('jobboard', 'pole-emploi.fr')
        l.add_value('scrapping_date', self.get_scrapping_date())
        l.add_value('contact', '')
        l.add_value('url_origin', job['url'])
        #content = ' '.join(response.css('div[class="modal-body"]').extract())
        #content = self.h.handle(content).encode('utf-8').strip()
        #content = self.h.handle(content).strip()
        location = ' '.join(response.css('p[itemprop="jobLocation"] span[itemprop="address"] span[itemprop="name"]').xpath('text()').extract())
        publish_date = ' '.join(response.css('p[itemprop="jobLocation"] span[itemprop="datePosted"]').xpath('@content').extract())
        profile = self.get_profile(response)
        sector = ' '.join(response.css('div[class="modal-body"] ul li span[itemprop="industry"]').xpath('text()').extract())
        company_name = ' '.join(response.css('span[itemprop="hiringOrganization"] span[itemprop="name"]').xpath('@content').extract())
        company_info = ' '.join(response.css('div[class="media"] div[class="media-body"]').xpath('text()').extract())
        salary = ' '.join(response.css('span[itemprop="baseSalary"] + ul li').xpath('text()').extract())
        experience_level = ' '.join(response.css('span[itemprop="experienceRequirements"]').xpath('text()').extract())
        education_level = ' '.join(response.css('span[itemprop="educationRequirements"]').xpath('text()').extract())
        header = ' '.join([' '.join(response.css('h1[itemprop="title"]').extract()), ' '.join(response.css('h1[itemprop="title"] + p').extract()), ' '.join(response.css('h1[itemprop="title"] + p + p').extract())])
        html_content = ' '.join(response.css('div[itemtype="http://schema.org/JobPosting"]').extract()).replace(' '.join(response.css('div[class="block-other-offers with-header results"]').extract()), '')
        content = html_content
        content = self.h.handle(content).strip()
        if len(sector) > 0:
            l.add_value('sector', sector)
        if len(location) > 0:
            l.add_value('location', location)
        if len(publish_date) > 0:
            l.add_value('publish_date', publish_date)
        if len(profile) > 0:
            l.add_value('profile', profile)
        if len(company_name) > 0:
            l.add_value('company_name', company_name)
        if len(company_info) > 0:
            l.add_value('company_info', company_info)
        if len(salary) > 0:
            l.add_value('salary', salary)
        if len(experience_level) > 0:
            l.add_value('experience_level', experience_level)
        if len(education_level) > 0:
            l.add_value('education_level', education_level)
        l.add_value('content', content)
        l.add_value('source', 'scrapy')
        l.add_value('header', header)
        l.add_value('html_content', html_content)
        yield l.load_item()
        job_details = {'html': html_content, 'title': header, 'url': job['url']}
        if not self.drain:
            self.es_exporter.insert_job_details_html(job_details)

    def get_profile(self, response):
        headers = response.css('div[class="modal-body"] h4[class="t6 skill-subtitle"]')
        uls = response.css('div[class="modal-body"] ul[class="skill-list list-unstyled"]')
        profile = []
        for i in range(0, len(headers)):
            profile.append(''.join(headers[i].xpath('text()').extract()) + ':')
            if len(uls) > i:
                skill =', '.join(uls[i].css('li span[class="skill"] span[class="skill-name"]').xpath('text()').extract())
                profile.append(skill)
        return ' '.join(profile)

    def check_published_at(self, response):
        if not self.delta:
            return True
        dates = response.css('ul[class="result-list list-unstyled"] li p[class="date"]').xpath('text()').extract()
        #pprint(dates)
        if type(dates) is list:
            for date in dates:
                if date.strip() in [u"Publié aujourd'hui", u"Publié il y a 1 jours", u"Publié hier"]:
                    return True
        return False

    def get_scrapping_date(self):
        scrapping_date = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        return scrapping_date

    def get_range(self, keyword):
        p = (self.pagination[keyword]['page'] - 1) * 20
        d = p + 19
        rng = '{p}-{d}'.format(p=p, d=d)
        return rng






