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
from jobscrapers.items import categories
import time
import pkgutil
from transliterate import translit
from scrapy_splash import SplashRequest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from random import randint

####################
# scrapy crawl indeed -a dirname=indeed-fr2 -a use_selenium=1 -a lang=fr -a params="&hl=fr&co=FR"
#########################

class IndeedSpider(scrapy.Spider):

    name = "indeed"
    publisher = "Indeed"
    publisherurl = 'https://resumes.indeed.com/search'
    lua_src1 = pkgutil.get_data('jobscrapers', 'lua/indeed-getlinks.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/indeed-getcv.lua')
    url_index = None
    dirname = 'indeed'
    limit = False
    drain = False
    params = ''#-a params="&hl=fr&co=FR"  for get FR CVs
    lang = None
    use_selenium = False

    def __init__(self, url_index=None, limit=False, drain=False, params='', dirname=False, lang=None, use_selenium=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if url_index is not None:
            self.url_index = int(url_index)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        self.params = params
        if dirname:
            self.dirname = str(dirname)
        self.lang = lang
        if use_selenium:
            self.use_selenium = True

    def start_requests(self):
        allowed_domains = ["https://resumes.indeed.com"]
        urls = []
        for category in categories:
            urls.append('https://resumes.indeed.com/search?l=&lmd=all&q={category}&searchFields=jt{params}'.format(category=urllib.quote(category), params=self.params))
        if not self.use_selenium:
            for index in range(len(urls)):
                if self.url_index is not None and index != self.url_index:
                    continue
                url = urls[index]
                splash_args = {'wait': 0.5, 'lua_source': self.lua_src1, 'timeout': 3600}
                if self.limit:
                    splash_args['limit'] = self.limit
                request = SplashRequest(url, self.get_jobs_list, endpoint='execute', args=splash_args)
                parsed = urlparse.urlparse(url)
                request.meta['industry'] = self.getIndustrySlug('-'.join(urlparse.parse_qs(parsed.query)['q']))
                request.meta['search_url'] = url
                yield request
        else:
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request('{url}?={p}'.format(url=url, p=randint(1, 100000)), callback=self.get_job_links_with_selenium)
            request.meta['urls'] = urls
            yield request

    def get_job_links_with_selenium(self, response):
        urls = response.meta['urls']
        driver = webdriver.Firefox()
        for index in range(len(urls)):
            if self.url_index is not None and index != self.url_index:
                continue
            url = urls[index]
            self.logger.info('URL: %s' % url)
            links = []
            driver.get(url)
            driver.implicitly_wait(10)
            self.wait_captcha_solve(driver)
            try:
                elements = driver.find_elements_by_css_selector('div[class="rezemp-ResumeSearchCard-contents"] div span a')
            except NoSuchElementException:
                pass
            else:
                links += map(lambda el: el.get_attribute('href'), elements)
            while True:
                try:
                    next_btn = driver.find_element_by_css_selector('span[class="icl-TextLink icl-TextLink--primary rezemp-pagination-nextbutton"]')
                    next_btn.click()
                    time.sleep(2)
                    alert = driver.find_element_by_css_selector('span[class ="icl-Alert-headline"]')
                    if alert.text == u'Erreur':
                        break
                    elements2 = driver.find_elements_by_css_selector('div[class="rezemp-ResumeSearchCard-contents"] div span a')
                    for el in elements2:
                        links.append(el.get_attribute('href'))
                    #links += map(lambda el: el.get_attribute('href'), elements2)
                except NoSuchElementException:
                    break
            local_url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request('{url}?={p}'.format(url=local_url, p=randint(1, 100000)), callback=self.get_jobs_list)
            #self.logger.info(str(links))
            request.meta['links'] = links
            parsed = urlparse.urlparse(url)
            request.meta['industry'] = self.getIndustrySlug('-'.join(urlparse.parse_qs(parsed.query)['q']))
            request.meta['search_url'] = url
            yield request
        driver.close()

    def wait_captcha_solve(self, driver):
        count = 100
        while True:
            links = driver.find_elements_by_css_selector('div[class="rezemp-ResumeSearchCard-contents"] div span a')
            if len(links) > 0 or count <= 0:
                self.logger.info('captcha solved!!!')
                break
            count -= 1
            self.logger.info('wait captcha!!!')
        return

    def get_jobs_list(self, response):
        search_url = response.meta['search_url']
        industry = response.meta['industry']
        if not self.use_selenium:
            data = json.loads(response.text).values()
        else:
            data = response.meta['links']
        if self.limit:
            data = data[:self.limit]
        for uri in data:
            name = uri[8:uri.find('?s')]
            if not self.use_selenium:
                url = '/'.join(['https://resumes.indeed.com', uri])
            else:
                url = uri
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src2, 'timeout': 3600})
            request.meta['name'] = name.split('/').pop()
            request.meta['industry'] = industry
            yield request

    def parse_job(self, response):
        data = json.loads(response.text)
        png_body = data[0]
        html = data[1]
        l = ItemLoader(item=IndeedItem())
        l.add_value('name', response.meta['name'])
        l.add_value('industry', response.meta['industry'])
        l.add_value('png', png_body)
        l.add_value('text', html)
        yield l.load_item()

    def transliterate(self, str):
        try:
            str = translit(str.strip().lower(), reversed=True)
        except Exception, ex:
            str = str.strip().lower()
        return str

    def removeNonValidChars(self, str):
        c = []
        valid = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-()'
        for ch in str:
            if ch in valid:
                c.append(ch)
            else:
                c.append('-')
        return ''.join(c)

    def getIndustrySlug(self, industry):
        return self.removeNonValidChars(self.transliterate(industry))


