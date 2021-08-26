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

class EmploymentcrossingSpider(scrapy.Spider):

    name = "employmentcrossing"
    publisher = "Employmentcrossing"
    publisherurl = 'https://www.employmentcrossing.com/'
    url_index = None
    dirname = 'employmentcrossing'
    limit = False
    drain = False
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
        #if use_selenium:
        #    self.use_selenium = True

    def start_requests(self):
        allowed_domains = ["https://www.employmentcrossing.com/"]
        urls = [
            'https://www.employmentcrossing.com/employers/resume-search.php?page=1&frmtp=otherlegal&kw=&text=&jt=&ind=&skl=&frm=&mi_ex=&ma_ex=&dgr=&uni=&fr_grdy=&to_grdy=&rg_sel=single&co=United+Kingdom&loc=&radius=500&mco=United+States&mloc=&rsu=&scrtcl=&asoc=&cert=&lang=',
            'https://www.employmentcrossing.com/employers/resume-search.php?page=1&frmtp=otherlegal&kw=&text=&jt=&ind=&skl=&frm=&mi_ex=&ma_ex=&dgr=&uni=&fr_grdy=&to_grdy=&rg_sel=single&co=United+States&loc=&radius=500&mco=United+States&mloc=&rsu=&scrtcl=&asoc=&cert=&lang=',
            'https://www.employmentcrossing.com/employers/resume-search.php?page=1&frmtp=otherlegal&kw=&text=&jt=&ind=&skl=&frm=&mi_ex=&ma_ex=&dgr=&uni=&fr_grdy=&to_grdy=&rg_sel=single&co=France&loc=&radius=500&mco=United+States&mloc=&rsu=&scrtcl=&asoc=&cert=&lang='
        ]

        if not self.use_selenium:
            for index in range(len(urls)):
                if self.url_index is not None and index != self.url_index:
                    continue
                url = urls[index]
                request = scrapy.Request(url, self.get_cv_list)
                parsed = urlparse.urlparse(url)
                request.meta['industry'] = self.getIndustrySlug('-'.join(urlparse.parse_qs(parsed.query)['co']))
                request.meta['page'] = 1
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

    def get_cv_list(self, response):
        search_url = response.meta['search_url']
        industry = response.meta['industry']
        page = response.meta['page']
        links = response.css('a[class="bgsmallbutton_green"]').xpath('@href').extract()
        for link in links:
            i = link.find('resumeid=')
            resumeid = link[i+9:]
            url = 'https://www.employmentcrossing.com/employers/lcjpresumepdf.php?resumeid={resumeid}&frmadmn=Yes'.format(resumeid=resumeid)
            request = scrapy.Request(url, callback=self.download_cv)
            request.meta['industry'] = industry
            request.meta['resumeid'] = resumeid
            yield request
        next_link = response.xpath('//*[text()="Next"]')[-1:].xpath('@href').extract()
        if len(next_link) > 0:
            next_link = next_link[0]
            curr_page = 'page={page}'.format(page=page)
            n_page = next_link[next_link.find('(')+1:next_link.find(')')]
            next_page = 'page={page}'.format(page=n_page)
            next_url = search_url.replace(curr_page, next_page)
            request = scrapy.Request(next_url, callback=self.get_cv_list)
            request.meta['search_url'] = next_url
            request.meta['page'] = n_page
            request.meta['industry'] = industry
            yield request

    def download_cv(self, response):
        l = ItemLoader(item=IndeedItem())
        headers = response.headers
        content_disposition = headers['Content-Disposition']
        i = content_disposition.find('filename=')
        filename = content_disposition[i+9:]
        l.add_value('name', '.'.join([response.meta['resumeid'], 'pdf']))
        l.add_value('industry', response.meta['industry'])
        l.add_value('body', response.body)
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


