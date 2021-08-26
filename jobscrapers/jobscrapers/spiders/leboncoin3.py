# -*- coding: utf-8 -*-
import scrapy
import time
from scrapy.loader import ItemLoader
import urllib
import urlparse
from pprint import pprint
import json
from scrapy.loader import ItemLoader
from jobscrapers.items import LeboncoinItem
from jobscrapers.items import PlainItem
from jobscrapers.items import annotations_list
import time
import pkgutil
from scrapy_splash import SplashRequest
import scrapy_splash
import math
import re
from base64 import b64encode
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from jobscrapers.items import wood_jobs
from random import randint

r_city = re.compile('^.*\(')
r_postal_code = re.compile('\([0-9]{1,}\)$')
r_city2 = re.compile('^.*\(')

###############################################
# scrap leboncoin as raw text use selenium
###############################################
class Leboncoin3Spider(scrapy.Spider):

    name = "leboncoin3"
    handle_httpstatus_list = [403]
    publisher = "Leboncoin"
    publisherurl = 'https://www.leboncoin.fr'
    lua_src = pkgutil.get_data('jobscrapers', 'lua/leboncoin3.lua')
    url_index = None
    dirname = 'leboncoin3'
    limit = False
    drain = False
    rundebug = False
    min_item_len = 5
    min_len = 50
    cities_file_content = pkgutil.get_data('jobscrapers', 'data/GEODATASOURCE-CITIES-FREE.TXT')
    cities = []
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    orders = {}
    driver = None

    def __init__(self, limit=False, drain=False, debug=False, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.rundebug = True
        self.cities = map(lambda k: k[1].strip().lower().decode('utf-8'), filter(lambda j: len(j) > 1, map(lambda i: i.split('\t'), self.cities_file_content.split('\n'))))
        self.driver = webdriver.Firefox()

    def start_requests(self):
        allowed_domains = ["https://www.leboncoin.fr"]
        if self.rundebug:
            self.logger.info('Debug!!!')
            #url = 'https://2ip.ru/'
            url = 'https://www.leboncoin.fr/offres_d_emploi/1633562515.htm/'
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = url[:-1].split('/').pop()
            pprint(url)
            yield request
        else:
            for title in wood_jobs[0:1]:
                url = 'https://www.leboncoin.fr/recherche/?text={title}'.format(title=title)
                title_links = []
                self.driver.get(url)
                time.sleep(3)
                self.wait_captcha_solve()
                links = self.get_links_from_page(url)
                pprint(links)
                title_links += links
                next_link = self.get_next_link()
                pprint(next_link)
                while next_link:
                    links = self.get_links_from_page(next_link)
                    title_links += links
                    next_link = self.get_next_link()
                url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
                request = scrapy.Request('{url}?={p}'.format(url=url, p=randint(1, 100000)), callback=self.get_jobs_for_title)
                request.meta['links'] = title_links
                yield request


    def get_next_link(self):
        try:
            link_next = self.driver.find_element_by_css_selector('nav[class="nMaRG"] ul li a')
        except NoSuchElementException:
            return False
        else:
            link_next_href = link_next.get_attribute('href')
            return link_next_href
        return False

    def get_links_from_page(self, url):
        self.driver.get(url)
        time.sleep(3)
        try:
            links = self.driver.find_elements_by_css_selector('a[class="clearfix trackable"]')
        except NoSuchElementException:
            return False
        else:
            links = map(lambda i: i.get_attribute('href'), links)
            return links

    def get_jobs_from_page_old(self, links):
        pprint('here!!!!!!!!!!!!')
        pprint(links)
        for link in links:
            self.logger.info('Scrap job: {link}'.format(link=link))
            self.driver.get(link)
            time.sleep(3)
            self.driver.execute_script("document.getElementsByClassName('TextLink-15wnQ')[0].click();")
            time.sleep(3)
            raw1 = self.driver.execute_script(
                "return document.querySelector('div[data-qa-id=\"adview_spotlight_description_container\"]').innerHTML;")
            raw2 = '<div>Description</div>' + self.driver.execute_script(
                "return document.querySelector('div[data-qa-id=\"adview_description_container\"] div span[class=\"content-CxPmi\"]').innerHTML;")
            raw3 = '<div>Critères</div>' + self.driver.execute_script(
                "return document.querySelector('div[data-qa-id=\"criteria_container\"]').innerHTML;")
            raw4 = '<div>Localisation</div>' + self.driver.execute_script(
                "return document.querySelector('div[data-qa-id=\"adview_location_container\"]').innerHTML;")
            html = '\n'.join([raw1, raw2, raw3, raw4])
            url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
            request = scrapy.Request('{url}?={p}'.format(url=url, p=randint(1, 100000)), callback=self.parse_job)
            request.meta['html'] = html
            request.meta['name'] = link[:-1].split('/').pop()
            yield request

    def get_jobs_for_title(self, response):
        links = response.meta['links']
        for link in links:
            url = link
            request = SplashRequest(url, self.parse_job, endpoint='execute', args={'wait': 0.5, 'lua_source': self.lua_src.decode('utf-8'), 'timeout': 3600})
            request.meta['name'] = link[:-1].split('/').pop()
            yield request


    def parse_job(self, response):
        try:
            data = json.loads(response.text)
        except Exception, ex:
            self.logger.error(response.text)
            raise Exception(ex)
        name = response.meta['name']
        # plain text
        l2 = ItemLoader(item=PlainItem())
        l2.add_value('name', name)
        l2.add_value('subfolder', 'plaintext')
        l2.add_value('itemtype', 'plaintext')
        html = '\n'.join([
            self.get_element(data, 'raw1'),
            self.get_element(data, 'raw2'),
            self.get_element(data, 'raw3'),
            self.get_element(data, 'raw4')
        ])
        l2.add_value('text', html)
        yield l2.load_item()

    def wait_captcha_solve(self):
        count = 100
        while True:
            links = self.driver.find_elements_by_css_selector('a[class="clearfix trackable"]')
            if len(links) > 0 or count <= 0:
                self.logger.info('captcha solved!!!')
                break
            count -= 1
            time.sleep(10)
            self.logger.info('wait captcha!!!')
        return

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

    def add_element_from_data(self, l, data, name, key, annotation, html=False):
        if key in data:
            value = data[key]
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def add_element(self, l, text, name, key, annotation, html=False):
            value = text
            if html:
                value = self.rm_spaces(self.cut_tags(value))
            l.add_value(key, value)
            l.add_value(key, annotation)
            self.add_order(name, key)

    def get_element(self, data, key):
        if key in data:
            return data[key]
        return ''




