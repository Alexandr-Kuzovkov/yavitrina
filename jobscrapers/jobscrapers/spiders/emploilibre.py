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
from scrapy_splash import SplashFormRequest
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from random import randint
import requests
import os

class EmploilibreSpider(scrapy.Spider):

    name = "emploilibre"
    publisher = "Emploilibre"
    publisherurl = 'https://www.emploilibre.fr'
    dirname = 'emploilibre'
    limit = False
    drain = False
    lang = None
    use_selenium = False
    lua_src = pkgutil.get_data('jobscrapers', 'lua/emploilibre.lua')
    lua_src2 = pkgutil.get_data('jobscrapers', 'lua/emploilibre-login.lua')
    email = 'm.picot@myxtramile.com'
    password = ''
    user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'
    env_content = pkgutil.get_data('jobscrapers', 'data/.env')
    search = 'er'
    rundebug = False
    cookie = 'PHPSESSID=oh1sietr1jmenag7gks2eker36; _ga=GA1.2.622534105.1559916714; _gid=GA1.2.1467617699.1560151300; _gat=1' #Look in browser after login

    def __init__(self, limit=False, drain=False, dirname=False, lang=None, debug=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if dirname:
            self.dirname = str(dirname)
        self.lang = lang
        self.password = map(lambda line: line.replace('EMPLOILIBRE_PASSWORD=', '').strip(), filter(lambda line: line.startswith('EMPLOILIBRE_PASSWORD'), self.env_content.split('\n')))[0]
        if debug:
            self.rundebug = True
        #if use_selenium:
        #    self.use_selenium = True

    def start_requests(self):
        allowed_domains = ["https://www.emploilibre.fr"]
        if not self.rundebug:
            url = 'https://www.emploilibre.fr/modules/connexion'
            self.logger.info('Account: email="{email}"; password="{password}"'.format(email=self.email, password=self.password))
            splash_args = {'wait': 0.5, 'lua_source': self.lua_src, 'timeout': 3600, 'email': self.email,
                           'password': self.password, 'search': self.search}
            request = SplashRequest(url, self.download_cv2, endpoint='execute', args=splash_args)
            yield request
        else:
            self.logger.info('Debug run!!!')
            self.debug()

    #link = 'modules_secur/telechargercv_r_publicite?nominitial=Catherine-CRENN-Assistante_Polyvalente.pdf&nomupload=4ypxtLGbLF&idcv=3286&dossier=gBqwlTg1JbgBO716w5tR1drslbyOVRd18tekcc3D&extension=pdf&publicite=oui'
    #url = 'https://www.emploilibre.fr/compte/modules_secur/telechargercv_r?nominitial=Val%C3%A9rie-LAHANQUE-Responsable_administratif_%E2%80%A2_Office_Manager.pdf&nomupload=77O4fIgIt7&idcv=3297&dossier=HS9hyfANCjs30cmpSWnJuAuAuM8Pn4RMERT1OaEe&extension=pdf'

    def get_cv_list(self, response):
        links = json.loads(response.text)['links'].values()
        cookies = json.loads(response.text)['cookies']
        cookie = ';'.join(map(lambda i: '='.join([i['name'], i['value']]), cookies))
        pprint(cookie)
        #pprint(links)
        self.logger.info('Fetched {count} links'.format(count=len(links)))
        time.sleep(3)
        for link in links:
            url = '/'.join(['https://www.emploilibre.fr/compte', link.replace('telechargercv_r_publicite', 'telechargercv_r').replace('&publicite=oui', '')])
            request = scrapy.Request(url, callback=self.download_cv)
            headers = request.headers
            headers['User-Agent'] = self.user_agent
            headers['Cookie'] = self.cookie
            headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
            headers['Accept-Encoding'] = 'gzip, deflate, br'
            headers['Accept-Language'] = 'en-US,en;q=0.9,ru;q=0.8,fr;q=0.7,de;q=0.6'
            headers['Cache-Control'] = 'max-age=0'
            headers['Connection'] = 'keep-alive'
            headers['Upgrade-Insecure-Requests'] = '1'
            if 'Referer' in headers:
                del headers['Referer']
            request = request.replace(headers=headers)
            yield request

    def download_cv(self, response):
        l = ItemLoader(item=IndeedItem())
        headers = response.headers
        if 'Content-disposition' in headers:
            content_disposition = headers['Content-disposition']
            i = content_disposition.find('filename=')
            filename = content_disposition[i + 9:]
            l.add_value('name', filename)
            l.add_value('body', response.body)
            yield l.load_item()
        else:
            self.logger.info('fail download CV:')
            headers = response.request.headers
            pprint(headers)

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


    def download_cv2(self, response):
        links = json.loads(response.text)['links'].values()
        cookies = json.loads(response.text)['cookies']
        cookie = ';'.join(map(lambda i: '='.join([i['name'], i['value']]), cookies))
        pprint(cookie)
        #pprint(links)
        self.logger.info('Fetched {count} links'.format(count=len(links)))
        time.sleep(3)
        for link in links:
            url = '/'.join(['https://www.emploilibre.fr/compte', link.replace('telechargercv_r_publicite', 'telechargercv_r').replace('&publicite=oui', '')])
            headers = {}
            headers['User-Agent'] = self.user_agent
            headers['Cookie'] = self.cookie
            headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
            headers['Accept-Encoding'] = 'gzip, deflate, br'
            headers['Accept-Language'] = 'en-US,en;q=0.9,ru;q=0.8,fr;q=0.7,de;q=0.6'
            headers['Cache-Control'] = 'max-age=0'
            headers['Connection'] = 'keep-alive'
            headers['Upgrade-Insecure-Requests'] = '1'
            res = requests.get(url=url, headers=headers, stream=True)
            if 'Content-disposition' in res.headers:
                content_disposition = res.headers['Content-disposition']
                i = content_disposition.find('filename=')
                filename = content_disposition[i + 9:].replace('/', '_')
                with open(os.path.sep.join(['files', self.dirname, filename]), 'wb') as f:
                    f.write(res.content)
        url = self.settings.get('SPLASH_URL', 'http://localhost:8050/')
        request = scrapy.Request('{url}?={p}'.format(url=url, p=randint(1, 100000)),callback=self.done)
        yield request

    def debug(self):
        url = 'https://www.emploilibre.fr/compte/modules_secur/telechargercv_r?nominitial=Jamila-MOUSSIKIAN-Secr%C3%A9taire_administrative.docx&nomupload=kB0meG6J1P&idcv=2711&dossier=wtrn4l1ycDH3BaD8AD1lndZWgyZHaUHxcPDZ0umU&extension=docx'
        headers = {}
        headers['User-Agent'] = self.user_agent
        headers['Cookie'] = 'PHPSESSID=oh1sietr1jmenag7gks2eker36; _ga=GA1.2.622534105.1559916714; _gid=GA1.2.1467617699.1560151300; _gat=1'
        headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
        headers['Accept-Encoding'] = 'gzip, deflate, br'
        headers['Accept-Language'] = 'en-US,en;q=0.9,ru;q=0.8,fr;q=0.7,de;q=0.6'
        headers['Cache-Control'] = 'max-age=0'
        headers['Connection'] = 'keep-alive'
        headers['Upgrade-Insecure-Requests'] = '1'
        res = requests.get(url=url, headers=headers, stream=True)
        pprint(res.headers)
        if 'Content-disposition' in res.headers:
            content_disposition = res.headers['Content-disposition']
            i = content_disposition.find('filename=')
            filename = content_disposition[i + 9:]
            with open(os.path.sep.join(['files', self.dirname, filename]), 'wb') as f:
                f.write(res.content)

    def done(self, response):
        self.logger.info('Done')




