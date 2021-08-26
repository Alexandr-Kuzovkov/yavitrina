#!/usr/bin/env python

#Script for tracker testing

import requests
import threading
import logging
import urllib3
import time
import certifi

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#enabling logging
logger = logging.getLogger('MYLOGGER')
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch2.setFormatter(formatter)
logger.addHandler(ch2)
logger.setLevel(1)



class myThread(threading.Thread):

    def __init__(self, url, logger):
        threading.Thread.__init__(self)
        self.url = url
        self.logger = logger

    def run(self):
        self.logger.info('get request to "%s"...' % self.url)
        res = requests.get(url, headers=headers)
        self.logger.info('url: %s, code: %i' % (url, res.status_code))



urls = [
    'http://localhost:3000/t/0e71b3b5-d909-43fb-a85a-59735e474a1f?s=095f02a2-0efb-11e7-844e-2c56dc4b235a',
    'http://localhost:3000/t/6c9e1b34-6a4c-11e8-8a4c-000d3a75f712?s=095f02a2-0efb-11e7-844e-2c56dc4b235a',
    'http://localhost:3000/t/0e71b3b5-d909-43fb-a85a-59735e474a1f?s=095f02a2-0efb-11e7-844e-2c56dc4b235a',
    'http://localhost:3000/t/70d7b74a-5685-11e7-8786-2c56dc4b235a?s=0aa2ed23-91eb-46e6-b2e6-7df56964b382',
    'http://localhost:3000/t/0e71b3b5-d909-43fb-a85a-59735e474a1f?s=095f02a2-0efb-11e7-844e-2c56dc4b2351',
    'http://localhost:3000/t/70d7b74a-5685-11e7-8786-2c56dc4b235a?s=0aa2ed23-91eb-46e6-b2e6-7df56964b311',
]

headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36'
}

threads = []

while True:
    time.sleep(1)
    for url in urls:
        logger.info('put request GET "%s" to queue' % url)
        threads.append(myThread(url, logger))
        threads[len(threads) - 1].start()
