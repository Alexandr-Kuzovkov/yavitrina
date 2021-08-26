#!/usr/bin/env python

#Script to check duplicate job url in jobmonitor's feed

from dbacc import *
from mylogger import logger
import requests
import re
import math

regularExp = re.compile('http:\/\/www.muenchenmarketingjobs.com\/job\/.*\.html')
LIMIT = 1000
URL = 'https://muenchenmarketingjobs.com/job.xml'
job_ids = []


d = {}
url =URL
r = requests.get(url, verify=False)
if r.status_code == 200:
    logger.info('... content of the feed "%s" got succesfully' % url)
    logger.info('...start checking...')
    content = r.text
    res = regularExp.findall(r.text)
    print len(res)
    for item in res:
        if item not in d:
            d[item] = 1
        else:
            d[item] += 1
    logger.info('...done...')
    logger.info('Result:')
    for url, count in d.items():
        print '%s: %i ' % (url, count)
logger.info('...all checking done!')






