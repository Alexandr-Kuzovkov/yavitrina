#!/usr/bin/env python

#Script to check duplicate job_id in dejobmonitor's feed

from dbacc import *
from mylogger import logger
import requests
import re
import math

regularExp = re.compile('<jobs total=\"[0-9]+\"')
regularExp2 = re.compile('<job id=\"[0-9]+\"')
LIMIT = 1000
URL = 'https://de.jobmonitor.com/search/xml/?utm_source=XTRA'
job_ids = []


def get_count_jobs(url):
    url = '&'.join([url, 'limit=1', 'page=1'])
    r = requests.get(url, verify=False)
    if r.status_code == 200:
        try:
            count = regularExp.search(r.text).group()[13:-1]
        except Exception, ex:
            logger.error('Can\'t get count jobs!')
            count = 0
        return int(count)

count_jobs = get_count_jobs(URL)
last_page = int(math.ceil(count_jobs / LIMIT) + 1)
logger.info('last page: %i' % last_page)
urls = map(lambda i: '%s&limit=%i&page=%i' % (URL, LIMIT, i), range(1, last_page + 1))

for url in urls:
    r = requests.get(url, verify=False)
    if r.status_code == 200:
        logger.info('... content of the feed "%s" got succesfully' % url)
        logger.info('...start checking...')
        content = r.text
        res = regularExp2.findall(r.text)
        for item in res:
            job_id = int(item[9:-1])
            if job_id not in job_ids:
                job_ids.append(job_id)
            else:
                logger.warning('job_id=%i is duplicate!' % job_id)
        logger.info('...done...')
logger.info('...all checking done!')






