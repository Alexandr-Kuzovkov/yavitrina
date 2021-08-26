#!/usr/bin/env python

#Clean srapyd queue

from mylogger import logger
import requests
import re
import math
import json
import sqlite3

URL = 'http://localhost:6800'
dbfile = '/home/ubuntu/dbs/feedgenerator.db'
dbfile_sk = '/home/ubuntu/SpiderKeeper.db'


def get_jobs(url, status):
    url = '/'.join([url, 'listjobs.json?project=feedgenerator'])
    r = requests.get(url, verify=False)
    jobs = []
    if r.status_code == 200:
        try:
            d = json.loads(r.text)
            for item in d[status]:
                jobs.append(item['id'])
        except Exception, ex:
            logger.error('Error parsing response: %s' % ex.message)
        return jobs

def clean_queue(dbfile):
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute('delete from spider_queue')
    conn.commit()
    conn.close()

def clean_queue_sk(dbfile):
    conn = sqlite3.connect(dbfile)
    cur = conn.cursor()
    cur.execute('delete from sk_job_instance')
    conn.commit()
    conn.close()



logger.info('clean queue in database...')
clean_queue(dbfile)
logger.info('done...')

logger.info('clean queue in database sk...')
clean_queue_sk(dbfile_sk)
logger.info('done...')

logger.info('fetching runnig jobs...')
jobs = get_jobs(URL, 'running')
logger.info('%i running jobs fetched' % len(jobs))
logger.info('removing running jobs...')
for job in jobs:
    url = '/'.join([URL, 'cancel.json'])
    payload = {'project': 'feedgenerator', 'job': job}
    r = requests.post(url, verify=False, data=payload)
    if r.status_code == 200:
        logger.info('...result: %s ' % r.text)
logger.info('...all done!')

logger.info('fetching pending jobs...')
jobs = get_jobs(URL, 'pending')
logger.info('%i pending jobs fetched' % len(jobs))
logger.info('removing pending jobs...')
for job in jobs:
    url = '/'.join([URL, 'cancel.json'])
    payload = {'project': 'feedgenerator', 'job': job}
    r = requests.post(url, verify=False, data=payload)
    if r.status_code == 200:
        logger.info('...result: %s ' % r.text)
logger.info('...done!')










