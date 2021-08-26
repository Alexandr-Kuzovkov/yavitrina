#!/usr/bin/env python

#Script to check that employer's jobs are in feeds
#Options:
#    -l , --local get feeds from local filesystem
#    --updated = 2018-06-01 get feeds updated at least this date
#    --employer=101 pass employer ID
#    --status=0,2 pass statuses of jobs, which will be search in feeds
#    -v  verbose mode, print job's external_unique_id

from dbacc import *
from mylogger import logger
import requests
import re
import math
from pg import PgSQLStore
import getopt
import sys

URL = 'http://feeds.xtramile.io'
FEED_DIR = '/mnt/boardfeeds'
EMPLOYER_ID=104
LOCAL = False
UPDATED = None
STATUSES = [1, 3]
VERBOSE = False

try:
    optlist, args = getopt.getopt(sys.argv[1:], 'lv', ['local', 'updated=', 'employer=', 'status='])
    if '-l' in map(lambda item: item[0], optlist):
        LOCAL = True
    elif '--local' in map(lambda item: item[0], optlist):
        LOCAL = True

    if '--updated' in map(lambda item: item[0], optlist):
        UPDATED = filter(lambda item: item[0] == '--updated', optlist)[0][1]

    if '--employer' in map(lambda item: item[0], optlist):
        EMPLOYER_ID = int(filter(lambda item: item[0] == '--employer', optlist)[0][1])

    if '--status' in map(lambda item: item[0], optlist):
        STATUSES = map(lambda i: int(i.strip()), (filter(lambda item: item[0] == '--status', optlist)[0][1]).split(','))

    if '-v' in map(lambda item: item[0], optlist):
        VERBOSE = True


except Exception, ex:
    print ex
    exit(1)

def get_feeds(updated=None):
    db = PgSQLStore(tools)
    if updated is not None:
        res = db._get('job_boards', ['feed'], "feed_updated_at >= %s", [updated])
    else:
        res = db._get('job_boards', ['feed'], 'TRUE')
    if res:
        return map(lambda i: i['feed'], res)
    return None


def get_jobs_external_unique_id(employer_id):
    db = PgSQLStore(tools)
    where = ' '.join(['employer_id=%s', 'AND status IN (%s)' % ','.join(map(lambda i: str(i), STATUSES))])
    res = db._get('jobs', ['external_unique_id'], where, [employer_id])
    if res:
        return map(lambda i: i['external_unique_id'], res)
    return None

def get_feed_content(feed, local):
    global URL
    global FEED_DIR
    if local:
        f = '/'.join([FEED_DIR, feed])
        logger.info('... opening file "%s" ...' % f)
        try:
            fh = open(f, 'r')
        except Exception, ex:
            logger.error('... error while reading content of the file "%s"!' % ex)
            return ''
        else:
            content = fh.read()
            fh.close()
            return content

    else:
        url = '/'.join([URL, feed])
        r = requests.get(url, verify=False)
        if r.status_code == 200:
            logger.info('... content of the feed "%s" got succesfully' % feed)
            content = r.text
            return content
        else:
            logger.error('... error while fetching content of the feed "%s"!' % feed)
            return ''


logger.info('Employer id=%i' % EMPLOYER_ID)
logger.info('Job status in %s' % str(STATUSES))
logger.info('Fetching feeds...')
feeds = get_feeds(UPDATED)

logger.info('Fetching employer\'s active jobs...')
jobs_ids = get_jobs_external_unique_id(EMPLOYER_ID)
jobs = {}

for feed in feeds:
    logger.info('...start checking feed %s...' % feed)
    jobs[feed] = []
    content = get_feed_content(feed, LOCAL)
    for job_id in jobs_ids:
        if content.find(job_id) > 0:
            jobs[feed].append(job_id)
    logger.info('in feed "%s" was found %i jobs of employer id=%i' % (feed, len(jobs[feed]), EMPLOYER_ID))

logger.info('...all checking done! Resume:')

if len(jobs) == 0:
    logger.info('Any jobs of employer id=%i was not found' % EMPLOYER_ID)
for feed, list_ids in jobs.items():
    if len(list_ids) == 0:
        continue
    if VERBOSE:
        logger.info('in feed "%s" was found %i jobs of employer id=%i: %s' % (feed, len(list_ids), EMPLOYER_ID, str(jobs[feed])))
    else:
        logger.info('in feed "%s" was found %i jobs of employer id=%i' % (feed, len(list_ids), EMPLOYER_ID))

