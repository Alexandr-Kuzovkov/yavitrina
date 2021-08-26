#!/usr/bin/env python

#Script to verify that jobs marked as expired in the database aren't present in their feeds.
#Take expired jobs from the database and check their availability in all feeds by "external_id"
#If such jobs will be found, warning message put into out

from dbacc import *
from pg import PgSQLStore
from mylogger import logger
import requests

db = PgSQLStore(tools)

#get jobleads feeds
logger.info('Fetching employer feeds...')
employers = db._get('employers', ['feed_url'], 'id=%s', [101])
feeds = employers[0]['feed_url'].split(',')
logger.info('... %i feeds was fetched' % len(feeds))

#get expired jobleads jobs
logger.info('Fetching employer expired jobs...')
jobs = db._get('jobs', ['id', 'external_id'], 'employer_id=%s AND status=%s', [101, 2])
logger.info('... %i expired jobs was fetched' % len(jobs))

logger.info('...start checking...')
for feed in feeds:
    logger.info('... Checking by feed %s ...' % feed)
    r = requests.get(feed)
    if r.status_code == 200:
        logger.info('... content of the feed "%s" got succesfully' % feed)
        content = r.text
        for job in jobs:
            if content.find('jobId=%s' % job['external_id']) > 0:
                logger.warning(' job with id=%s are exists in feed "%s" but expired in database!' % (job['id'], feed))
    else:
        logger.error('... error while fetching content of the feed "%s"!' % feed)
logger.info('...checking done!')




