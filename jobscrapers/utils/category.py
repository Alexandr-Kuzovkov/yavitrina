#!/usr/bin/env python

#Script for replace jobs.attributes.category from string to integer

from mylogger import logger
from pg import PgSQLStore

LIMIT = 1000
update_count = 0

#db = {'dbname': 'xtramile_dev', 'dbhost': 'localhost', 'dbport': 15432, 'dbuser': 'xtramile', 'dbpass': 'xtramileDev'}
db = {'dbname': 'xtramile_prod', 'dbhost': 'tools.xtramile.tech', 'dbport': 5432, 'dbuser': 'postgres', 'dbpass': 'aDb91-UxT*1@l%tnopZ'}
pg = PgSQLStore(db)


res = pg._getraw("SELECT count(*) AS count FROM jobs WHERE attributes->>'category' NOTNULL", ['count'])
count_jobs = res[0]['count']

logger.info('count jobs=%i' % count_jobs)

for offset in range(0, count_jobs, LIMIT):
    logger.info('fetching jobs %i - %i' % (offset+1, min(offset + LIMIT, count_jobs)))
    jobs = pg._getraw("SELECT id, attributes FROM jobs WHERE attributes->>'category' NOTNULL ORDER BY id OFFSET %i LIMIT %i" % (offset, LIMIT), ['id', 'attributes'])
    pg.dbopen()
    for job in jobs:
        attributes = job['attributes']
        if type(attributes['category']) is not int:
            attributes['category'] = int(attributes['category'])
            attributes['jobType'] = int(attributes['jobType'])
            sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
            pg.cur.execute(sql, [pg._serialise_dict(attributes), job['id']])
            update_count += 1
    pg.conn.commit()
    logger.info('%i jobs updated' % update_count)
logger.info('Done')
logger.info('%i jobs was updated' % update_count)


