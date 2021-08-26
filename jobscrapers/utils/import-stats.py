#!/usr/bin/env python

#Script for imsert stat rows from csv file to table stats

from mylogger import logger
from pg import PgSQLStore

LIMIT = 1000
insert_count = 0
path = '/home/user1/Documents/JOB/csv/select___from_stats_where_id___12409211.tsv'

#db = {'dbname': 'xtramile_dev', 'dbhost': 'localhost', 'dbport': 15432, 'dbuser': 'xtramile', 'dbpass': 'xtramileDev'}
#db = {'dbname': 'xtramile_prod', 'dbhost': 'tools.xtramile.tech', 'dbport': 5432, 'dbuser': 'postgres', 'dbpass': 'aDb91-UxT*1@l%tnopZ'}
db = {'dbname': 'xtramile_prod', 'dbhost': 'platform-xtramile.postgres.database.azure.com', 'dbport': 5432, 'dbuser': 'xt_symfony@platform-xtramile', 'dbpass': '=XaD32r*hvpTeB9MLpu4AsD9s6URT5'}
pg = PgSQLStore(db)


def insertData(db, data):
    db.dbopen()
    sets = []
    table = 'stats'
    for key, val in data.items():
        sets.append(key + '= %s')
    sql = ' '.join(['INSERT INTO', table, '(', ','.join(data.keys()), ') VALUES (', ','.join(['%s' for i in data.keys()]), ');'])
    db.cur.execute(sql, data.values())

lines = map(lambda i: i.replace("\n", ''), open(path, 'r').readlines())
fld_lst = pg._get_fld_list('stats')
data = []
for line in lines:
    row = line.split('\t')
    if len(fld_lst) == len(row):
        d = {}
        for i in range(0, len(fld_lst)):
            d[fld_lst[i]] = row[i]
        data.append(d)
logger.info('%i rows data fetched' % len(data))
count = 1
sql = "select setval('stats_id_seq', (select max(id) from stats))"
pg.run(sql)

for row in data:
    sql = "SELECT id FROM stats WHERE user_ip=%s AND user_agent=%s AND employer_id=%s AND job_id=%s AND user_token=%s AND action=%s AND created_at=%s"
    res = pg._getraw(sql, ['id'], [row['user_ip'], row['user_agent'], int(row['employer_id']), row['job_id'], row['user_token'], int(row['action']), row['created_at']], False)
    if len(res) == 0:
        for key, val in row.items():
            if key in ['action', 'employer_id', 'duplicate_id', 'bot_id', 'candidate_id', 'click_status', 'job_board_id']:
                if len(val) == 0:
                    row[key] = None
                else:
                    row[key] = int(row[key])
            if key in ['cpc', 'cpc_origin', 'bot_cpc', 'bot_cpc_origin']:
                if len(val) == 0:
                    row[key] = None
                else:
                    row[key] = float(row[key])
            if key in ['is_bot', 'duplicate']:
                if len(val) == 0:
                    row[key] = False
                elif val == 'false':
                    row[key] = False
                elif val == 'true':
                    row[key] = True
        del row['id']
        insertData(pg, row)
        insert_count += 1
        logger.info('%i/%s row inserted' % (count, len(data)))
    else:
        logger.info('%i/%s row skipped' % (count, len(data)))
    count += 1

pg.conn.commit()
pg.dbclose()

logger.info('%i rows was inserted' % insert_count)
logger.info('Done')




