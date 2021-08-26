#!/usr/bin/env python

#####################################################################################################################
#Script for database connection checking
#for dev:
#./dbchk.py --host api.xtramile.tech --port 5432 --user xtramile --pass devPLK-012Yuzxcwdfejjk#\$6^dfdsf7 --db xtramile_dev
#for prod:
#./dbchk.py --host 104.46.32.67 --port 5432 --user postgres --pass Y+Bbtw3JRP7LllwWHWPdfECi --db xtramile_prod
###################################################################################################################

from optparse import OptionParser
import psycopg2
import json
import time
import logging
import random
from terminaltables import SingleTable
from pprint import pprint
from datetime import datetime

parser = OptionParser()
parser.add_option('--port', action='store', dest='port', help='Port')
parser.add_option('--host', action='store', dest='host', help='Host')
parser.add_option('--user', action='store', dest='user', help='User')
parser.add_option('--pass', action='store', dest='password', type='string', help='Password')
parser.add_option('--db', action='store', dest='db', type='string', help='Database name')

(options, args) = parser.parse_args()


#enabling logging
logfile = 'log.txt'
logger = logging.getLogger('MYLOGGER')
ch = logging.FileHandler(logfile, 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch2.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(ch2)
logger.setLevel(1)


def main():
    for opt in ['user', 'port', 'host', 'password', 'db']:
        if not getattr(options, opt):
            logger.error('option "%s" is requred' % opt)
            exit(1)
    port = int(options.port)
    host = options.host
    user = options.user
    password = options.password
    db = options.db
    conn = psycopg2.connect(dbname=db, user=user, password=password, host=host, port=port)
    cur = conn.cursor()
    start_time = datetime.now()
    queryes = [
        "SELECT id FROM jobs LIMIT 5",
        "SELECT id FROM employers LIMIT 5",
        "SELECT max(created_at) FROM stats"
    ]

    while True:
        for sql in queryes:
            x = random.randint(3*60, 10*60)
            logger.info('Connection: %s@%s:%i %s; start time: %s' % (user, host, port, db, start_time))
            logger.info('Pause: %i sec' % x)
            time.sleep(x)
            logger.info('Run query: "%s"' % sql)
            cur.execute(sql)
            data = cur.fetchall()
            table_data = [
                [],
                data
            ]
            table = SingleTable(table_data)
            logger.info('Result:')
            print(table.table)

if __name__ == '__main__':
    main()

