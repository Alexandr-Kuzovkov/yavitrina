#!/usr/bin/env python

#####################################################################################################################
#Script for pixel checking
###################################################################################################################

from optparse import OptionParser
import json
import time
import logging
import random
from terminaltables import SingleTable
from pprint import pprint
from datetime import datetime
import requests

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
    pixels = [
        "https://pixel.xtramile.io/p/0426f504-4454-11e7-9daa-2c56dc4b235a/c6df7.js",
        "https://pixel.xtramile.io/0426f504-4454-11e7-9daa-2c56dc4b235a/fff73.js?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&a=view",
        "https://pixel.xtramile.io/t/0426f504-4454-11e7-9daa-2c56dc4b235a/20d9d?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&s=&u=",
        "https://pixel.xtramile.io/p/0426f504-4454-11e7-9daa-2c56dc4b235a/87f29.js",
        "https://pixel.xtramile.io/0426f504-4454-11e7-9daa-2c56dc4b235a/fff73.js?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&a=apply_start",
        "https://pixel.xtramile.io/t/0426f504-4454-11e7-9daa-2c56dc4b235a/c9f4e?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&s=&u=",
        "https://pixel.xtramile.io/p/0426f504-4454-11e7-9daa-2c56dc4b235a/3d184.js",
        "https://pixel.xtramile.io/0426f504-4454-11e7-9daa-2c56dc4b235a/fff73.js?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&a=apply",
        "https://pixel.xtramile.io/t/0426f504-4454-11e7-9daa-2c56dc4b235a/45d0e?job_external_id=41578037-530d-43fc-be34-a68af0e1712f&candidate_id=8535&s=&u="
    ]

    while True:
        x = random.randint(1*60, 3*60)
        logger.info('Pause: %i sec' % x)
        time.sleep(x)
        data = [['uri', 'start', 'end', 'http_code']]
        logger.info('Get pixels...')
        for url in pixels:
            start = str(datetime.now())
            res = requests.get(url)
            code = res.status_code
            end = str(datetime.now())
            data.append([url.replace('https://pixel.xtramile.io', ''), start, end, code])
        table = SingleTable(data)
        logger.info('Result:')
        print(table.table)

if __name__ == '__main__':
    main()

