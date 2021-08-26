#!/usr/bin/env python

#Script to clean disk on swarm nodes

import os
from mylogger import logger
from pprint import pprint

DEVICE = '/dev/sda5'
LIMIT = 90


def do_clean():
    commands = [
        'sudo rm /var/log/syslog.1',
        'sudo rm /var/log/auth.log.1',
        'sudo rm /var/log/kern.log.1',
        'sudo rm /home/ubuntu/.lsyncd/lsyncd.log',
        #sudo find /var/www/facebook-api/logs/ -maxdepth 1 -type f -delete
        'sudo docker container prune -f',
        'sudo docker image prune -a -f'
    ]
    for command in commands:
        logger.info('Command: "%s"' % command)
        output = os.popen(command).read()
        logger.info('Ouput: %s' % output)


logger.info('Checking disk usage...')
command = "df -h | grep %s" % DEVICE
output = os.popen(command).read()
usage = 0
try:
    usage = int(filter(lambda i: len(i.strip()) > 0, output.split(' '))[4][:-1])
except Exception, ex:
    logger.error('Can\'t get usage value: %s' % ex.message)

logger.info('...disk usage %i%%, limit: %i%%' % (usage, LIMIT))

if usage >= LIMIT:
    logger.info('...run do_clean()')
    do_clean()
logger.info('Done')
