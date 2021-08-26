#!/usr/bin/env python

#Script to check swarm service balance

import os
from mylogger import logger
from pprint import pprint

LIMIT = 5
SERVICES_TO_UPDATE = []


def force_update(names):
    for name in names:
        command = 'sudo docker service update --force %s' % name
        logger.info('Command: "%s"' % command)
        output = os.popen(command).read()
        logger.info('Ouput: %s' % output)


logger.info('Checking swarm services...')
command = "docker node ps $(docker node ls -q) | grep Running  |  tr -s ' ' | cut -d ' ' -f 2,3,4,5 | grep platform"
output = os.popen(command).read()
rows = filter(lambda k: len(k) > 0, map(lambda i: filter(lambda j: len(j.strip()) > 0 and j.strip() != '\_' and j.strip() != 'Running', i.strip().split(' ')), output.split("\n")))
services = {}
duplicate = []
for row in rows:
    if row[0] not in duplicate:
        duplicate.append(row[0])
    else:
        continue
    task = row[0].split('.')[0]
    if task in services:
        services[task]['instance'] += 1
        if row[2] in services[task]['nodes']:
            services[task]['nodes'][row[2]] += 1
        else:
            services[task]['nodes'][row[2]] = 1
    else:
        services[task] = {'instance': 1, 'nodes': {row[2]: 1}}

print services

for service_name in services.keys():
    if services[service_name]['instance'] > 1 and len(services[service_name]['nodes']) == 1:
        SERVICES_TO_UPDATE.append(service_name)

if len(SERVICES_TO_UPDATE) > LIMIT:
    force_update(SERVICES_TO_UPDATE)

logger.info('Done')
