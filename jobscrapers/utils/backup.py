#!/usr/bin/env python

import os
import time
from os.path import expanduser
import logging
from dbacc import *

env = 'dev'
dump_path = 'projects/backup-restore/dumps'
if env == 'prod':
    rsa_key_file = '/home/user1/.ssh/rsa-key-prod'
elif env == 'dev':
    rsa_key_file = '/home/user1/.ssh/rsa-key-dev'
if env == 'prod':
    source_acc = 'ubuntu@213.32.30.200'
elif env == 'dev':
    source_acc = 'ubuntu@167.114.250.156'

archive_path = 'projects/backup-restore/archive'

#enabling logging
logger = logging.getLogger('BACKUP')
ch = logging.FileHandler('log.txt', 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch2.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(ch2)
logger.setLevel(1)
home = expanduser("~")

#list folders
fb = {'folder': '/home/ubuntu/docker/facebook-api', 'exclude': ['src/app/logs/*', 'src/app/cache/*']}
backend = {'folder': '/home/ubuntu/docker/xtramile-backend', 'exclude': ['src/var/logs/*', 'src/var/cache/*']}
coreapi = {'folder': '/home/ubuntu/docker/xtramile-CoreAPI', 'exclude': []}
coreapi_data = {'folder': '/home/ubuntu/docker/xtramile-CoreAPI-data', 'exclude': []}
proxy = {'folder': '/home/ubuntu/docker/proxy', 'exclude': []}
ra = {'folder': '/home/ubuntu/docker/xtramile-recruiter_webapp', 'exclude': ['src/tmp/*']}
drupal = {'folder': '/home/ubuntu/docker/mainsite', 'exclude': []}
drupal_db = {'folder': '/home/ubuntu/docker/xtramile-db', 'exclude': []}
bo = {'folder': '/home/ubuntu/docker/xtramile-backoffice', 'exclude': ['src/tmp/*']}
lp = {'folder': '/home/ubuntu/docker/xtramile-job-landing', 'exclude': []}
coreapi_db = {'folder': '/home/ubuntu/docker/xtramile-CoreAPI-db', 'exclude': []}
coreapi_pg_db = {'folder': '/home/ubuntu/docker/xtramile-CoreAPI-postgres-db', 'exclude': []}
coreapi_phppgadmin = {'folder': '/home/ubuntu/docker/xtramile-phppgadmin', 'exclude': []}
dns = {'folder': '/home/ubuntu/docker/dns', 'exclude': []}
elasticsearch = {'folder': '/home/ubuntu/docker/xtramile-elasticsearch', 'exclude': []}
xtramile_db = {'folder': '/home/ubuntu/docker/xtramile-db', 'exclude': []}
cv_ranker = {'folder': '/home/ubuntu/docker/cv-ranker', 'exclude': []}
utils = {'folder': '/home/ubuntu/docker/utils', 'exclude': []}
devops = {'folder': '/home/ubuntu/devops', 'exclude': []}

#folder_list = [fb, backend, coreapi, coreapi_data, proxy, ra, drupal, drupal_db, bo, lp, coreapi_db, coreapi_pg_db, coreapi_phppgadmin, dns, elasticsearch, xtramile_db, cv_ranker, utils, devops]
folder_list = [devops]


def backup_db(db):
    passfile = '/'.join([home, '.pgpass'])
    f = open(passfile, 'w')
    f.write('%s:%s:%s:%s:%s' % (db['dbhost'], str(db['dbport']), db['dbname'], db['dbuser'], db['dbpass']))
    f.close()
    logger.info('save password to "%s"' % passfile)
    os.chmod(passfile, 0600)
    date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time())))
    dumpfile = '/'.join([home, dump_path, '.'.join([db['dbname'], date, 'dump'])])
    command = ' '.join(['pg_dump -Fc -U', db['dbuser'], '-h', db['dbhost'], '-p', str(db['dbport']), db['dbname'], '>', dumpfile])
    logger.info('start dumping database %s to %s' % (db['dbname'], dumpfile))
    os.system(command)
    logger.info('dumping done')
    os.remove(passfile)
    logger.info('remove file "%s"' % passfile)

def backup_folder(folder):
    exclude = ' '.join(map(lambda i: '='.join(['--exclude', '/'.join([folder['folder'], i])]), folder['exclude']))
    archive_name = folder['folder'].split('/').pop() + '.tgz'
    logger.info('start backup folder "%s" to "%s"' % (folder['folder'], '/'.join([home, archive_path, archive_name])))
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo tar cz', exclude, folder['folder'], '>', '/'.join([home, archive_path, archive_name])])
    os.system(command)
    logger.info('backup done')

def backup_mainsite_db():
    date = time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(int(time.time())))
    dumpfile = '/'.join([home, dump_path, '.'.join(['mainsite', date, 'sql.gz'])])
    logger.info('start dumping database %s to %s' % ('centur_drup1', dumpfile))
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, "sudo docker exec xtramile-drupaldb mysqldump -u centur_drup1 --password='M*X9@P8Prp3or\&igzI[98]\(3' centur_drup1 | gzip", '>', dumpfile])
    os.system(command)
    logger.info('dumping done')

def backup_folders(folder_list):
    for folder in folder_list:
        backup_folder(folder)

#backup_db(dev)
#backup_db(coreapi_dev)

#backup_mainsite_db()

#backup_folder(fb)
backup_folders(folder_list)



