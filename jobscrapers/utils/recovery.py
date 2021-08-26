#!/usr/bin/env python

import os
import time
import sys
from os.path import expanduser
import logging
from dbacc import *

dump_path = 'projects/backup-restore/dumps'
archive_path = 'projects/backup-restore/archive'

#enabling logging
logger = logging.getLogger('RECOVERY')
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

target = {'dbname': '', 'dbhost': 'localhost', 'dbport': 5432, 'dbuser': 'postgres', 'dbpass': 'rootpostgres'}
target_mysql = {'dbname': 'centur_drup1', 'dbhost': 'localhost', 'dbport': 3306, 'dbuser': 'root', 'dbpass': 'rootroot'}


def recovery_db(dumpname):
    passfile = '/'.join([home, '.pgpass'])
    f = open(passfile, 'w')
    target['dbname'] = 'copy_'+dumpname.split('.')[0]
    f.write('%s:%s:%s:%s:%s' % (target['dbhost'], str(target['dbport']), '*', target['dbuser'], target['dbpass']))
    f.close()
    logger.info('save password to "%s"' % passfile)
    os.chmod(passfile, 0600)
    logger.info('dropping database "%s"' % (target['dbname'],))
    command = ' '.join(['dropdb  -U', target['dbuser'], '-h', target['dbhost'], target['dbname']])
    os.system(command)
    logger.info('database "%s" dropped' % (target['dbname'],))
    command = ' '.join(['createdb  -U', target['dbuser'], '-O', target['dbuser'], '-h', target['dbhost'], '-T template0', target['dbname']])
    logger.info('creating database "%s"' % (target['dbname'],))
    os.system(command)
    logger.info('creating database done')
    logger.info('start recovery dump "%s" to database "%s"' % (dumpname, target['dbname']))
    command = ' '.join(['pg_restore -U', target['dbuser'], '-d', target['dbname'], '-h', target['dbhost'], '/'.join([home, dump_path, dumpname])])
    os.system(command)
    logger.info('recovery database done')
    os.remove(passfile)
    logger.info('remove file "%s"' % passfile)

def recovery_mainsite_db():
    dumps = filter(lambda i: i.split('.')[0] == 'mainsite', os.listdir('/'.join([home, dump_path])))
    dumpname = '/'.join([home, dump_path, dumps[0]])
    logger.info('start recovery dump "%s" to database "%s"' % (dumpname, target_mysql['dbname']))
    command = ' '.join(['gunzip < ', dumpname, ' | mysql -u' + target_mysql['dbuser'], '-p' + target_mysql['dbpass'], target_mysql['dbname']])
    #print command
    #exit()
    os.system(command)
    logger.info('recovery database done')

def extract_archive(archive):
    logger.info('start extracting archive "%s"' % archive)
    command = ' '.join(['cd', '/', '&&', 'tar xfz', archive])
    os.system(command)
    logger.info('extracting archive done')

def recovery_folders():
    archives = os.listdir('/'.join([home, archive_path]))
    for archive in archives:
        archive = '/'.join([home, archive_path, archive])
        extract_archive(archive)

def recovery_databases():
    dumps = os.listdir('/'.join([home, dump_path]))
    for dump in dumps:
        recovery_db(dump)

#recovery_mainsite_db()
#recovery_databases()
recovery_folders()




