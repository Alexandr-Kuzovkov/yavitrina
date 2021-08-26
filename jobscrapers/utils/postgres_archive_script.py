#!/usr/bin/env python

#Script for "archive_command" directive in postgresql.conf file
# archive_mode = on # (change requires restart)
#archive_command = '/home/postgres/postgres_archive_script.py %p %f'


import subprocess
import os
import sys

ARCHIVE_DIR = '/archive/postgres/archive'
RESTORE_DIR = '/archive/postgres/restore'
STORE_TIME = 10 #store time in minutes
TARGET_ACCOUNTS = [{'sshkey': '/home/postgres/.ssh/id_rsa', 'account': 'user1@192.168.0.115'}]

def copy_file(file, account):
    global RESTORE_DIR
    command = 'scp -i %s %s/%s %s:%s/%s' % (account['sshkey'], ARCHIVE_DIR, file, account['account'], RESTORE_DIR, file)
    os.system(command)
    command = ' '.join(['ssh -i', account['sshkey'], account['account'], 'sudo chown postgres:postgres %s/%s' % (RESTORE_DIR, file)])
    os.system(command)
    command = ' '.join(['ssh -i', account['sshkey'], account['account'], 'find', RESTORE_DIR, '-mmin', '+' + str(STORE_TIME),'-delete'])
    output = os.system(command)

if len(sys.argv) > 2:
    p = sys.argv[1]
    f = sys.argv[2]
    #remove old files
    command = ' '.join(['find', ARCHIVE_DIR, '-mmin', '+' + str(STORE_TIME), '-exec rm {} \;'])
    os.system(command)

    #copy WAL file
    command = 'test ! -f %s/%s && cp %s %s/%s' % (ARCHIVE_DIR, f, p, ARCHIVE_DIR, f)
    return_code = subprocess.call(command, shell=True)

    #copy WAL file to server
    for account in TARGET_ACCOUNTS:
        copy_file(f, account)

    #exit and return code
    sys.exit(return_code)
else:
    sys.exit(1)




