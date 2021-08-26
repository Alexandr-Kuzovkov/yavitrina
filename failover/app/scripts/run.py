#!/usr/bin/env python

import os
import time
from os.path import expanduser
import stat
import json
import re
import time
from myconfig import config as config, config_file
from myconfig import options as options
from mylogger import logger as logger
from notifier import slack
from notifier import email
import myserver

home = expanduser("~")
#data_directory = '/var/lib/postgresql/9.6/main' #for 9.6
data_directory = '/var/lib/postgresql/10/data'  # for 10.0
#bin_directory = '/usr/lib/postgresql/9.6/bin' # for 9.6
bin_directory = '/usr/lib/postgresql/10/bin' # for 10.0

def get_nodes():
    nodes = {'masters': [], 'slaves': [], 'fails': []}
    config.read(config_file)
    for key in config['nodes']:
        node = json.loads(config['nodes'][key])
        command = 'nc %s %i' % (node['ip'], node['port'])
        try:
            response = os.popen(command).read()
        except Exception as ex:
            logger.error(ex.message)
        p = re.compile('master is running')
        res = p.search(response)
        if res and res.group() == 'master is running':
            node['os'] = get_os_info(node)
            nodes['masters'].append(node)
        else:
            p = re.compile('slave is running')
            res = p.search(response)
            if res and res.group() == 'slave is running':
                node['os'] = get_os_info(node)
                nodes['slaves'].append(node)
            else:
                node['os'] = get_os_info(node)
                nodes['fails'].append(node)
    return nodes

def get_node_by_ip(ip):
    config.read(config_file)
    for key in config['nodes']:
        node = json.loads(config['nodes'][key])
        if node['ip'] == ip:
            node['os'] = get_os_info(node)
            return node
    return None

def send_notify(message):
    slack(message)
    email('FAILOVER NOTIFY', message)

def run_command(command):
    logger.info('Command: "%s"' % command)
    output = os.popen(command).read()
    logger.info('Ouput: %s' % output)
    return output

def select_slave_to_promote(nodes):
    if len(nodes['slaves']) > 0:
        return nodes['slaves'][0]
    return None

def get_os_info(node):
    if os.path.isfile('/root/.ssh/config'):
        os.chmod('/root/.ssh/config', stat.S_IRUSR | stat.S_IWUSR)
        stat_info = os.stat('/root/.ssh/config')
        uid = stat_info.st_uid
        gid = stat_info.st_gid
        os.chown('/root/.ssh/config', 0, 0)

    logger.info('Look OS version')
    rsa_key_file = '/root/.ssh/' + node['sshkey']
    source_acc = '@'.join([node['user'], node['ip']])
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'lsb_release -a'])
    output = run_command(command)
    data = {}
    try:
        ls = list(map(lambda row: {row.split(':')[0].strip(): row.split(':')[1]}, list(filter(lambda row: len(row.strip()) > 0 and ':' in row, output.split("\n")))))
        for row in ls:
            for key, val in row.items():
                data[key.strip()] = val.strip()
    except Exception as ex:
        logger.error('Get OS information fail!')
        data = None
    if os.path.isfile('/root/.ssh/config'):
        os.chown('/root/.ssh/config', uid, gid)
    return data

def promote_node_to_master(node):
    if os.path.isfile('/root/.ssh/config'):
        os.chmod('/root/.ssh/config', stat.S_IRUSR | stat.S_IWUSR)
        stat_info = os.stat('/root/.ssh/config')
        uid = stat_info.st_uid
        gid = stat_info.st_gid
        os.chown('/root/.ssh/config', 0, 0)

    logger.info('Starting promoting slave node "%s" to master' % node['ip'])
    rsa_key_file = '/root/.ssh/' + node['sshkey']
    source_acc = '@'.join([node['user'], node['ip']])
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo -u postgres %s/pg_ctl promote -D %s/' % (bin_directory, data_directory)])
    run_command(command)

    if os.path.isfile('/root/.ssh/config'):
        os.chown('/root/.ssh/config', uid, gid)
    logger.info('Promoting "%s" done' % node['ip'])

def bind_slave_to_master(slave, master):
    if slave is None or master is None:
        logger.warning('Slave or Master is None! Binding fail!')
        return
    if os.path.isfile('/root/.ssh/config'):
        os.chmod('/root/.ssh/config', stat.S_IRUSR | stat.S_IWUSR)
        stat_info = os.stat('/root/.ssh/config')
        uid = stat_info.st_uid
        gid = stat_info.st_gid
        os.chown('/root/.ssh/config', 0, 0)

    logger.info('Starting binding slave "%s" to new master "%s"' % (slave['ip'], master['ip']))
    rsa_key_file = '/root/.ssh/' + slave['sshkey']
    source_acc = '@'.join([slave['user'], slave['ip']])
    #stop postgres server
    if slave['os'] and slave['os']['Distributor ID'] == 'Ubuntu' and float(slave['os']['Release']) >= 16:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo systemctl stop postgresql'])
    else:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo service postgresql stop'])
    run_command(command)

    #run rewind
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, '"sudo -u postgres %s/pg_rewind -D %s/ --source-server=\\"host=%s port=%i user=%s password=%s\\""' % (bin_directory, data_directory, master['ip'], master['dbport'], master['dbuser'], master['dbpass'])])
    run_command(command)

    #rename recovery.done to recovery.conf
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo mv %s/recovery.done %s/recovery.conf' % (data_directory, data_directory)])
    run_command(command)

    #edit recovery.conf
    lines = ["standby_mode = 'on'\n"]
    lines.append("primary_conninfo = 'user=%s password=%s host=%s port=%i sslmode=prefer sslcompression=1 krbsrvname=postgres target_session_attrs=any'\n" % (master['dbuser'], master['dbpass'], master['ip'], master['dbport']))
    lines.append("recovery_target_timeline = 'latest'\n")
    lines.append("restore_command = 'cp /archive/postgres/restore/%f %p'\n")
    with open('/tmp/recovery.conf', 'w') as f:
        f.writelines(lines)
    f.close()
    command = ' '.join(['scp -i', rsa_key_file, '/tmp/recovery.conf', '/'.join([source_acc+':', 'home', slave['user'], 'recovery.conf'])])
    run_command(command)
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo cp ', ''.join(['/home/', slave['user'], '/recovery.conf']), '%s/recovery.conf' % data_directory])
    run_command(command)
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo chown postgres:postgres %s/recovery.conf' % data_directory])
    run_command(command)

    #start postgres server
    if slave['os'] and slave['os']['Distributor ID'] == 'Ubuntu' and float(slave['os']['Release']) >= 16:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo systemctl start postgresql'])
    else:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo service postgresql start'])
    run_command(command)

    if os.path.isfile('/root/.ssh/config'):
        os.chown('/root/.ssh/config', uid, gid)
    logger.info('Binding slave "%s" to new master "%s" done' % (slave['ip'], master['ip']))

def basebackup_slave_from_master(slave, master):
    if slave is None or master is None:
        logger.warning('Slave or Master is None! Binding fail!')
        return
    if os.path.isfile('/root/.ssh/config'):
        os.chmod('/root/.ssh/config', stat.S_IRUSR | stat.S_IWUSR)
        stat_info = os.stat('/root/.ssh/config')
        uid = stat_info.st_uid
        gid = stat_info.st_gid
        os.chown('/root/.ssh/config', 0, 0)

    logger.info('Starting basebackup slave "%s" from master "%s"' % (slave['ip'], master['ip']))
    rsa_key_file = '/root/.ssh/' + slave['sshkey']
    source_acc = '@'.join([slave['user'], slave['ip']])
    #stop postgres server
    if slave['os'] and slave['os']['Distributor ID'] == 'Ubuntu' and float(slave['os']['Release']) >= 16:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo systemctl stop postgresql'])
    else:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo service postgresql stop'])
    run_command(command)

    #remove old data
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo sh -c \\"rm -Rf %s/*\\"' % data_directory])
    run_command(command)

    #run pg_basebackup
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo pg_basebackup -P -R -X stream -c fast -h %s -U %s -D %s' % (master['ip'], master['dbuser'], data_directory)])
    run_command(command)

    #rename recovery.done to recovery.conf
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo mv %s/recovery.done %s/recovery.conf' % (data_directory, data_directory)])
    run_command(command)

    #edit recovery.conf
    lines = ["standby_mode = 'on'\n"]
    lines.append("primary_conninfo = 'user=%s password=%s host=%s port=%i sslmode=prefer sslcompression=1 krbsrvname=postgres target_session_attrs=any'\n" % (master['dbuser'], master['dbpass'], master['ip'], master['dbport']))
    lines.append("recovery_target_timeline = 'latest'\n")
    lines.append("restore_command = 'cp /archive/postgres/restore/%f %p'\n")
    with open('/tmp/recovery.conf', 'w') as f:
        f.writelines(lines)
    f.close()
    command = ' '.join(['scp -i', rsa_key_file, '/tmp/recovery.conf', '/'.join([source_acc+':', 'home', slave['user'], 'recovery.conf'])])
    run_command(command)
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo cp ', ''.join(['/home/', slave['user'], '/recovery.conf']), '%s/recovery.conf' % data_directory])
    run_command(command)
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo chown postgres:postgres %s/recovery.conf' % data_directory])
    run_command(command)
    command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo chown -R postgres:postgres /var/lib/postgresql'])
    run_command(command)

    #start postgres server
    if slave['os'] and slave['os']['Distributor ID'] == 'Ubuntu' and float(slave['os']['Release']) >= 16:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo systemctl start postgresql'])
    else:
        command = ' '.join(['ssh -i', rsa_key_file, source_acc, 'sudo service postgresql start'])
    run_command(command)

    if os.path.isfile('/root/.ssh/config'):
        os.chown('/root/.ssh/config', uid, gid)
    logger.info('Basebackup slave "%s" from master "%s" done' % (slave['ip'], master['ip']))

def handle_master_failover(nodes):
    if len(nodes['fails']) > 0:
        ip = nodes['fails'][0]['ip']
    else:
        ip = 'unknown'
    send_notify('You master was died! It IP may be "%s"' % ip)
    node_for_promote = select_slave_to_promote(nodes)
    logger.info('Selected slave to promote: %s' % str(node_for_promote['ip']))
    if node_for_promote is None:
        logger.warning('You have not slaves to promote!')
        send_notify('You have not slaves to promote!')
        return
    promote_node_to_master(node_for_promote)
    time.sleep(10)
    nodes = get_nodes()
    if len(nodes['masters']) < 1:
        logger.warning('Fail promote slave to master for "%s"' % node_for_promote['ip'])
        send_notify('Fail promote slave to master for "%s"' % node_for_promote['ip'])
        #TODO decrement priority
    else:
        logger.info('New master "%s" was up' % node_for_promote['ip'])
        send_notify('New master "%s" was up' % node_for_promote['ip'])
        for node in nodes['slaves']:
            bind_slave_to_master(node, nodes['masters'][0])

def handle_bind_slave_option(nodes):
    logger.info('Option set node "%s" as slave was found' % options['slave'])
    slave = get_node_by_ip(options['slave'])
    if slave is None:
        logger.warning('Slave with ip="%s" was not found!' % options['slave'])
        return
    print(options['master'])
    if 'master' in options:
        if len(nodes['masters']) >= 1:
            for node in nodes['masters']:
                if node['ip'] == options['master']:
                    bind_slave_to_master(slave, get_node_by_ip(options['master']))
                    return
        logger.warning('Master not found!')
        return
    master = nodes['masters'][0]
    bind_slave_to_master(slave, master)

def handle_promote_option(nodes):
    for node in nodes['slaves']:
        if node['ip'] == options['promote']:
            promote_node_to_master(get_node_by_ip(options['promote']))
            exit()
    logger.warning('Slave for promote not found!')

def handle_basebackup_option(nodes):
    logger.info('Option set node "%s" as slave with backup was found' % options['basebackup'])
    slave = get_node_by_ip(options['basebackup'])
    if slave is None:
        logger.warning('Slave with ip="%s" was not found!' % options['slave'])
        exit()

    if 'master' in options:
        if len(nodes['masters']) >= 1:
            for node in nodes['masters']:
                if node['ip'] == options['master']:
                    basebackup_slave_from_master(slave, get_node_by_ip(options['master']))
                    exit()
        logger.warning('Master not found!')
        exit()
    master = nodes['masters'][0]
    basebackup_slave_from_master(slave, master)

def main():
    logger.info('START FAILOVER')
    nodes = get_nodes()
    logger.info('Nodes found: ')
    logger.info('   Master: ')
    for node in nodes['masters']:
        logger.info('      %s - OS: %s' % (str(node['ip']), str(node['os'])))
    logger.info('   Slave: ')
    for node in nodes['slaves']:
        logger.info('      %s - OS: %s' % (str(node['ip']), str(node['os'])))
    logger.info('   Fail: ')
    for node in nodes['fails']:
        logger.info('      %s - OS: %s' % (str(node['ip']), str(node['os'])))

    if 'slave' in options: #handle option --bind-slave bind slave to master
        handle_bind_slave_option(nodes)
    elif 'promote' in options: #handle option --promote-slave
        handle_promote_option(nodes)
    elif 'basebackup' in options: #handle option --basebackup
        handle_basebackup_option(nodes)
    else:
        #if master was died
        if len(nodes['masters']) < 1:
            logger.warning('Master fail was found!')
            handle_master_failover(nodes)
        #send message if slaves left less than defined
        min_number_slaves = int(config.get('parameters', 'min_number_slaves'))
        if len(nodes['slaves']) < min_number_slaves:
            logger.warning('You have less than %i slaves in your cluster!' % min_number_slaves)
            send_notify('You have less than %i slaves in your cluster!' % min_number_slaves)

    logger.info('STOP FAILOVER')

if __name__ == "__main__":
    main()












