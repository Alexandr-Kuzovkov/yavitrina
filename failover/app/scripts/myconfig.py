import configparser
import getopt
import sys

DEFAULT_CONFIG_FILE = '/app/config.ini'
config = None
config_file = None
options = {}

try:
    optlist, args = getopt.getopt(sys.argv[1:], 'c:b:m:p:', ['config-file=', 'bind-slave=', 'master=', 'promote-slave=', 'basebackup='])
    if '-c' in map(lambda item: item[0], optlist):
        config_file = list(filter(lambda item: item[0] == '-c', optlist))[0][1]
    elif '--config-file' in map(lambda item: item[0], optlist):
        config_file = list(filter(lambda item: item[0] == '--config-file', optlist))[0][1]
    else:
        config_file = DEFAULT_CONFIG_FILE
    if '-b' in map(lambda item: item[0], optlist):
        options['slave'] = list(filter(lambda item: item[0] == '-b', optlist))[0][1]
    elif '--bind-slave' in map(lambda item: item[0], optlist):
        options['slave'] = list(filter(lambda item: item[0] == '--bind-slave', optlist))[0][1]
    else:
        pass
    if '-m' in map(lambda item: item[0], optlist):
        options['master'] = list(filter(lambda item: item[0] == '-m', optlist))[0][1]
    elif '--master' in map(lambda item: item[0], optlist):
        options['master'] = list(filter(lambda item: item[0] == '--master', optlist))[0][1]
    else:
        pass
    if '-p' in map(lambda item: item[0], optlist):
        options['promote'] = list(filter(lambda item: item[0] == '-p', optlist))[0][1]
    elif '--promote-slave' in map(lambda item: item[0], optlist):
        options['promote'] = list(filter(lambda item: item[0] == '--promote-slave', optlist))[0][1]
    else:
        pass
    if '--basebackup' in map(lambda item: item[0], optlist):
        options['basebackup'] = list(filter(lambda item: item[0] == '--basebackup', optlist))[0][1]

    config = configparser.ConfigParser()
    config.read(config_file)

except Exception as ex:
    print(ex)
    exit()


def save_config():
    with open(config_file, 'w') as configfile:
        config.write(configfile)
