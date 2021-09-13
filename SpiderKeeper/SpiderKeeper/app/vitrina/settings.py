from os import path
from configparser import ConfigParser
import os

CONFIG_FILE = '/home/root/config.ini'

def load_config(config_file=CONFIG_FILE):
    try:
        if path.isfile(config_file):
            config = ConfigParser()
            with open(config_file) as f:
                config.read_file(f)
            #raise ('Fake error!!!!')
            return config
        else:
            raise Exception('Config file not found')
    except Exception as ex:
        raise Exception('ERROR parse config file')





