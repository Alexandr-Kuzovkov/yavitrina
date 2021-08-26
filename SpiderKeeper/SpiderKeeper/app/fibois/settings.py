from os import path
from configparser import ConfigParser
import os

CONFIG_FILE = '/home/root/config.ini'
try:
    if path.isfile(CONFIG_FILE):
        config = ConfigParser()
        with open(CONFIG_FILE) as config_file:
            config.read_file(config_file)
            SCRAPING_ES_URL = config['ELASTICSEARCH']['SCRAPING_URL']
            SCRAPING_ES_INDEX = config['ELASTICSEARCH']['SCRAPING_INDEX']

            PARSING_ES_URL = config['ELASTICSEARCH']['PARSING_URL']
            PARSING_ES_INDEX = config['ELASTICSEARCH']['PARSING_INDEX']

            PAGE_RESULTS_HTML_INDEX = config['ELASTICSEARCH']['PAGE_RESULTS_HTML_INDEX']
            JOB_DETAILS_HTML_INDEX = config['ELASTICSEARCH']['JOB_DETAILS_HTML_INDEX']
    else:
        raise Exception('Config file not found')
except Exception as ex:
    raise Exception('ERROR parse config file')



