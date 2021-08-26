#!/usr/bin/env python

from mylogger import logger
from azurestorage import AzureStorage
import sys


if len(sys.argv) > 1:
    full_path_to_file = sys.argv[1]
else:
    print 'Usage: %s <path_to_file>' % sys.argv[0]

azs = AzureStorage('xtramilefeeds', 'jT6jTaU2023I4E4zIRR8s8rncBBkJWR/q7UdaPzoDLOABzXEAlnwxUGypS7AEicLrsSZ4J4tFp/mFxyHkLyklQ==')
logger.info('...push to Azure Storage')
print full_path_to_file
azure_url = azs.push_file(full_path_to_file)
logger.info('...done, URL: %s' % azure_url)

