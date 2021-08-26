#!/usr/bin/env python
#coding=utf-8

import logging

logger = logging.getLogger(__name__)
ch = logging.FileHandler('log.txt', 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch3 = logging.FileHandler('status.log', 'a', 'utf8')
ch3.setLevel(logging.CRITICAL)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch3.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(ch2)
logger.addHandler(ch3)
logger.setLevel(1)
logger.debug('thit is debug message')
logger.info('thit is info message')
logger.warning('this is warning message')
logger.critical('this is critical message')


