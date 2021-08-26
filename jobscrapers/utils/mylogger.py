import logging


#enabling logging
logfile = 'log.txt'
logger = logging.getLogger('MYLOGGER')
ch = logging.FileHandler(logfile, 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch2.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(ch2)
logger.setLevel(1)
