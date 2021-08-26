import logging


#enabling logging
logger = logging.getLogger('DB FAILOVER')
ch = logging.FileHandler('/app/logs/log.txt', 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch2.setFormatter(formatter)
logger.addHandler(ch)
logger.addHandler(ch2)
logger.setLevel(1)

#enabling logging
logger2 = logging.getLogger('CRON JOB FAILOVER')
ch = logging.FileHandler('/app/logs/cronlog.txt', 'a', 'utf8')
ch.setLevel(logging.DEBUG)
ch2 = logging.StreamHandler()
ch2.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
ch2.setFormatter(formatter)
logger2.addHandler(ch)
logger2.addHandler(ch2)
logger2.setLevel(1)