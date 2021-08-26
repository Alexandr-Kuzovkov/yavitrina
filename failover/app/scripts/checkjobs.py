#!/usr/bin/env python

import os
import time
import json
import re
import time
from mylogger import logger2 as logger
from notifier import slack
from notifier import email
import cronjobs


def send_slack_notify(message):
    slack(message, '#incident-dev')

def check_jobs():
    logger.info('START CHECK JOBS')
    jobs = cronjobs.get_died_jobs_for_notify()
    for job in jobs:
        logger.info('Died job "{slug}" was found for notification'.format(slug=job['slug']))
        message = '''Cron job with slug="{slug} and Description="{description}" was not send ping. Please pay attention on it'''.format(slug=job['slug'], description=job['description'])
        send_slack_notify(message)
        job['last_notification'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
        job['start_time'] = None
        cronjobs.update_job(job['slug'], job)
    logger.info('STOP CHECK JOBS')

def check_errors():
    logger.info('START CHECK ERRORS')
    errors = cronjobs.get_errors_for_notify()
    for error in errors:
        logger.info('Error was occured when command "{desc}" with slug "{slug}" executed. ERROR: {error}'.format(slug=error['slug'], desc=error['job_description'], error=error['error_message']))
        message = '''Error was occured when command "{desc}" with slug "{slug}" executed. ERROR: {error}'''.format(slug=error['slug'], desc=error['job_description'], error=error['error_message'])
        send_slack_notify(message)
        cronjobs.set_notification_flag(error)
    logger.info('STOP CHECK ERRORS')

def set_jobs_start_time():
    now = time.localtime()
    jobs = cronjobs.get_job_list()
    for job in jobs:
        if is_time_match(job['m'], job['h'], job['dom'], job['mon'], job['dow'], now):
            job['start_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))
            cronjobs.update_job(job['slug'], job)

def is_time_match(m, h, dom, mon, dow, now):
    "time.struct_time(tm_year=2019, tm_mon=2, tm_mday=27, tm_hour=18, tm_min=8, tm_sec=44, tm_wday=2, tm_yday=58, tm_isdst=0)"
    if item_match(m, now, 4) and item_match(h, now, 3) and item_match(dom, now, 2) and item_match(mon, now, 1) and item_match(dow, now, 6):
        return True
    return False

def item_match(item, now, index):
    if item == '*':
        return True
    elif item.isdigit() and int(item.strip()) == now[index]:
        return True
    elif '*/' in item and now[index] % int(item.strip().split('*/')[1]) == 0:
        return True
    return False


def main():
    set_jobs_start_time()
    check_jobs()
    check_errors()

if __name__ == "__main__":
    main()












