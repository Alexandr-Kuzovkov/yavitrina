#!/usr/bin/env python

import urllib2
import urllib
import json
from mylogger import logger
from pprint import pprint


def slack(channel, message):
    slack_channel = channel
    SLACK_WEB_HOOK_POINT = 'https://hooks.slack.com/services/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'
    SLACK_WEB_HOOK_POINT = 'https://hooks.slack.com/services/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'
    data = urllib.urlencode({'payload': json.dumps(
        {'channel': slack_channel, 'text': message, 'username': 'Xtramile-chat-bot', 'icon_emoji': ':ghost:'})})
    req = urllib2.Request(url=SLACK_WEB_HOOK_POINT)
    req.add_data(data)
    try:
        response = urllib2.urlopen(req)
        pprint(response)
    except Exception, ex:
        logger.error('Send slack notify fail!: {err}'.format(err=ex))
    else:
        logger.info('Send slack notify successfully')


slack('#incident', 'webhook test message')
