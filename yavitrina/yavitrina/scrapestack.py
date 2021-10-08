import scrapy
import copy
from urllib import urlencode
from pprint import pprint
import logging

class ScrapestackRequest(scrapy.Request):

    url_origin = ''
    access_key = ''
    retry = {}
    max_attempts = 30
    retry_enabled = True

    def __init__(self, url, callback, method='GET', meta=None, access_key='', options=None, **kwargs):
        meta = copy.deepcopy(meta) or {}
        self.url_origin = url
        self.retry['callback'] = callback
        self.retry['method'] = method
        self.retry['meta'] = meta
        self.retry['options'] = options
        if 'attempt' not in self.retry:
            self.retry['attempt'] = 1
        else:
            self.retry['attempt'] += 1
        params = {'access_key': access_key, 'url': url}
        if options is not None and type(options) is dict:
            for key, val in options.items():
                params[key] = val
        url = 'http://api.scrapestack.com/scrape?' + urlencode(params)
        super(ScrapestackRequest, self).__init__(url, callback, method, meta=meta, **kwargs)

    def retry_request(self):
        if not self.retry_enabled:
            return None
        if self.retry['attempt'] < self.max_attempts:
            return ScrapestackRequest(self.url_origin, callback=self.retry['callback'], access_key=self.access_key,
                                          method=self.retry['method'], meta=self.retry['meta'], options=self.retry['options'])
        return None





