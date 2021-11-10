import scrapy
import copy
from urllib import urlencode
from pprint import pprint
import logging

class SelenuimRequest(scrapy.Request):

    url_origin = ''
    retry = {}
    max_attempts = 30
    retry_enabled = True
    method = 'GET'

    def __init__(self, url, callback, method='GET', meta=None, options=None, **kwargs):
        meta = copy.deepcopy(meta) or {}
        if 'http://selenium' not in url:
            self.url_origin = url
        self.retry['callback'] = callback
        self.retry['method'] = method
        self.retry['meta'] = meta
        self.retry['options'] = options
        self.method = method
        if 'attempt' not in self.retry:
            self.retry['attempt'] = 1
        else:
            self.retry['attempt'] += 1
        params = {'url': url}
        if options is not None and type(options) is dict:
            for key, val in options.items():
                params[key] = val
        #pprint(urlencode(params))
        url = 'http://selenium:8000/html?' + urlencode(params)
        super(SelenuimRequest, self).__init__(url, callback=callback, method=self.method, meta=meta, **kwargs)






