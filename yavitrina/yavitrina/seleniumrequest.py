import scrapy
import copy
from urllib import urlencode
from pprint import pprint
import logging

class SelenuimRequest(scrapy.Request):

    url_origin = ''
    max_attempts = 30
    method = 'GET'

    def __init__(self, url, callback, method='GET', meta=None, options=None, **kwargs):
        meta = copy.deepcopy(meta) or {}
        #self.url_origin = url
        if 'http://selenium' not in url:
            self.url_origin = url
        self.method = method
        params = {'url': url}
        if options is not None and type(options) is dict:
            for key, val in options.items():
                params[key] = val
        #pprint(urlencode(params))
        url = 'http://selenium:8000/html?' + urlencode(params)
        super(SelenuimRequest, self).__init__(url, callback=callback, method=self.method, meta=meta, **kwargs)






