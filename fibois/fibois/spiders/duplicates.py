# -*- coding: utf-8 -*-
import scrapy
import json
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest
import math
import re
import os
import pkgutil
from pprint import pprint
from fibois.items import JobStatus
import time
import html2text
import datetime
import logging
import urllib
from fibois.keywords import occupations
from fibois.keywords import companies
from fibois.keywords import last_posted
from fibois.keywords import job_expired_messages
from fibois.scrapestack import ScrapestackRequest


#Worker for removing duplicates of jobs
class DuplicatesSpider(scrapy.Spider):
    name = 'duplicates'
    allowed_domains = []
    dirname = 'duplicates'
    logger = logging.getLogger()
    limit = None #page limit
    drain = False
    pagination = {}
    scrapestack_access_key = ''
    use_scrapestack = True
    es_exporter = None
    debug = False
    port = None
    es = None
    index = None
    REQUEST_TIMEOUT = 60

    '''
    @param keywords Type of keywords ['occupations', 'companies']
    @param limit Limit of number pages for parsing, int
    @param keywords_limit Limit of number used keywords from keywords list, int
    @param drain Run without save data
    @param delta_days Age of article in days for scraping
    '''
    def __init__(self, limit=False, drain=False, debug=False, port=None, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        if limit:
            self.limit = int(limit)
        if drain:
            self.drain = True
        if debug:
            self.debug = True
        self.port = port

    def start_requests(self):
        url = 'http://localhost:6800'
        self.index = self.es_exporter.index
        self.es = self.es_exporter.es
        if self.port is not None:
            url = 'http://localhost:{port}'.format(port=self.port)
        request = scrapy.Request(url, callback=self.start)
        yield request

    def start(self, response):
        self.logger.info('Removing jobs duplicates...')
        self.delete_duplicates_job_details_html()
        self.remove_duplicates_from_scraping_index()

    '''
    Fetching duplicates from "scrapping-job-details-html-v01" index
    '''
    def get_duplicates_job_details_html(self):
        job_details_index = 'scrapping-job-details-html'
        search_body = {
            "size": 0,
            "aggs": {
                "parents": {
                    "terms": {
                        "field": "url.keyword",
                        "size": 10000
                    },
                    "aggs": {
                        "doc_id": {
                            "top_hits": {
                                "size": 1,
                                "_source": False
                            }
                        }
                    }
                }
            }
        }
        res = self.es.search(index=job_details_index, body=search_body, track_scores=True, request_timeout=self.REQUEST_TIMEOUT)
        res = list(filter(lambda i: i['doc_count'] > 1, res['aggregations']['parents']['buckets']))
        data = list(
            map(lambda i: {'url': i['key'], 'doc_count': i['doc_count'], '_id': i['doc_id']['hits']['hits'][0]['_id']},
                res))
        self.logger.debug(data)
        self.logger.info(len(data))
        return data

    '''
    Deleting duplicates in "scrapping-job-details-html" index
    '''
    def delete_duplicates_job_details_html(self):
        self.logger.info('Deleting duplicates in "scrapping-job-details-html"...')
        duplicates = self.get_duplicates_job_details_html()
        index = 'scrapping-job-details-html'
        self.logger.info('fetched {count} duplicates records'.format(count=len(duplicates)))
        if len(duplicates) > 0:
            urls = list(map(lambda i: i['url'], duplicates))
            ids = list(map(lambda i: i['_id'], duplicates))
            # pprint(urls)
            # pprint(ids)
            query_body = {
                "query": {
                    "bool": {
                        "must": {
                            "terms": {
                                "url.keyword": urls
                            }
                        },
                        "must_not": {
                            "ids": {
                                "values": ids
                            }
                        }
                    }
                }
            }
            res = self.es.delete_by_query(index=index, body=query_body, request_timeout=self.REQUEST_TIMEOUT)
            self.logger.info(res)
            self.logger.info('done')

    '''
    Request for fetching duplicates from "scraping-81d2cf20-3977-11eb-822b-bbe610d3c74d" index
    '''
    def get_duplicates_scraping_index(self, after=None):
        index = self.index
        #index = 'scraping-sandbox2'
        search_body = {
            "size": 0,
            "aggs": {
                "parents": {
                    "composite": {
                        "sources": [
                            {"url": {"terms": {"field": "url.keyword", "order": "desc"}}},
                            {"search_term": {"terms": {"field": "search_term.keyword", "order": "desc"}}}
                        ],
                        "size": 10000
                    },
                    "aggs": {
                        "doc_id": {
                            "top_hits": {
                                "size": 1,
                                "_source": False
                            }
                        }
                    }
                }
            }
        }
        if after is not None:
            search_body['aggs']['parents']['composite']['after'] = after
        res = self.es.search(index=index, body=search_body, track_scores=True, request_timeout=self.REQUEST_TIMEOUT * 10)
        if 'after_key' in res['aggregations']['parents']:
            after = res['aggregations']['parents']['after_key']
        else:
            after = None
        res = list(filter(lambda i: i['doc_count'] > 1, res['aggregations']['parents']['buckets']))
        data = list(map(
            lambda i: {'url': i['key']['url'], 'search_term': i['key']['search_term'], 'doc_count': i['doc_count'],
                       '_id': i['doc_id']['hits']['hits'][0]['_id']}, res))
        self.logger.debug(data)
        self.logger.debug(len(data))
        return data, after

    '''
    Deleting duplicates in "scraping-81d2cf20-3977-11eb-822b-bbe610d3c74d" index
    '''
    def delete_duplicates_scraping_index(self, duplicates):
        index = self.index
        #index = 'scraping-sandbox2'
        self.logger.info('fetched {count} duplicates records'.format(count=len(duplicates)))
        if len(duplicates) > 0:
            for offset in range(0, len(duplicates), 1000):
                records = duplicates[offset:1000]
                urls = list(map(lambda i: i['url'], records))
                ids = list(map(lambda i: i['_id'], records))
                self.logger.debug(urls)
                self.logger.debug(ids)
                should_items = list(map(lambda item: {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "search_term.keyword": item['search_term']
                                }
                            },
                            {
                                "term": {
                                    "url.keyword": item['url']
                                }
                            }
                        ]

                    }
                }, records))

                query_body = {
                    "query": {
                        "bool": {
                            "should": should_items,
                            "must_not": {
                                "ids": {
                                    "values": ids
                                }
                            }
                        }
                    }
                }
                res = self.es.delete_by_query(index=index, body=query_body, request_timeout=self.REQUEST_TIMEOUT * 10)
                print(res)

    '''
    Deleting duplicates from "scraping-81d2cf20-3977-11eb-822b-bbe610d3c74d" index in cycle
    '''
    def remove_duplicates_from_scraping_index(self):
        self.logger.info('Deleting duplicates in "scraping-81d2cf20-3977-11eb-822b-bbe610d3c74d"...')
        count = 0
        after = None
        data, after = self.get_duplicates_scraping_index()
        self.delete_duplicates_scraping_index(data)
        count += 1
        while after is not None:
            data, after = self.get_duplicates_scraping_index(after)
            self.logger.info('{count}: {length}'.format(count=count, length=len(data)))
            self.delete_duplicates_scraping_index(data)
            count += 1
        self.logger.info('done')












