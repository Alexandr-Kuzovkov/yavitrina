from SpiderKeeper.app.fibois.elastic import Elastic
from SpiderKeeper.app.fibois.settings import SCRAPING_ES_URL
from SpiderKeeper.app.fibois.settings import SCRAPING_ES_INDEX
from SpiderKeeper.app.fibois.settings import PARSING_ES_INDEX
from SpiderKeeper.app.fibois.settings import PARSING_ES_URL
from SpiderKeeper.app.fibois.settings import PAGE_RESULTS_HTML_INDEX
from SpiderKeeper.app.fibois.settings import JOB_DETAILS_HTML_INDEX
from SpiderKeeper.app.fibois.constants import jobboards
from SpiderKeeper.app.fibois.constants import occupations
from SpiderKeeper.app.fibois.constants import lesjeudis_keywords
import elasticsearch as elastic
import datetime

import time

REQUEST_TIMEOUT = 30

SCRAPING_INDEX_EUROPA = 'scraped-jobs_ec.europa.eu'
PAGE_RESULTS_HTML_INDEX_EUROPA = 'scrapping-page-results-html_ec.europa.eu'
JOB_DETAILS_HTML_INDEX_EUROPA = 'scrapping-job-details-html_ec.europa.eu'

def get_stat():
    es = Elastic().getEs()
    body = {'query': {'match_all': {}}}
    stat = {}
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    count2 = es.count(index=SCRAPING_INDEX_EUROPA, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    stat['total_jobs'] = count + count2

    stat['total_processed_jobs'] = get_count_per_status("processed") 
    stat['total_pending_jobs'] = get_count_per_status("pending")
    stat['total_errored_jobs'] = get_count_per_status("errored")

    count = es.count(index=PAGE_RESULTS_HTML_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    count2 = es.count(index=PAGE_RESULTS_HTML_INDEX_EUROPA, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    stat['total_results_html'] = count+count2
    count = es.count(index=JOB_DETAILS_HTML_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    count2 = es.count(index=JOB_DETAILS_HTML_INDEX_EUROPA, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    stat['total_details_html'] = count+count2
    return stat

def get_stat_by_jobboards():
    stat = {}
    es = Elastic().getEs()
    for jobboard in jobboards:
        body = {
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                    "jobboard.keyword": jobboard
                  }
                }
              ]
            }
          }
        }
        count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
        stat[jobboard] = count
    return stat

def get_count_for_jobboard(jobboard):
    es = Elastic().getEs()
    body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "jobboard.keyword": jobboard
              }
            }
          ]
        }
      }
    }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count


# returns the total count per status in {"errored", "processed", "pending"}
def get_count_per_status(status):
    es = Elastic().getEs()
    body = {}
    # for pending jobs, we check the absence of the status field
    if status == "pending":
      body = {
        "query": {
          "bool": {
            "must_not": {"exists": {"field": "status"}}
          }
        }
      }
    else: 
      body = {
      "query": {
        "bool": {
          "must": [
            { "term": { "status.keyword": status  }}
          ]
        }
      }
    }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count



#retrieve the count of jobboards given a status in {"errored", "processed", "pending"}
def get_count_for_jobboard_per_status(jobboard, status):
    es = Elastic().getEs()
    body = {}
    # for pending jobs, we check the absence of the status field
    if status == "pending":
      body = {
        "query": {
          "bool": {
            "must": [{"term": {"jobboard.keyword": jobboard}}],
            "must_not": {"exists": {"field": "status"}}
          }
        }
      }
    else: 
      body = {
      "query": {
        "bool": {
          "must": [
            { "term": {"jobboard.keyword": jobboard }}, 
            { "term": { "status.keyword": status  }}
          ]
        }
      }
    }
    if jobboard in ['ec.europa.eu-api']:
        index = SCRAPING_INDEX_EUROPA
    else:
        index = SCRAPING_ES_INDEX
    count = es.count(index=index, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count


def get_stat_by_date():
    es = Elastic().getEs()
    stat = []
    now = int(time.time())
    for i in range(0, 30):
        datestr = time.strftime('%Y-%m-%d', time.gmtime(now - i * 86400))
        body = {
          "query": {
                    "match_phrase_prefix": {"scrapping_date": datestr}
                }
        }
        count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
        stat.append({'date': datestr, 'count': count})
    return stat

def get_dates():
    dates = []
    now = int(time.time())
    for i in range(0, 30):
        datestr = time.strftime('%Y-%m-%d', time.gmtime(now - i * 86400))
        dates.append(datestr)
    return dates

def get_count_for_date(datestr):
    es = Elastic().getEs()
    start_date, end_date = get_dates_range(datestr)
    body ={
              "query": {
                "range": {
                  "scrapping_date": {
                    "gte": start_date,
                    "lte": end_date
                  }
                }
              }
            }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    count2 = es.count(index=SCRAPING_INDEX_EUROPA, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    count += count2
    return count

#retrieve the total count per date given a status in {"errored", "processed", "pending"}
def get_count_for_date_per_status(datestr, status):
    es = Elastic().getEs()
    start_date, end_date = get_dates_range(datestr)
    body = {}
    if status == "pending":
        body = {
          "query": {
            "bool": {
              "must_not": {"exists": {"field": "status"}},
              "must": [
                {
                  "range": {
                    "scrapping_date": {
                      "gte": start_date,
                      "lte": end_date
                    }
                  }
                }
              ]
            }
          }
        }
    else:
        body = {
          "query": {
            "bool": {
              "must": [
                { "term": { "status.keyword": status }},
                {
                  "range": {
                    "scrapping_date": {
                      "gte": start_date,
                      "lte": end_date
                    }
                  }
                }
              ]
            }
          }
        }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count


#retrieve the total count of *parsed* jobs per date
def get_parsed_count_for_date(datestr):
    es = elastic.Elasticsearch([PARSING_ES_URL])
    start_date, end_date = get_dates_range_for_parsing_index(datestr)
    body ={
      "query": {
        "range": {
          "created_at": {
            "gte": start_date,
            "lte": end_date
          }
        }
      }
    }
    count = es.count(index=PARSING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count       

def get_stat_by_jb_date():
    es = Elastic().getEs()
    stat = {}
    for jobboard in jobboards:
        if not jobboard in stat:
            stat[jobboard] = []
        now = int(time.time())
        for i in range(0, 30):
            datestr = time.strftime('%Y-%m-%d', time.gmtime(now - i * 86400))
            start_date, end_date = get_dates_range(datestr)
            body = {
              "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "jobboard.keyword": jobboard
                      }
                    },
                    {
                        "range": {
                          "scrapping_date": {
                            "gte": start_date,
                            "lte": end_date
                          }
                        }
                    }
                  ]
                }
              }
            }
            if jobboard in ['ec.europa.eu-api']:
                index = SCRAPING_INDEX_EUROPA
            else:
                index = SCRAPING_ES_INDEX
            count = es.count(index=index, body=body, request_timeout=REQUEST_TIMEOUT)['count']
            stat[jobboard].append({'date': datestr, 'count': count})
    return stat

def get_count_for_jb_date(jobboard, datestr):
    es = Elastic().getEs()
    start_date, end_date = get_dates_range(datestr)
    body = {
      "query": {
        "bool": {
          "must": [
            {
              "term": {
                "jobboard.keyword": jobboard
              }
            },
              {
                  "range": {
                      "scrapping_date": {
                          "gte": start_date,
                          "lte": end_date
                      }
                  }
              }
          ]
        }
      }
    }
    if jobboard in ['ec.europa.eu-api']:
        index = SCRAPING_INDEX_EUROPA
    else:
        index = SCRAPING_ES_INDEX
    count = es.count(index=index, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count

def get_jobboards():
    return jobboards

def get_stat_by_keywords(jobboard):
    stat = []
    es = Elastic().getEs()
    if jobboard == 'lesjeudis.com':
        keywords = lesjeudis_keywords
    else:
        keywords = occupations
    keywords.sort()
    for keyword in keywords:
        body = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "search_term.keyword": unicode(keyword, 'utf8')
                            }
                        },
                        {
                            "term": {
                                "jobboard.keyword": jobboard
                            }
                        }
                    ]
                }
            }
        }
        if jobboard in ['ec.europa.eu-api']:
            index = SCRAPING_INDEX_EUROPA
        else:
            index = SCRAPING_ES_INDEX
        count = es.count(index=index, body=body, request_timeout=REQUEST_TIMEOUT)['count']
        stat.append({'keyword': unicode(keyword, 'utf8'), 'count': count})
    return stat

def get_count_for_keywords(jobboard, keyword_index):
    es = Elastic().getEs()
    if jobboard == 'lesjeudis.com':
        keywords = lesjeudis_keywords
    else:
        keywords = occupations
    keywords.sort()
    keyword = keywords[int(keyword_index)]
    if jobboard in ['ec.europa.eu-api']:
        index = SCRAPING_INDEX_EUROPA
    else:
        index = SCRAPING_ES_INDEX
    body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "search_term.keyword": unicode(keyword, 'utf8')
                        }
                    },
                    {
                        "term": {
                            "jobboard.keyword": jobboard
                        }
                    }
                ]
            }
        }
    }
    count = es.count(index=index, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count

def get_keywords(jobboard):
    if jobboard == 'lesjeudis.com':
        keywords = lesjeudis_keywords
    else:
        keywords = occupations
    keywords.sort()
    return map(lambda i: unicode(i, 'utf8'), keywords)

def get_expired_for_jobboard(jobboard):
    es = Elastic().getEs()
    body = {
              "query": {
                "bool": {
                  "must": [
                    {
                      "term": {
                        "jobboard.keyword": jobboard
                      }
                    },
                    {
                        "match": {"url_status": "expired"}
                    }
                  ]
                }
              }
            }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count

def get_expired_total():
    es = Elastic().getEs()
    body = {
              "query": {
                "bool": {
                  "must": [
                    {
                        "match": {"url_status": "expired"}
                    }
                  ]
                }
              }
            }
    count = es.count(index=SCRAPING_ES_INDEX, body=body, request_timeout=REQUEST_TIMEOUT)['count']
    return count

def get_dates_range(datestr):
    d = list(map(lambda i: int(i.strip()), datestr.split('-')))
    dt = datetime.datetime(d[0], d[1], d[2], 0, 0, 0, 0)
    startdate = dt.strftime('%Y-%m-%d 00:00:00')
    enddate = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
    return (startdate, enddate)


# returns date range for parsing index, the format is slightly different from the scraping index
def get_dates_range_for_parsing_index(datestr):
    d = list(map(lambda i: int(i.strip()), datestr.split('-')))
    dt = datetime.datetime(d[0], d[1], d[2], 0, 0, 0, 0)
    startdate = dt.strftime('%Y-%m-%dT00:00:00Z')
    enddate = (dt + datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00Z')
    return (startdate, enddate)