#!/usr/bin/env python
#coding=utf-8

import os
import time
import sys
import requests
from pprint import pprint
import re
from mylogger import logger



start_date = '2019-01-01'
end_date = '2019-02-28'
URL = 'https://neuvoo.ca/advertiser/campaign/index.php?lang=en&startDate={start_date}&endDate={end_date}'.format(start_date=start_date, end_date=end_date)

headers1 = '''Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
Accept-Encoding: gzip, deflate, br
Accept-Language: en-US,en;q=0.9,ru;q=0.8,fr;q=0.7,de;q=0.6
Cache-Control: no-cache
Connection: keep-alive
Content-Length: 172
Content-Type: application/x-www-form-urlencoded
Cookie: PHPSESSID=hfvrfl8bpgikme4lqcvd72p0i0
Host: neuvoo.ca
Origin: https://neuvoo.ca
Pragma: no-cache
Referer: {url}
Upgrade-Insecure-Requests: 1
User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'''.format(url=URL)


headers2 = '''Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8
    Accept-Encoding: gzip, deflate, br
    Accept-Language: en-US,en;q=0.9,ru;q=0.8,fr;q=0.7,de;q=0.6
    Cache-Control: no-cache
    Connection: keep-alive
    Cookie: PHPSESSID=hfvrfl8bpgikme4lqcvd72p0i0
    Host: neuvoo.ca
    Pragma: no-cache
    Upgrade-Insecure-Requests: 1
    User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'''

select = '''<select id='empcode_select' name='empcode'>
<option value ='xtramile' >xtramile</option>
<option value ='xtramile-aux-us' >xtramile-aux-us</option>
<option value ='xtramile-jobleads-uk' >xtramile-jobleads-uk</option>
<option value ='xtramile-jobmonitor-de' >xtramile-jobmonitor-de</option>
<option value ='xtramile-jobleads-usa' >xtramile-jobleads-usa</option>
<option value ='xtramile-jobleads-at' >xtramile-jobleads-at</option>
<option value ='xtramile-jobleads-de' >xtramile-jobleads-de</option>
<option value ='xtramile-jobleads-fr' >xtramile-jobleads-fr</option>
<option value ='xtramile-jobleads-nl' >xtramile-jobleads-nl</option>
<option value ='xtramile-jobleads-ch' >xtramile-jobleads-ch</option>
<option value ='xtramile-jobleads-nz' >xtramile-jobleads-nz</option>
<option value ='xtramile-jobleads-ca' >xtramile-jobleads-ca</option>
<option value ='xtramile-jobleads-in' >xtramile-jobleads-in</option>
<option value ='xtramile-jobleads-be' >xtramile-jobleads-be</option>
<option value ='xtramile-jobleads-es' >xtramile-jobleads-es</option>
<option value ='xtramile-jobleads-tr' >xtramile-jobleads-tr</option></select>'''

def get_headers(headers_raw):
    headers_list = map(lambda i: i.split(':'), headers_raw.split('\n'))
    headers = {}
    for item in headers_list:
        headers[item[0].strip()] = item[1].strip()
    return headers


def change_company(company):
    form_data = {
        'empcode': company,
        'action': 'change-empcode',
        'return_address': URL
    }
    url = 'https://neuvoo.ca/advertiser/a/actions.php?lang=en'
    res = requests.post(url=url, headers=get_headers(headers1), data=form_data)
    #pprint(res.text)
    return res.status_code


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext

def get_companies(select):
    regopt = re.compile("<option value ='.*?' >")
    opts = regopt.findall(select)
    companies = map(lambda i: i.split("'")[1], opts)
    return companies



summary_stat = {}

companies = get_companies(select)
for company in companies:
    logger.info('get stat for company "%s"' % company)
    pprint(change_company(company))
    #pprint(get_headers())
    url = URL
    res = requests.get(url=url, headers=get_headers(headers2))
    #pprint(res.text)
    regdate = re.compile("<td\ class='left\ Date'>.*?<\/td>")
    dates = regdate.findall(res.text)
    regclicks = re.compile("<td\ class='left\ Paid\ Clicks'>.*?<\/td>")
    clicks = regclicks.findall(res.text)
    if len(clicks) == 0:
        regclicks = re.compile("<td\ class='left\ Clics\ payés'>.*?<\/td>")
        clicks = regclicks.findall(res.text)
    regcost = re.compile("<td\ class='left\ Cost'>.*?<\/td>")
    costs = regcost.findall(res.text)
    if len(costs) == 0:
        regcost = re.compile("<td\ class='left\ Coût'>.*?<\/td>")
        costs = regcost.findall(res.text)

    stat = []
    for i in range(0, min(len(dates), len(clicks), len(costs))):
        if dates[i] == u'Total':
            continue
        stat.append({'date': cleanhtml(dates[i]), 'clicks': int(cleanhtml(clicks[i]).replace(',', '')), 'costs': float(cleanhtml(costs[i]).replace(' (EUR)', '').replace(',', ''))})
    logger.info('done')
    #pprint(stat)
    summary_stat[company] = stat

total_stat = {}

for stat in summary_stat.values():
    for daystat in stat:
        if daystat['date'] in total_stat:
            total_stat[daystat['date']]['clicks'] += daystat['clicks']
            total_stat[daystat['date']]['costs'] += daystat['costs']
        else:
            total_stat[daystat['date']] = {'clicks': daystat['clicks'], 'costs': daystat['costs']}

pprint(total_stat)
open('neuvoo.{start_date}:{end_date}.txt'.format(start_date=start_date, end_date=end_date), 'w').write(str(total_stat))




