#!/usr/bin/env python

####################################################
# Script for generate annotation for swgger-php
####################################################

from pg import PgSQLStore
from pprint import pprint
from mylogger import logger
import time
import requests
import certifi
import json
import sys
import re


def get_type(value):
    if type(value) is int:
        return 'integer'
    if type(value) is float:
        return 'float'
    if type(value) is str or type(value) is unicode:
        if value.isdigit():
            return 'integer'
        else:
            if value.replace('.', '').isdigit():
                return 'float'
        if value == '{}':
            return 'array'
        if value in ['t', 'f', 'true', 'false']:
            return 'boolean'
        p = re.compile('\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d')
        if p.match(value):
            return 'datetime'
        p = re.compile('\d\d\d\d-\d\d-\d\d')
        if p.match(value):
            return 'datetime'
        return 'string'

argc = len(sys.argv)
body = None
method = 'GET'
BASE_URL = 'http://localhost:8000/app_dev.php'
TAG = 'TAG'
TAG = 'X-Jobs'
request_body = ''

if argc < 2:
    print 'Usage: %s url [GET|POST|PUT] [body]' % sys.argv[0]
    exit(0)

#headers = {'Authorization': 'reJwHFHwH1dxl3CS01oMIUQDKfaGzCb1NUZ0KqLGYpUvoezpRZxsIDIsa4kkHWHd', 'Content-Type': 'application/json'} #admin
headers = {'Authorization': '5RJL0GL1BIWByCc0whlmKAU5poOZLJ3tHPOwx1tSXBETfH5J0nRmiNUxsbEkg9WH', 'Content-Type': 'application/json'} #employer 31
if 'napi' in sys.argv[1]:
    BASE_URL = 'http://localhost:8000'
url = ''.join([BASE_URL, sys.argv[1]])
if argc > 2:
    method = sys.argv[2]
if argc > 3:
    body = sys.argv[3]
    if body[0:1] == '@':
        body = json.loads(open(body.replace('@', ''), 'r').read())
    else:
        body = json.loads(body)

if method == 'GET':
    res = requests.get(url=url, headers=headers)
elif method == 'POST':
    headers['Accept'] = 'application/json'
    res = requests.post(url=url, headers=headers, data=json.dumps(body))
elif method == 'PUT':
    headers['Accept'] = 'application/json'
    res = requests.put(url=url, headers=headers, data=json.dumps(body))
else:
    exit()


try:
    output = []
    #pprint(res.text)
    response = json.loads(res.text)
    if type(response) is list and len(response) > 0:
        item = response[0]
    elif type(response) == dict:
        item = response
    else:
        logger.info('Result is empty')
        exit(0)
    line = '''

    /**
     * @OA\Schema(
     *     schema="entity",'''
    output.append(line)
    for key, val in item.items():
        if '@' in key:
            continue
        fld_type = get_type(val)
        if fld_type == 'array':
            output.append('*            @OA\Property(property="%s",type="array", @OA\Items(type="string")),' % (key,))
        else:
            output.append('*            @OA\Property(property="%s",type="%s"),' % (key, fld_type))
    output.append("example=\n%s" % json.dumps(item).replace(',', ',\n'))
    line = '''*       )
     * **/'''

    output.append(line)
    parameters = '''@OA\Parameter(
 *     name="id",
 *     in="path",
 *     required=true,
 *     description="ID",
 *     @OA\Schema(type="integer")
 *   ),
 *      @OA\Parameter(
 *     name="page",
 *     in="query",
 *     required=false,
 *     description="Page number",
 *     @OA\Schema(type="integer")
 *   ),
 *     @OA\Parameter(
 *     name="size",
 *     in="query",
 *     required=false,
 *     description="Items per page",
 *     @OA\Schema(type="integer")
 *   ),
 *     @OA\Parameter(
 *     name="sort",
 *     in="query",
 *     required=false,
 *     description = "Field for sorting result, - before means DESC",
 *     @OA\Schema(type="string")
 *   ),
 *      @OA\Parameter(
 *     name="totalItems",
 *     in="query",
 *     required=false,
 *     description="Get total items instead result",
 *     @OA\Schema(type="string")
 *   ),'''

    if method in ['POST', 'PUT']:
        if type(body) is list and len(body) > 0:
            item = body[0]
        elif type(body) is dict:
            item = body
        request_body = []
        line = '''@OA\RequestBody(
                *         @OA\MediaType(
                *             mediaType="application/json",
                *             @OA\Schema('''
        request_body.append(line)
        for key, val in item.items():
            if '@' in key:
                continue
            fld_type = get_type(val)
            if fld_type == 'array':
                request_body.append('*            @OA\Property(property="%s",type="array", @OA\Items(type="string")),' % (key,))
            else:
                request_body.append('*            @OA\Property(property="%s",type="%s"),' % (key, fld_type))
        request_body.append("example=\n%s" % json.dumps(item).replace(',', ',\n'))
        line = '''
 *                          )
 *                  )
 *          ),
         '''
        request_body.append(line)
        parameters = '\n'.join(request_body)
    line = '''/**
 * @OA\%s(
 *     path="%s",
 *     summary="SUMMARY",
 *     tags={"%s"},
 *      %s
 *     @OA\Response(
 *         response=200,
 *         description="OK",
 *          @OA\MediaType(
 *             mediaType="application/json",
 *             @OA\Schema(
 *                 type="array",
 *                  @OA\Items(ref="#/components/schemas/entity"),
 *             )
 *         )
 *     )
 * )
 */''' % (method.lower().capitalize(), sys.argv[1], TAG, parameters)

    output.append(line)
    output = '\n'.join(output)
    print output

except Exception, ex:
    logger.error(ex.message)
