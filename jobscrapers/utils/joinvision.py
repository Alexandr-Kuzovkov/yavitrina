#!/usr/bin/env python

import suds

class Joinvision:


    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.bucketname = u'myxtramile'
        self.customer = unicode(self.login + ':' + self.password,'utf-8')

    def query_index(self, contentString, constraintsString, focus='job', resultSize=1):
        try:
            client = suds.client.Client('http://global.joinvision.com/matchpoint/indexservicesoap?wsdl')
            contents = client.factory.create('StringContainer')
            if type(contentString) is list:
                contents.strings.extend(contentString)
            else:
                contents.strings.extend([contentString])
            constraints = client.factory.create('StringContainer')
            if type(constraintsString) is list:
                constraints.strings.extend(constraintsString)
            else:
                constraints.strings.extend([contentString])
            #buckets = client.service.getAvailableBuckets(self.customer)
            searchresult = client.service.queryIndex(self.customer, contents, constraints, focus, resultSize)
            if hasattr(searchresult, 'results'):
                return searchresult.results
            else:
                return {'results':'no results'}
        except Exception, ex:
            return {'error': ex.message}


    def analyse(self, contentString, lang='en'):
        try:
            client = suds.client.Client('http://global.joinvision.com/matchpoint/indexservicesoap?wsdl')
            contents = client.factory.create('StringContainer')
            if type(contentString) is list:
                contents.strings.extend(contentString)
            else:
                contents.strings.extend([contentString])
            searchresult = client.service.analyse(self.customer, contents, lang)
            return searchresult
        except Exception, ex:
            return {'error': ex.message}

username = 'myxtramile'
password = 'n89zk?'
jv = Joinvision(username, password)

#res = jv.query_index([u'id=job_21119;type=job', u'keyword=Java'], u'', 'job', 200)
#print res
#print len(res)

#res = jv.analyse(u'id=job_21119;type=job')
res = jv.analyse([u'id=cv_1339;type=cv'])
print res



