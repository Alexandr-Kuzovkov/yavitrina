#coding=utf-8

import elasticsearch as elastic
from elasticsearch import serializer, compat, exceptions
import json

class JSONSerializerPython2(serializer.JSONSerializer):
    """Override elasticsearch library serializer to ensure it encodes utf characters during json dump.
    See original at: https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L42
    A description of how ensure_ascii encodes unicode characters to ensure they can be sent across the wire
    as ascii can be found here: https://docs.python.org/2/library/json.html#basic-usage
    """
    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, compat.string_types):
            return data
        try:
            return json.dumps(data, default=self.default, ensure_ascii=True)
        except (ValueError, TypeError) as e:
            raise exceptions.SerializationError(data, e)

class Elastic:
    _es = None

    class __Elastic:
        _es = None
        def __init__(self, SCRAPING_ES_URL):
            self._es = elastic.Elasticsearch(
                [SCRAPING_ES_URL],
                serializer=JSONSerializerPython2()
            )

        def getEs(self):
            return self._es

    instance = None
    def __init__(self, SCRAPING_ES_URL):
        if not Elastic.instance:
            Elastic.instance = Elastic.__Elastic(SCRAPING_ES_URL)
        else:
            Elastic._es = Elastic.instance._es

    def __getattr__(self, name):
        return getattr(self.instance, name)







