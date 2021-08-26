import elasticsearch as elastic

from SpiderKeeper.app.fibois.settings import SCRAPING_ES_URL

class Elastic:
    _es = None

    class __Elastic:
        _es = None
        def __init__(self):
            self._es = elastic.Elasticsearch(
                [SCRAPING_ES_URL],
            )

        def getEs(self):
            return self._es

    instance = None
    def __init__(self):
        if not Elastic.instance:
            Elastic.instance = Elastic.__Elastic()
        else:
            Elastic._es = Elastic.instance._es

    def __getattr__(self, name):
        return getattr(self.instance, name)
