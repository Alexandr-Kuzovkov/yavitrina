import scrapy
import time
from scrapy.loader import ItemLoader
from jobscrapers.items import JobItem
from pprint import pprint
import urllib

class PernodricardSpider(scrapy.Spider):

    name = "swissre"
    publisher = "SwissRe"
    publisherurl = 'http://www.swissre.com/careers/'

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)

    def start_requests(self):
        allowed_domains = ["https://career2.successfactors.eu"]
        urls = [
            'https://career2.successfactors.eu/career?company=SwissRe&career_ns=job_listing_summary'
        ]
        data_str = formdata.data['swissre']['data']
        data = {}
        for par_val in data_str.split('&'):
            data[par_val.split('=')[0]] = par_val.split('=')[1]
        pprint(data)
        body = urllib.urlencode(data)
        headers = formdata.data['swissre']['headers']
        headers['Content-Length'] = len(body)
        for url in urls:
            #request = scrapy.Request(url=url, callback=self.getJobList, body=body, method='GET', headers=headers)
            request = scrapy.Request(url=url, callback=self.getJobList, method='GET')
            yield request



    def getJobList(self, response):
        url = 'https://career2.successfactors.eu/xi/ajax/remoting/call/plaincall/careerJobSearchControllerProxy.getInitialJobSearchData.dwr'
        data_str = formdata.data['swissre']['data2']
        data = {}
        for par_val in data_str.split('&'):
            data[par_val.split('=')[0]] = par_val.split('=')[1]
        pprint(data)
        body = urllib.urlencode(data)
        headers = formdata.data['swissre']['headers2']
        headers['Content-Length'] = len(body)
        request = scrapy.Request(url=url, callback=self.parsePage, body=body, method='POST', headers=headers)
        pprint(request.headers)
        pprint(request.body)
        yield request


    def parsePage(self, response):
        self.logger.info('Parsing page %s ...' % response.url)
        #links = list(set(response.xpath('//*').re('https://tas-pr.taleo.net/careersection/jobdetail\.ftl\?job=[A-Z]{2,5}[0-9]{2,6}')))
        f = open('swissre.txt', 'w')
        f.write(response.text.encode('utf-8'))
        f.close()
        response.text
        exit()
        items = response.css('ul.jobList li.jobResultItem')
        for item in items:
            url = item.xpath('a[@class="readMore"]/@href').extract()[0]
            date = self.getPublishDate(item)
            request = scrapy.Request(url=url, callback=self.parseJob)
            request.meta['date'] = date
            yield request

        select_to_link_next = response.xpath("//a[contains(.//text(), 'Next')]")
        if len(select_to_link_next) != 0:
            link_to_next = select_to_link_next.xpath('@href').extract()[0]
            yield scrapy.Request(url=link_to_next, callback=self.parsePage)


    def parseJob(self, response):
        self.logger.info('...Parsing job: %s ...' % response.url)
        #raise Exception('Custom error')
        l = ItemLoader(item=JobItem(), response=response)
        l.add_value('url', response.url)
        l.add_value('title', response.css('h1').xpath('text()').extract()[0].strip())
        l.add_value('date', response.meta['date'])
        try:
            l.add_value('category', response.css('div.jobDetail').xpath('p/text()')[0].extract().strip())
        except Exception, ex:
            l.add_value('category', '')
        try:
            l.add_value('contract', response.css('div.jobDetail').xpath('p/text()')[1].extract().strip())
        except Exception, ex:
            l.add_value('contract', '')
        try:
            l.add_value('location', response.css('div.jobDetail').xpath('p/text()')[2].extract().strip())
        except Exception, ex:
            l.add_value('location', '')
        try:
            l.add_value('country', response.css('div.jobDetail').xpath('p/text()')[3].extract().strip())
        except Exception, ex:
            l.add_value('country', '')
        l.add_value('description',  self.getDescription(response))
        l.add_value('utime', int(time.time()))
        yield l.load_item()


    def getPublishDate(self, selector):
        months = {u'Jan': 1, u'Feb': 2, u'Mar': 3, u'Apr': 4, u'May': 5, u'Jun': 6, u'Jul': 7, u'Aug': 8, u'Sep': 9,
                  u'Oct': 10, u'Nov': 11, u'Dec': 12}
        date_string = selector.xpath('p[@class="jobDef"]/span')[2].xpath('text()').extract()[0].split('Published date:').pop().strip()
        day, month, year = date_string.split('-')
        day = int(day)
        month = months[month]
        year = int(year)
        date = int(time.mktime((year, month, day, 0, 0, 0, 0, 0, 0)))
        return date

    def getDescription(self, response):
        desc = ''.join(response.xpath('//div[@class="jobDetailDescription"]//*|//div[@class="jobDetailDescription"]/text()').extract())
        return desc
