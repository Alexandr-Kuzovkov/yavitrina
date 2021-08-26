import hashlib
from lxml import etree
import zeep
import time
from zeep import Client, Settings
from zeep import transports
from requests import Session
from zeep import xsd
from zeep.plugins import HistoryPlugin
import urllib3
from pprint import pprint
import certifi
import re
from gcd_apec_feedgenerator.items import category2naf
urllib3.disable_warnings()
def zeep_pythonvalue(self, xmlvalue):
    return xmlvalue
zeep.xsd.simple.AnySimpleType.pythonvalue = zeep_pythonvalue
from tidylib import tidy_document, tidy_fragment


class MyTransport(transports.Transport):
    headers = None
    xml_request = None

    def post(self, address, message, headers):
        if self.xml_request is not None:
            message = self.xml_request
        if self.headers is not None:
            headers = self.headers
        for line in message.split('\n'):
            pprint(line)
        pprint(address)
        pprint(headers)
        response = super(self.__class__, self).post(address, message, headers)
        self.response = response
        return response


class ApecAPI:

    APEC_ACCOUNT = {
        'wsdl': 'https://testadepsides.apec.fr/v5/positions?wsdl',
        #wsdl = 'https://testadepsep.apec.fr/v5/positions?wsdl'
        #wsdl = 'https://adepsides.apec.fr/v5/positions?wsdl'
        #wsdl = 'https://adepsep.apec.fr/v5/positions?wsdl'
        'recruiter_id': '100061739',
        'atsId': 1000050,
        'salt': 'bb5845db99f88fe182b910777f3d248170ce4343615582e48c52a61f85e1da951d87fa1ba99a557ec0f03839bc8af91c317849618f6d5b4e089f6744ae5be822',
        'password': '"Password123',
        'numeroDossier': '50021040i'
    }
    settings = None
    client = None
    client_raw = None
    store = None
    geocode = None
    mytransport = None

    def __init__(self, spider, raw_response=False):
        self.raw_response = raw_response
        env = spider.settings.get('APEC_ENV', 'PROD')
        account = spider.settings.get('APEC_ACCOUNT', None)
        if account is None or env == 'DEV':
            account = self.APEC_ACCOUNT
        self.recruiter_id = account['recruiter_id']
        self.atsId = account['atsId']
        self.salt = account['salt']
        self.password = account['password']
        self.numeroDossier = account['numeroDossier']
        self.wsdl = account['wsdl']
        self.store = spider.store
        self.geocode = spider.geocode
        self.create_clients(self.raw_response)
        pprint(account)

    def create_clients(self, raw_response):
        session = Session()
        session.verify = certifi.where()
        self.history = HistoryPlugin()
        transport = transports.Transport(session=session)
        self.mytransport = MyTransport()
        self.settings = Settings(strict=False, xml_huge_tree=True, raw_response=raw_response)
        self.client = Client(wsdl=self.wsdl, settings=self.settings, transport=transport, plugins=[self.history])
        self.client_raw = Client(wsdl=self.wsdl, settings=self.settings, transport=self.mytransport, plugins=[self.history])

    def get_autentication(self):
        atsPassword = hashlib.sha512(self.salt + self.password).hexdigest()
        authentication_type = self.client.get_type('ns2:AuthenticationType')
        authentication = authentication_type(atsId=self.atsId, numeroDossier=self.numeroDossier, atsPassword=atsPassword)
        return authentication

    def get_tracking_id(self):
        entity_id_type = self.client.get_type('ns1:EntityIdType')
        uniquePayloadTrackingId = entity_id_type(IdValue='-'.join([str(self.recruiter_id), str(int(time.time()*1000))]))
        return uniquePayloadTrackingId

    def get_list_recruiter_position_openings(self, return_raw_request=False):
        entity_id_type = self.client.get_type('ns1:EntityIdType')
        recruiterId = entity_id_type(IdValue=self.recruiter_id)
        if return_raw_request:
            res = self.client_raw.create_message(self.client.service, 'listRecruiterPositionOpenings', authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), recruiterId=recruiterId)
            return etree.tostring(res, pretty_print=True)
        res = self.client_raw.service.listRecruiterPositionOpenings(authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), recruiterId=recruiterId)
        return res

    def open_position(self, job, employer, return_raw_request=False):
        self.mytransport.headers = {'content-type': 'text/xml;charset=UTF-8', 'SOAPAction': 'http://adep.apec.fr/hrxml/sides/openPosition'}
        self.mytransport.xml_request = self.createOpenPositionRawRequest(job, employer)
        if return_raw_request:
            res = self.client_raw.create_message(self.client.service, 'openPosition', authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), position=xsd.SkipValue)
            return etree.tostring(res, pretty_print=True)
        try:
            res = self.client_raw.service.openPosition(authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), position=xsd.SkipValue)
        except Exception, ex:
            return {'error': ex.message}
        return res

    def update_position(self):
        pass

    def update_position_status(self, position_id, status, return_raw_request=False):
        entity_id_type = self.client.get_type('ns1:EntityIdType')
        clientPositionId = entity_id_type(IdValue=position_id)
        if return_raw_request:
            res = self.client.create_message(self.client.service, 'updatePositionStatus', authentication=self.get_autentication(),uniquePayloadTrackingId=self.get_tracking_id(), clientPositionId=clientPositionId, newPositionStatus=status)
            return etree.tostring(res, pretty_print=True)
        try:
            res = self.client.service.updatePositionStatus(authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), clientPositionId=clientPositionId, newPositionStatus=status)
        except Exception, ex:
            return {'error': ex.message}
        return res

    def get_position_status(self, position_id, return_raw_request=False):
        entity_id_type = self.client.get_type('ns1:EntityIdType')
        clientPositionId = entity_id_type(IdValue=position_id)
        if return_raw_request:
            res = self.client.create_message(self.client.service, 'getPositionStatus', authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), clientPositionId=clientPositionId)
            return etree.tostring(res, pretty_print=True)
        try:
            res = self.client.service.getPositionStatus(authentication=self.get_autentication(), uniquePayloadTrackingId=self.get_tracking_id(), clientPositionId=clientPositionId)
        except Exception, ex:
            return {'error': ex.message}
        return res

    def cleanhtml(self, raw_html):
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', raw_html)
        return cleantext

    def cut_nonvalid_tags(self, text):
        allowed_tags = 'strong|b|em|br|ul|ol|li|p|i|u|div|span'.split('|')
        all_tags_re = re.compile('<.*?>')
        all_tags = all_tags_re.findall(text)
        # pprint(all_tags)
        all_tags = map(lambda i: i.split(' ')[0].replace('<', '').replace('>', '').replace('/', ''), all_tags)
        # pprint(list(set(all_tags)))
        for tag in all_tags:
            if tag not in allowed_tags:
                if tag in ['table', 'tbody', 'thead', 'header', 'footer', 'nav', 'section', 'article', 'aside',
                           'address', 'figure', 'td', 'th', 'tr', 'img']:
                    text = re.sub("""<%s.*?>""" % (tag,), '', text)
                    text = re.sub("""<\/%s>""" % (tag,), '', text)
                else:
                    text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)
        return text

    def createOpenPositionRawRequest(self, job, employer):
        atsPassword = hashlib.sha512(self.salt + self.password).hexdigest()
        uniquePayloadTrackingId = '-'.join([str(self.recruiter_id), str(int(time.time()*1000))])
        order_type = 'ODD' # in [ODD, ODC]
        status_job = 'CADRE_PRIVE' # in [CADRE_PRIVE, CADRE_PUBLIC, CADRE_DE_MAITRISE]
        part_time = 'false'
        job_type = '1'
        profile_description = 'profile-description profile-description profile-description profile-description profile-description profile-description'
        position = """<ns2:staffingOrder xml:lang="fr">
                       <OrderId idOwner="CLIENT">
                          <IdValue>{id}</IdValue>
                       </OrderId>
                       <ReferenceInformation/>
                       <CustomerReportingRequirements/>
                       <OrderClassification orderStatus="new" orderType="{order_type}"/>
                       <OrderContact contactType="placed by">
                          <ContactInfo>
                             <PersonName/>
                             <ContactMethod>
                                <InternetWebAddress>{url}</InternetWebAddress>
                             </ContactMethod>
                          </ContactInfo>
                       </OrderContact>
                       <PositionQuantity>1</PositionQuantity>
                       <MultiVendorDistribution>true</MultiVendorDistribution>
                       <StaffingPosition>
                          <PositionHeader>
                             <PositionId>
                                <Id>{naf_code}</Id>
                                <Domain>
                                   <IdIssuer>INSEE</IdIssuer>
                                   <IdType>NAF</IdType>
                                </Domain>
                             </PositionId>
                             <PositionTitle>{title}</PositionTitle>
                             <PositionType>CDI</PositionType>
                             <PositionDescription><![CDATA[{description}]]></PositionDescription>
                             <Quantity>1</Quantity>
                          </PositionHeader>
                          <CustomerReportingRequirements/>
                          <PositionDateRange>
                             <StartDate>{created_at}</StartDate>
                          </PositionDateRange>
                          <PositionContact contactType="recipient">
                             <ContactInfo>
                                <PersonName/>
                                <ContactMethod>
                                   <InternetWebAddress>{url}</InternetWebAddress>
                                </ContactMethod>
                             </ContactInfo>
                          </PositionContact>
                          <Rates rateStatus="proposed" rateType="minPayRate">
                             <Amount currency="EUR" rateAmountPeriod="yearly">{salary[0]}</Amount>
                             <Class>regular</Class>
                             <StartDate>{created_at}</StartDate>
                          </Rates>
                          <Rates rateStatus="proposed" rateType="maxPayRate">
                             <Amount currency="EUR" rateAmountPeriod="yearly">{salary[1]}</Amount>
                             <Class>regular</Class>
                             <StartDate>{created_at}</StartDate>
                          </Rates>
                          <WorkSite>
                             <WorkSiteId>
                                <Id>{insee_code}</Id>
                                <Domain>
                                   <IdIssuer>INSEE</IdIssuer>
                                   <IdType>LOCATION_CODE</IdType>
                                </Domain>
                             </WorkSiteId>
                          </WorkSite>
                          <WorkSite>
                             <WorkSiteId>
                                <Id>AUCUN</Id>
                                <Domain>
                                   <IdIssuer>APEC</IdIssuer>
                                   <IdType>LOCATION_ZONE_DEPLACEMENT</IdType>
                                </Domain>
                             </WorkSiteId>
                          </WorkSite>
                          <StaffingShift>
                             <Id idOwner="CLIENT">
                                <IdValue>{employer_id}</IdValue>
                             </Id>
                          </StaffingShift>
                          <PositionRequirements interviewRequired="false" resumeRequired="false">
                             <Competency name="GLOBAL_EXPERIENCE_LEVEL">
                                <CompetencyEvidence>
                                   <StringValue>6</StringValue>
                                </CompetencyEvidence>
                             </Competency>
                          </PositionRequirements>
                       </StaffingPosition>
                       <UserArea>
                          <ns2:ProfileDescription><![CDATA[{profile_description}]]></ns2:ProfileDescription>
                          <ns2:PresentationDescription/>
                          <ns2:RecruitmentDescription/>
                          <ns2:OrganizationDescription>{organization_description}</ns2:OrganizationDescription>
                          <ns2:OrganizationName>{employer_name}</ns2:OrganizationName>
                          <ns2:DisplayLogoType>true</ns2:DisplayLogoType>
                          <ns2:StatusJob>{status_job}</ns2:StatusJob>
                          <ns2:DisplayedPay>3</ns2:DisplayedPay>
                          <ns2:PartTime>{part_time}</ns2:PartTime>
                          <ns2:JobType>{job_type}</ns2:JobType>
                       </UserArea>
                    </ns2:staffingOrder>
        """.format(id=str(job['id']), employer_name=employer['name'], employer_id=str(job['employer_id']),
                   title=job['title'][0:76],
                   description=self.get_description(job), url=job['url'], created_at=self.get_now(),
                   organization_description=self.get_company_description(employer),
                   profile_description=profile_description,
                   order_type=order_type, status_job=status_job, part_time=part_time,
                   salary=self.get_salary(job), insee_code=self.get_insee_code(job),
                   naf_code=self.get_naf_code(job), job_type=job_type
                   )

        body = """<?xml version="1.0" encoding="utf-8" ?>
        <S:Envelope xmlns:S="http://schemas.xmlsoap.org/soap/envelope/">
          <S:Body>
            <ns2:openPositionRequest xmlns:ns2="http://adep.apec.fr/hrxml/sides" xmlns="http://ns.hr-xml.org/2006-02-28">
              <ns2:authentication>
               <ns2:atsId>%s</ns2:atsId>
               <ns2:atsPassword>%s</ns2:atsPassword>
                <ns2:numeroDossier>%s</ns2:numeroDossier>
              </ns2:authentication>
              <ns2:uniquePayloadTrackingId idOwner="CLIENT">
                <IdValue>%s</IdValue>
              </ns2:uniquePayloadTrackingId>
              <ns2:position>
               %s
              </ns2:position>
            </ns2:openPositionRequest>
          </S:Body>
        </S:Envelope>
        """ % (str(self.atsId), atsPassword, self.numeroDossier, uniquePayloadTrackingId, position)
        return body

    def get_company_description(self, employer):
        description = self.store.get_company_description(employer)
        description = self.cleanhtml(description)[0:3000]
        if (len(description) < 100):
            description = ' '.join([description, '.' * (100-len(description))])
        return description

    def get_naf_code(self, job):
        if 'category' in job['attributes']:
            if int(job['attributes']['category']) in category2naf:
                return category2naf[int(job['attributes']['category'])][1]
            else:
                return '9499Z'

    def get_description(self, job):
        description = self.cut_nonvalid_tags(job['description'])
        description, errors = tidy_fragment(description[0:2900], options={'numeric-entities':1})
        if (len(description) < 200):
            description = ' '.join([description, '.' * (201-len(description))])
        return description

    def get_salary(self, job):
        default = [20000, 100000]
        if 'salary' in job['attributes']:
            salary = map(lambda i: int(i.strip().replace('.', '')), job['attributes']['salary'].split('-'))
            if len(salary) == 0:
                return default
            if len(salary) < 2:
                salary.append(salary[0]+1000)
            return salary
        return default

    def get_insee_code(self, job):
        default = '75101'
        attributes = job['attributes']
        if 'selectedCountry' in attributes:
            if 'countryCode' in attributes['selectedCountry']:
                if attributes['selectedCountry']['countryCode'] == 'FR':
                    if job['city'] is not None and len(job['city']) > 0:
                        code = self.geocode.city2insee_code(job['city'].strip())
                        if code is not None:
                            return code
        return default

    def get_now(self):
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(time.time())))






