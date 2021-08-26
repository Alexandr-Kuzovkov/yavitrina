#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import hashlib
import time
from pprint import pprint
from pg import PgSQLStore
import re

recruiter_id = '100061739'
atsId = 1000050
salt = 'bb5845db99f88fe182b910777f3d248170ce4343615582e48c52a61f85e1da951d87fa1ba99a557ec0f03839bc8af91c317849618f6d5b4e089f6744ae5be822'
password = '"Password123'
numeroDossier = '50021040i'
atsPassword = hashlib.sha512(salt + password).hexdigest()
uniquePayloadTrackingId = '-'.join([str(recruiter_id), str(int(time.time()))])
db = {'dbname': 'xtramile_prod', 'dbport':5400, 'dbhost': 'pg.xtramile.io', 'dbuser': 'postgres', 'dbpass': 'Y+Bbtw3JRP7LllwWHWPdfECi'}

pg = PgSQLStore(db)
jobs = pg._get('jobs', None, 'id=%s', [15084])
job = jobs[0]

url = "https://testadepsides.apec.fr/v5/positions"
#headers = {'content-type': 'application/soap+xml'}
headers = {'content-type': 'text/xml;charset=UTF-8', 'SOAPAction': 'http://adep.apec.fr/hrxml/sides/openPosition'}


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext


position = """<ns2:staffingOrder xml:lang="fr">
               <OrderId idOwner="CLIENT">
                  <IdValue>{id}</IdValue>
               </OrderId>
               <ReferenceInformation/>
               <CustomerReportingRequirements/>
               <OrderClassification orderStatus="new" orderType="ODD"/>
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
                        <Id>8211Z</Id>
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
                     <StartDate>2018-10-05</StartDate>
                  </Rates>
                  <WorkSite>
                     <WorkSiteId>
                        <Id>35238</Id>
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
                  <ns2:StatusJob>CADRE_PRIVE</ns2:StatusJob>
                  <ns2:DisplayedPay>3</ns2:DisplayedPay>
                  <ns2:PartTime>false</ns2:PartTime>
                  <ns2:JobType>1</ns2:JobType>
               </UserArea>
            </ns2:staffingOrder>
""".format(id=str(job['id'])+'/7', employer_name=job['company_slug'], employer_id=str(job['employer_id']), title=job['title'],
           description=cleanhtml(job['description'])[0:3000], url=job['url'], created_at=str(job['created_at']),
           organization_description='organization-scription organization-scription organization-scription organization-scription organization-scription organization-scription',
           profile_description='profile-description profile-description profile-description profile-description profile-description profile-description',
           salary=(32000, 37000)
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
""" % (str(atsId), atsPassword, numeroDossier, uniquePayloadTrackingId, position)

for line in body.split('\n'):
    pprint(line)

response = requests.post(url, data=body, headers=headers)
pprint(response.content)
