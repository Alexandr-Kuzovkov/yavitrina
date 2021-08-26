# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

feed2itemMap = {
    'job_external_id': 'external_id',
    'url': 'url',
    'title': 'title',
    'city': 'city',
    'state': 'state',
    'country': 'country',
    'description': 'description',
    'job_type': 'job_type',
    'company': 'company',
    'category': 'category',
    'posted_at': 'posted_at',
    'expire_date': 'expire_date'
}


class JobItem(scrapy.Item):
    uuid = scrapy.Field()
    uid = scrapy.Field()
    external_id = scrapy.Field()
    external_unique_id = scrapy.Field()
    external_uid = scrapy.Field()
    employer_id = scrapy.Field()
    job_group_id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    city = scrapy.Field()
    state = scrapy.Field()
    country = scrapy.Field()
    description = scrapy.Field()
    job_type = scrapy.Field()
    company = scrapy.Field()
    category = scrapy.Field()
    posted_at = scrapy.Field()
    expire_at = scrapy.Field()
    status = scrapy.Field()
    budget = scrapy.Field()
    budget_spent = scrapy.Field()
    created_at = scrapy.Field()
    updated_at = scrapy.Field()
    expire_date = scrapy.Field()
    slug = scrapy.Field()
    company_slug = scrapy.Field()
    attributes = scrapy.Field()
    keywords = scrapy.Field()
    is_editable = scrapy.Field()


class GenericItem(scrapy.Item):
    id = scrapy.Field
    updated_at = scrapy.Field
    created_at = scrapy.Field

jobLeadsJobGroups = {
    'Germany': {30: 111, 1: 129, 3: 131, 6: 127, 9: 128, 14: 130, 2: 140, 5: 141},
    'Netherlands': {1: 114, 2: 113, 3: 116, 6: 112, 9: 115, 30: 117},
    'United Kingdom': {1: 95, 2: 98, 3: 102, 4: 97, 5: 99, 6: 96, 9: 100, 10: 101, 13: 29, 14: 103, 30: 93},
    'United States': {1: 122, 2: 123, 3: 119, 5: 118, 6: 125, 9: 120, 30: 121},
    'Switzerland': {1: 135, 2: 136, 3: 133, 6: 137, 9: 134, 14: 138, 30: 132, 5: 142},
    'Austria': {1: 183, 2: 184, 3: 185, 5: 186, 6: 187, 9: 188, 30: 189},
    'France': {1: 175, 2: 176, 3: 177, 5: 178, 6: 179, 9: 180, 14: 181, 30: 182}
}

jobLeadsMapCountries = {
    'Germany': 'Deutschland',
    'Schweiz': 'Switzerland',
    u'Österreich': 'Austria'
}

jobLeadsMapCountriesRev = {
    'Deutschland': 'Germany',
    'U.A.E': 'United Arab Emirates'
}

jobLeadsCategoryMap = {
    u'Management & Opérations': 30,
    u'Ventes': 3,
    u'Ressources humaines': 6,
    u'Droit': 14,
    u'Informatique & Technologie': 2,
    u'Génie et Technique': 5,
    u'Bio & Pharma & Health': 7,
    u'Consulting / Beratung': 18,
    u'Consulting': 18,
    u'Other': 30,
    u'Bio & Pharmacology & Health': 7
}

GeneralCategoryMap = {
    u'Chef & Cook': 8,
    u'Waiter or Waitress': 8,
    u'Events & Promotion': 9,
    u'Driver & Courier': 10,
    u'Office & Admin': 13,
    u'Barista & Bartender': 8,
    u'Waiteror Waitress': 8,
    None: 30,
    u'Kitchenporter': 8,
    u'Careworkers & Health': 7,
    u'Beauty & Wellness': 7,
    u'Other': 30,
    u'Education': 11,
    u'Cleaning': 26,
    u'Sales & Marketing': 3,
    u'Warehouse': 10,
    u'Construction': 12,
    u'Retail': 17,
    u'Waiter orWaitress': 8,
    u'Kitchen porter': 8
}

TataCategoryMap = {
    u'FINANCE': 1,
    u'CONSULTANCY': 18,
    u'MARKETING AND SALES': 3,
    u'BUSINESS PROCESS SERVICES': 13,
    u'IT INFRASTRUCTURE SERVICES': 2,
    u'TECHNOLOGY': 5
}


germanpersonalCategories = {
    155: [u'Montage/Inbetriebnahme', 27],
    157: [u'Ingenieurswesen', 5],
    156: [u'Kundendienst/Service/CallCenter', 4],
    159: [u'Projektmanagement', 13],
    158: [u'Technische Berufe', 5],
    50: [u'Forschung u. Entwicklung (F&E)', 11],
    115: [u'Erziehung/Bildung/Therapie', 11],
    116: [u'Land-/Forstwirtschaft', 20],
    69: [u'Medienproduktion/Medientechnik (Film, ], Funk, ], Fernsehen, ], Verlag)', 15],
    48: [u'Hilfskraft/Aushilfe/Hilfst\xe4tigkeit', 26],
    49: [u'Produktion/Produktionsplanung', 19],
    46: [u'Sekretariat/Assistenz/Office Management', 4],
    47: [u'Top Management/Gesch\xe4ftsf\xfchrung', 13],
    44: [u'Organisation/Projekte/Beratung', 18],
    45: [u'Qualit\xe4tsmanagement (Produkt, ], Prozess, ], Kontrolle etc.)', 33],
    42: [u'Recht', 14],
    43: [u'Informationstechnologie (IT)', 2],
    40: [u'Vertrieb u. Verkauf', 3],
    41: [u'Verwaltung/Dienstleistung', 33],
    999: [u'Sonstige T\xe4tigkeitsbereiche', 30],
    70: [u'Hotel/Gastronomie/Kantine', 8],
    160: [u'Banking und Finanzdienstleistung', 1],
    161: [u'Sozialwesen/Pflege', 21],
    162: [u'Sonstige kaufm\xe4nnische Berufe', 30],
    163: [u'Sonstige gewerbliche Berufe', 30],
    39: [u'Marketing/Kommunikation/Werbung', 9],
    38: [u'Logistik u. Materialwirtschaft (Einkauf, ], Lager, ], Transport v. G\xfcter u. Personen)', 10],
    57: [u'Medizin/Pharma/Pflege', 7],
    56: [u'Handwerk', 19],
    51: [u'Elektrik/Elektronik/Elektrotechnik', 36],
    36: [u'Personalwesen/HR', 6],
    53: [u'Controlling/Finanz- und Rechnungswesen', 1],
    52: [u'Umwelt/Verkehrspolitik/Energie', 23]
}





'''
29	Admin Jobs	13
93	Other/General Jobs	30
95	Accounting & Finance Jobs	1
96	HR & Recruitment Jobs	6
97	Customer Services Jobs	4
98	IT Jobs	2
99	Engineering Jobs	5
100	PR, Advertising & Marketing Jobs	9
101	Logistics & Warehouse Jobs	10
102	Sales Jobs	3
103	Legal Jobs	14
111	Other/General Jobs - Germany	30
112	HR & Recruitment Jobs - Netherlands	6
113	IT jobs -Netherlands	2
114	Accounting & Finance- Netherlands	1
115	PR, Advertising & Marketing Jobs - Netherlands	9
116	Sales jobs - Netherlands	3
117	Other/General Jobs - Netherlands	30
118	Engineering- US	5
119	Sales Jobs - US	3
120	PR, Advertising & Marketing Jobs - US	9
121	Other/General Jobs - US	30
122	Accounting & Finance - US	1
123	IT Jobs -US	2
125	HR & Recruitment Jobs - US	6
127	HR & Recruitment Jobs - Germany	6
128	PR, Advertising & Marketing Jobs - Germany	9
129	Accounting & Finance Jobs - Germany	1
130	Legal Jobs - Germany	14
131	Sales Jobs - Germany	3
132	Other/General Jobs - Switzerland	30
133	Sales Jobs - Switzerland	3
134	PR, Advertising & Marketing Jobs - Switzerland	9
135	Accounting & Finance Jobs - Switzerland	1
136	IT jobs - Switzerland	2
137	HR & Recruitment Jobs - Switzerland	6
138	Legal Jobs - Switzerland	14
140	IT jobs -Germany	2
141	Engineering Jobs - Germany	5
142	Engineering Jobs - Switzerland	5
175	Accounting & Finance Jobs - FR	1
176	IT Jobs - FR	2
177	Sales Jobs - FR	3
178	Engineering Jobs - FR	5
179	HR & Recruitment Jobs - FR	6
180	PR, Advertising & Marketing Jobs - FR	9
181	Legal Jobs - FR	14
182	Other/General Jobs - FR	30
183	Accounting & Finance Jobs - AT	1
184	IT Jobs - AT	2
185	Sales Jobs - AT	3
186	Engineering Jobs - AT	5
187	HR & Recruitment Jobs - AT	6
188	PR, Advertising & Marketing Jobs - AT	9
189	Other/General Jobs - AT	30
'''