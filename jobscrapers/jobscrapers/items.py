# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import Join, MapCompose, TakeFirst
from w3lib.html import remove_tags


class JobItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    name = scrapy.Field()
    referencenumber = scrapy.Field()
    location = scrapy.Field()
    category = scrapy.Field()
    date = scrapy.Field(serializer=str)
    contract = scrapy.Field()
    # description = scrapy.Field(input_processor=MapCompose(remove_tags))
    description = scrapy.Field()
    utime = scrapy.Field(serializer=str)
    country = scrapy.Field()
    experience = scrapy.Field()

class PwcrecruitJob(JobItem):
    experience = scrapy.Field()

class ReseauallianceJob(JobItem):
    city = scrapy.Field()


class SocietegeneraleJob(JobItem):
    businessunit = scrapy.Field()


class LorealJob(JobItem):
    country = scrapy.Field()


class XtramileJob(scrapy.Item):
    country = scrapy.Field()
    city = scrapy.Field()
    location = scrapy.Field()
    url = scrapy.Field()
    category = scrapy.Field()
    title = scrapy.Field()
    description = scrapy.Field()
    referencenumber = scrapy.Field()
    date = scrapy.Field()
    company = scrapy.Field()
    job_type = scrapy.Field()

class PernodricardJob(JobItem):
    id = scrapy.Field()
    company = scrapy.Field()
    description = scrapy.Field()
    date = scrapy.Field()
    education = scrapy.Field()
    travel = scrapy.Field()
    job_type = scrapy.Field()

class SwissreJob(JobItem):
    job_type = scrapy.Field()
    id = scrapy.Field()

class OnepointJob(JobItem):
    id = scrapy.Field()

class LynxJob(JobItem):
    id = scrapy.Field()
    salary = scrapy.Field()

class HaysJob(scrapy.Item):
    title = scrapy.Field()
    subtitle = scrapy.Field()
    description = scrapy.Field()
    industry = scrapy.Field()
    name = scrapy.Field()


annotations_list = [
    'company',
    'position',
    'mission',
    'experience',
    'experience_duration',
    'education',
    'city',
    'country',
    'postal_code',
    'contrat_type',
    'contract_duration',
    'position_scheduled',
    'hard_skills',
    'soft_skills',
    'salary'
]

class SynergieItem(scrapy.Item):
    url = scrapy.Field()
    title = scrapy.Field()
    name = scrapy.Field()
    referencenumber = scrapy.Field()
    location = scrapy.Field()
    location_l = scrapy.Field()
    category = scrapy.Field()
    category_l = scrapy.Field()
    date = scrapy.Field(serializer=str)
    contract = scrapy.Field()
    # description = scrapy.Field(input_processor=MapCompose(remove_tags))
    description = scrapy.Field()
    utime = scrapy.Field(serializer=str)
    country = scrapy.Field()
    experience = scrapy.Field()
    experience2 = scrapy.Field()
    experience_l = scrapy.Field()
    experience2_l = scrapy.Field()
    city = scrapy.Field()
    city2 = scrapy.Field()
    city_l = scrapy.Field()
    postal_code = scrapy.Field()
    postal_code_l = scrapy.Field()
    postal_code2 = scrapy.Field()
    ref = scrapy.Field()
    job_type = scrapy.Field()
    job_type_l = scrapy.Field()
    salary = scrapy.Field()
    salary_l = scrapy.Field()
    education = scrapy.Field()
    education_l = scrapy.Field()
    start_time = scrapy.Field()
    start_time_l = scrapy.Field()
    end_time = scrapy.Field()
    end_time_l = scrapy.Field()
    desc0 = scrapy.Field()
    desc1 = scrapy.Field()
    number2 = scrapy.Field()
    text1 = scrapy.Field()
    desc2 = scrapy.Field()
    desc3 = scrapy.Field()
    desc4 = scrapy.Field()
    desc0_l = scrapy.Field()
    desc1_l = scrapy.Field()
    desc2_l = scrapy.Field()
    desc3_l = scrapy.Field()
    desc4_l = scrapy.Field()
    company = scrapy.Field()
    mission = scrapy.Field()
    mission_l = scrapy.Field()
    info = scrapy.Field()
    index = scrapy.Field()
    title2 = scrapy.Field()
    position_scheduled = scrapy.Field()
    desc5 = scrapy.Field()
    desc6 = scrapy.Field()
    desc7 = scrapy.Field()
    desc8 = scrapy.Field()
    desc9 = scrapy.Field()
    desc10 = scrapy.Field()
    app_crit = scrapy.Field()
    experience_duration = scrapy.Field()
    hard_skills = scrapy.Field()
    hard_skills2 = scrapy.Field()
    hard_skills3 = scrapy.Field()
    id = scrapy.Field()
    id_l = scrapy.Field()
    company_desription = scrapy.Field()
    company_desription_l = scrapy.Field()
    candidat_description = scrapy.Field()
    candidat_description_l = scrapy.Field()

    
class SpieItem(scrapy.Item):
    name = scrapy.Field()
    title = scrapy.Field()
    title1 = scrapy.Field()
    title3 = scrapy.Field()
    title7 = scrapy.Field()
    company2 = scrapy.Field()
    expired_l = scrapy.Field()
    expired = scrapy.Field()
    skills = scrapy.Field()
    skill_l = scrapy.Field()
    number = scrapy.Field()
    location2 = scrapy.Field()
    education = scrapy.Field()
    city2 = scrapy.Field()
    index = scrapy.Field()
    info = scrapy.Field()
    company = scrapy.Field()
    title6 = scrapy.Field()
    ref = scrapy.Field()
    title8 = scrapy.Field()
    desc1 = scrapy.Field()
    desc2 = scrapy.Field()
    title2_l = scrapy.Field()
    title3_1 = scrapy.Field()
    company3 = scrapy.Field()
    title2 = scrapy.Field()
    gen_info = scrapy.Field()
    info_l = scrapy.Field()
    desc_header = scrapy.Field()
    title4_l = scrapy.Field()
    title4 = scrapy.Field()
    title5_l = scrapy.Field()
    title5 = scrapy.Field()
    desc1_l = scrapy.Field()
    desc1_2 = scrapy.Field()
    desc2_l = scrapy.Field()
    word = scrapy.Field()
    definition = scrapy.Field()
    synonyms = scrapy.Field()
    text = scrapy.Field()
    number2 = scrapy.Field()
    contrat_type_l = scrapy.Field()
    contrat_type = scrapy.Field()
    position_scheduled_l = scrapy.Field()
    position_scheduled = scrapy.Field()
    contract_duration_l = scrapy.Field()
    contract_duration = scrapy.Field()
    category_l = scrapy.Field()
    category = scrapy.Field()
    mission_l = scrapy.Field()
    mission = scrapy.Field()
    experience_l = scrapy.Field()
    experience = scrapy.Field()
    location = scrapy.Field()
    city_l = scrapy.Field()
    city = scrapy.Field()
    app_crit = scrapy.Field()
    experience_duration_l = scrapy.Field()
    experience_duration = scrapy.Field()
    education_l = scrapy.Field()
    hard_skills_l = scrapy.Field()
    hard_skills = scrapy.Field()
    hard_skills2_l = scrapy.Field()
    hard_skills2 = scrapy.Field()
    langs_l = scrapy.Field()
    langs = scrapy.Field()
    langs2_l = scrapy.Field()
    langs2 = scrapy.Field()
    industry = scrapy.Field()
    ambition = scrapy.Field()
    ambition_l = scrapy.Field()
    location_l = scrapy.Field()


class JobItem2(scrapy.Item):
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
    text = scrapy.Field()

class GermanpersonalItem(scrapy.Item):
    PositionRecordInfo = scrapy.Field()
    PositionPostings = scrapy.Field()
    PositionProfile = scrapy.Field()
    original_url = scrapy.Field()
    title = scrapy.Field()
    external_id = scrapy.Field()
    category = scrapy.Field()
    category_code = scrapy.Field()


class LeboncoinItem(scrapy.Item):
    name = scrapy.Field()
    company = scrapy.Field()
    title = scrapy.Field()
    desc = scrapy.Field()
    desc_l = scrapy.Field()
    contrat_type = scrapy.Field()
    category = scrapy.Field()
    jobduty = scrapy.Field()
    experience_duration = scrapy.Field()
    education = scrapy.Field()
    position_scheduled = scrapy.Field()
    location = scrapy.Field()
    criteries = scrapy.Field()
    contrat_type_l = scrapy.Field()
    category_l = scrapy.Field()
    jobduty_l = scrapy.Field()
    experience_duration_l = scrapy.Field()
    education_l = scrapy.Field()
    position_scheduled_l = scrapy.Field()
    location_l = scrapy.Field()
    criteries_l = scrapy.Field()
    salary = scrapy.Field()
    text = scrapy.Field()
    itemtype = scrapy.Field()

class ReannotationItem(scrapy.Item):
    spidername = scrapy.Field()
    spiderdirname = scrapy.Field()
    list_of_files = scrapy.Field()
    source_dir = scrapy.Field()

class IndeedItem(scrapy.Item):
    name = scrapy.Field()
    text = scrapy.Field()
    png = scrapy.Field()
    html = scrapy.Field()
    industry = scrapy.Field()
    body = scrapy.Field()

class PlainItem(scrapy.Item):
    name = scrapy.Field()
    text = scrapy.Field()
    subfolder = scrapy.Field()
    industry = scrapy.Field()
    itemtype = scrapy.Field()

categories = [
            'Accounting & Finance',
            'IT',
            'Sales',
            'Customer Services',
            'Engineering',
            'HR & Recruitment',
            'Healthcare & Nursing',
            'Hospitality & Catering',
            'PR, Advertising & Marketing',
            'Logistics & Warehouse',
            'Teaching, Training & Scientific',
            'Trade & Construction',
            'Admin',
            'Legal',
            'Culture & Medias',
            'Graduate',
            'Retail',
            'Consultancy',
            'Manufacturing & Craftsmanship',
            'Agriculture & Environmental',
            'Social work',
            'Travel',
            'Energy, Oil & Gas',
            'Property',
            'Charity & Voluntary',
            'Domestic help & Cleaning',
            'Installation & Maintenance',
            'Part time',
            'Defence',
            'Other/General',
            'Chartered accountancy',
            'Logistics',
            'Setting/Control',
            'Automation/Robotics',
            'Drawing/Studies',
            'Electrical',
            'Maintenance',
            'Mounting/Assembly'
        ]

class MonsterItem(scrapy.Item):
    name = scrapy.Field()
    title = scrapy.Field()
    index = scrapy.Field()
    text = scrapy.Field()
    info = scrapy.Field()
    company = scrapy.Field()
    ref = scrapy.Field()
    published_l = scrapy.Field()
    resume_title = scrapy.Field()
    desc1 = scrapy.Field()
    title2_l = scrapy.Field()
    title2 = scrapy.Field()
    contrat_type_l = scrapy.Field()
    itemtype = scrapy.Field()
    position2 = scrapy.Field()
    contrat_type = scrapy.Field()
    position_scheduled_l = scrapy.Field()
    position_scheduled = scrapy.Field()
    contract_duration_l = scrapy.Field()
    published = scrapy.Field()
    criteries_title = scrapy.Field()
    criteries = scrapy.Field()
    description = scrapy.Field()
    contract_type = scrapy.Field()
    contract_duration = scrapy.Field()
    contract_type_l = scrapy.Field()
    ref_l = scrapy.Field()
    category_l = scrapy.Field()
    category = scrapy.Field()
    mission_l = scrapy.Field()
    mission = scrapy.Field()
    other = scrapy.Field()
    experience_l = scrapy.Field()
    experience = scrapy.Field()
    location = scrapy.Field()
    city_l = scrapy.Field()
    city = scrapy.Field()
    app_crit = scrapy.Field()
    experience_duration_l = scrapy.Field()
    experience_duration = scrapy.Field()
    education_l = scrapy.Field()
    education = scrapy.Field()
    hard_skills_l = scrapy.Field()
    hard_skills = scrapy.Field()
    hard_skills2_l = scrapy.Field()
    hard_skills2 = scrapy.Field()
    langs_l = scrapy.Field()
    langs = scrapy.Field()
    langs2_l = scrapy.Field()
    langs2 = scrapy.Field()
    industry = scrapy.Field()
    contactname = scrapy.Field()
    industries = scrapy.Field()
    phone = scrapy.Field()
    posted = scrapy.Field()
    referencecode = scrapy.Field()
    fax = scrapy.Field()
    contactname_l = scrapy.Field()
    industries_l = scrapy.Field()
    phone_l = scrapy.Field()
    posted_l = scrapy.Field()
    referencecode_l = scrapy.Field()
    fax_l = scrapy.Field()
    duration = scrapy.Field()
    png = scrapy.Field()
    position = scrapy.Field()
    postal_code = scrapy.Field()
    desc = scrapy.Field()
    salary_l = scrapy.Field()
    salary = scrapy.Field()
    summary = scrapy.Field()
    summary_l = scrapy.Field()
    location_l = scrapy.Field()
    job_type_l = scrapy.Field()
    job_type = scrapy.Field()
    address_l = scrapy.Field()
    address = scrapy.Field()
    other0 = scrapy.Field()
    other1 = scrapy.Field()
    other2 = scrapy.Field()
    other3 = scrapy.Field()
    other4 = scrapy.Field()
    other5 = scrapy.Field()
    other6 = scrapy.Field()
    other7 = scrapy.Field()
    other8 = scrapy.Field()
    other11 = scrapy.Field()
    other1_l = scrapy.Field()
    other2_l = scrapy.Field()
    other3_l = scrapy.Field()
    other4_l = scrapy.Field()
    other5_l = scrapy.Field()
    other6_l = scrapy.Field()
    other7_l = scrapy.Field()
    other8_l = scrapy.Field()
    other11_l = scrapy.Field()
    other0_l = scrapy.Field()
    poste = scrapy.Field()
    region = scrapy.Field()
    age1 = scrapy.Field()
    age2 = scrapy.Field()
    footer = scrapy.Field()
    side_title = scrapy.Field()
    side_title1 = scrapy.Field()
    side_title2 = scrapy.Field()
    side_title3 = scrapy.Field()
    lang_title = scrapy.Field()
    lang = scrapy.Field()
    exp_title = scrapy.Field()
    exp = scrapy.Field()
    permis_title = scrapy.Field()
    permis = scrapy.Field()
    type_title = scrapy.Field()
    type = scrapy.Field()
    contract_type_title = scrapy.Field()
    website = scrapy.Field()
    country = scrapy.Field()


categories2 = [
    u'comptabilité',
    u'banque-finance-assurances',
    u'informatique-internet',
    u'commerce et distribution',
    u'industrie',
    u'immobilier',
    u'ressources humaines',
    u'tourisme et restauration',
    u'marketing',
    u'cours et formation',
    u'BTP',
    u'juridique',
    u'culture et médias',
    u'production industrielle',
    u'agriculture et environnement',
    u'santé-social',
    u'transport-logistique',
    u'Énergie pétrole et gaz',
    u'propriété intellectuelle',
    u'bénévole',
    u'service à la personne',
    u'installation et maintenance',
    u'sécurité-armée'
]


'''  Onisep data json
{
        "id":"81100",
        "label": "gestionnaire de contrats d'assurance",
        "alt_label": [
            "conseille/ère relation client",
            "gestionnaire contrats à distance",
            "télé-gestionnaire"
        ],
        "description": "Vol, incendie, accidents... Le gestionnaire de contrats d'assurances est l'interlocuteur privilégié des assurés, qu'il accompagne de l'établissement du contrat jusqu'à la réparation du dommage. Il intervient aussi pour indemniser en cas de sinistre.",
        "activities": [
            {
                "act_title": "Établir un contrat sur mesure",
                "act_description": "Que ce soit pour assurer un appartement, une voiture ou souscrire une assurance-vie, il établit le contrat d'assurance et l'adapte à la situation de chaque client. Après avoir étudié sa demande, il soumet une proposition de garantie adaptée aux risques à couvrir. Puis il ouvre le contrat, le codifie, l'enregistre et le tarifie. Il peut aussi le modifier, si besoin. Pour les risques dits classiques (véhicule accidenté, dégât des eaux...), le gestionnaire se réfère à des contrats préétablis et applique des clauses-types. Si le risque assuré est plus complexe (risque industriel, construction...), il rédige des clauses particulières. Il établit également les appels à cotisations et encaisse les versements des clients."
            },
            {
                "act_title": "Indemniser les assurés",
                "act_description": "Dégât des eaux, accident ou vol de voiture... ce gestionnaire doit gérer, parfois dans l'urgence, divers sinistres. Il dépêche un expert sur les lieux, étudie son rapport et vérifie que les garanties prévues par le contrat permettent d'indemniser l'assuré, selon sa part de responsabilité (était-il en tort ? y a-t-il eu une négligence ?). Il peut aussi prendre en charge la réparation du sinistre : par exemple, lors d'un dégât des eaux, c'est lui qui envoie sur place des artisans pour remettre en état la pièce endommagée."
            }

        ],
        "educ_min": "bac+2",
        "status": "statut salarié",
        "sector": [
            "banques- assurances"
        ],
        "competencies":[{
            "comp_title":"Rigueur scientifique",
            "comp_descr": "Une clause spécifique mal rédigée ? La responsabilité de la société d'assurances est alors engagée pour des sommes importantes. Proposer une garantie, établir un devis, évaluer un dommage : toutes les étapes de ce métier demandent une grande rigueur, et des capacités d'analyse et de synthèse. Le gestionnaire doit également être à l'aise avec l'informatique."
        },
        {
            "comp_title":"Compétences juridiques",
            "comp_descr":"Parce qu'il n'y a pas un, mais plusieurs types de contrats d'assurances, le gestionnaire doit parfaitement connaître les produits que propose son entreprise pour répondre à la demande des clients. Des compétences juridiques sont indispensables pour gérer les procédures d'indemnisation et les contentieux."
            }],
        "educations":[
            {"educ_level":"bac + 2",
            "educ_labels":["BTS assurance","DUT carrières juridiques"]},
            {"educ_level":"bac + 3",
            "educ_labels":["Licence pro assurance"]},
            {"educ_level":"bac + 5",
            "educ_labels":["Master droit des assurances ; monnaie, banque, finance, assurance ; finance"]}
        ],
        "formations":[
            {"form_level":"bac + 2",
            "form_title":["BTS Assurance","BTS Management commercial opérationnel (ex-BTS MUC)","BTS Négociation et digitalisation de la relation client (ex BTS négociation et relation client)"]},
            {"form_level":"bac + 3",
            "form_title":["Licence pro assurance, banque, finance spécialité gestion juridique des contrats d'assurance","Licence pro assurance, banque, finance : supports opérationnels"]},
            {"form_level":"bac + 5",
            "form_title":["Master droit des assurances","Master monnaie, banque, finance, assurance"]}
        ]
    },
'''


onisep_data_template = {
        "id": None,
        "label": None,
        "alt_label": [],
        "description": None,
        "activities": [],
        "educ_min": None,
        "status": "statut salarié",
        "sector": [],
        "competencies":[],
        "educations":[],
        "formations":[]
    }

onisep_act_template = {
        "act_title": "",
        "act_description": ""
    }

onisep_comp_template = {
        "comp_title":"",
        "comp_descr": ""
    }


onisep_education_template = {
    "educ_level": "",
    "educ_labels": []
}

onisep_formation_template = {
    "form_level": "",
    "form_title": []
}

wood_jobs = [
    u"Agenceur",
    u"Agent Forestier",
    u"Bucheron",
    u"Chargé d'affaires",
    u"Chargé d'approvisionnement",
    u"Chargé d'études",
    u"Charpentier",
    u"Chauffeur grumier",
    u"Chef d'équipe",
    u"Commis forestier",
    u"Conducteur d'engin forestier",
    u"Constrcuteur Bois",
    u"Constructeur bois",
    u"Designer",
    u"Ebéniste",
    u"Encadreur",
    u"Entrepreneur de travaux forestiers",
    u"Expert forestier",
    u"Finisseur / vernisseur",
    u"Marqueteur",
    u"Menuisier",
    u"Métreur",
    u"Opérateur de scierie",
    u"Opérateur de sylviculture-reboisement",
    u"Pilote de scie",
    u"Responsable Affutage",
    u"Responsable de production",
    u"Responsable recherche et développement",
    u"Sculpteur sur bois",
    u"technicien forestier",
    u"Technico-commercial / Produits Bois",
    u"Tonnelier",
    u"Tourneur sur bois"
]
