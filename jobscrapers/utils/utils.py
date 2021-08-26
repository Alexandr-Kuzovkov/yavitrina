#!/usr/bin/env python

from pg import PgSQLStore
from pprint import pprint
from mylogger import logger
import time
import requests
import certifi
import json
import sys
import yaml
from terminaltables import SingleTable


db = {'dbname': 'xtramile_dev', 'dbport':5432, 'dbhost': 'api.xtramile.tech', 'dbuser': 'xtramile', 'dbpass': 'devPLK-012Yuzxcwdfejjk#$6^dfdsf7'}
#db = {'dbname': 'xtramile_prod', 'dbport':5432, 'dbhost': 'platform-xtramile.postgres.database.azure.com', 'dbuser': 'xt_main@platform-xtramile', 'dbpass': 'zxcjJAQ98><xNb3610292UpklLQghj*&!@#$%^&azxcvyrtRR'}
db2 = {'dbname': 'xtramile_prod', 'dbport':5400, 'dbhost': 'pg.xtramile.io', 'dbuser': 'postgres', 'dbpass': 'Y+Bbtw3JRP7LllwWHWPdfECi'}



#pg = PgSQLStore(db)
#res = pg._get('x_ranking_candidates', None, 'id=%s', [1000000000063])

#res = pg._get('candidates', None, 'id=%s', [2970])
#pprint(res[0]['attributes']['joinvision'])

#skills = open('/home/user1/Documents/JOB/tasks/domains/EducationType_DE.csv', 'r').read().strip()
#out = map(lambda i: i.split(';')[1], skills.split('\n'))
#print out

#-------------------------------------------------------------------------

def run_query():
    if len(sys.argv) > 3:
        db = globals()[sys.argv[2]]
        sql = sys.argv[3]
        pg = PgSQLStore(db)
        res = pg.run(sql)
        pprint(res)
    else:
        print 'Usage: %s %s %s %s' % (sys.argv[0], 'run_query', 'db', 'sql')

def exec_query():
    if len(sys.argv) > 3:
        db = globals()[sys.argv[2]]
        sql = sys.argv[3]
        pg = PgSQLStore(db)
        res = pg.execute(sql)
        pprint(res)
    else:
        print 'Usage: %s %s %s %s' % (sys.argv[0], 'exec_query', 'db', 'sql')

#Update jobs->attributes->category value for some jobs
def update_job_attributes():
    pg = PgSQLStore(db)
    where = "(attributes->>'category')::INTEGER=%s and (employer_id=%s OR employer_id IN (SELECT id FROM employers WHERE parent_id=%s))"
    res = pg._get('jobs', None, where, [37, 142, 142])
    #pprint(res)

    def save_attributes(pg, job, attributes):
        pg.dbopen()
        job_id = int(job[u'id'])
        sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
        pg.cur.execute(sql, [pg._serialise_dict(attributes), job_id])
        pg.conn.commit()

    for job in res:
        attributes = (job['attributes'])
        attributes[u'category'] = 27
        save_attributes(pg, job, attributes)


#----------------------------------------------------------------------------


#recreate indexes
def cp_indexes():
    pg = PgSQLStore(db2)
    sql = "SELECT indexdef FROM pg_indexes where schemaname='public'"
    res = pg._getraw(sql, ['indexdef'])
    pg2 = PgSQLStore(db)

    for row in res:
        sql = row['indexdef']
        pg2.dbopen()
        try:
            pg2.cur.execute(sql)
            pg2.conn.commit()
        except Exception, ex:
            pg2.dbclose()
            logger.error('Command was: %s' % ex)
        else:
            logger.info('Success: %s' % sql)


#----------------------------------------------------------------------------------


def inc_slot(slot):
    slot = list(slot)
    if slot[3] < 23:
        slot[3] += 1
    else:
        slot[3] = 0
        slot[2] += 1
    return tuple(slot)

def row_in_rows(row, rows):
    for item in rows:
        if row['user_ip'] == item['user_ip']:
            if row['job_id'] == row['job_id']:
                if row['employer_id'] == item['employer_id']:
                    if row['job_board_id'] == item['job_board_id']:
                        if row['action'] == item['action']:
                            if row['user_token'] == item['user_token']:
                                if row['candidate_id'] == item['candidate_id']:
                                    #pprint(str(row['created_at']))
                                    #pprint(str(item['created_at']))
                                    if str(row['created_at']) == str(item['created_at']):
                                        return True

def insert_missed_rows(pg2, rows1, rows2, without_candidates=False):
    pg2.dbopen()
    toinsert = []
    for row1 in rows1:
        if not row_in_rows(row1, rows2):
            del row1['id']
            if without_candidates == True:
                del row1['candidate_id']
            toinsert.append(row1)
    res = pg2._insert('stats', toinsert)
    # res = {'result': True}
    if res['result']:
        return len(toinsert)
    else:
        return 'Error: %s' % res['error']

#insert missed stat
def insert_missed_stats():
    pg = PgSQLStore(db)
    pg2 = PgSQLStore(db2)

    start_point = (2018, 9, 19, 8, 0, 0, 0, 0, 0)
    delta = 54  # number of hours
    point = start_point
    next_point = inc_slot(start_point)
    for i in range(0, delta):
        logger.info('Processing interval: ' + time.strftime('%Y-%m-%d %H:%M:%S', point) + '-' + time.strftime(
            '%Y-%m-%d %H:%M:%S', next_point))
        point = next_point
        next_point = inc_slot(next_point)
        rows1 = pg._get('stats', None, 'created_at >= %s AND created_at < %s',
                        [time.strftime('%Y-%m-%d %H:%M:%S', point), time.strftime('%Y-%m-%d %H:%M:%S', next_point)])
        rows2 = pg2._get('stats', None, 'created_at >= %s AND created_at < %s',
                         [time.strftime('%Y-%m-%d %H:%M:%S', point), time.strftime('%Y-%m-%d %H:%M:%S', next_point)])
        try:
            inserted_rows = insert_missed_rows(pg2, rows1, rows2)
        except Exception, ex:
            inserted_rows = insert_missed_rows(pg2, rows1, rows2, True)
        logger.info('len(rows1)=%i' % len(rows1))
        logger.info('len(rows2)=%i' % len(rows2))
        if type(inserted_rows) is int:
            logger.info('%i rows was inserted' % inserted_rows)
        else:
            logger.info('Error: %s' % inserted_rows)
    return False


#fill personality matching
def fill_personality_matching():
    pg = PgSQLStore(db)

    candidates = pg._get('x_ranking_candidates', ['id', 'description'], 'TRUE', None)
    headers = {'Content-Type': 'application/json'}
    count = 1
    for candidate in candidates:
        logger.info('Processing candidate id=%i (%i/%i)' % (candidate['id'], count, len(candidates)))
        res = requests.post(url='https://satisfaction-prediction.rd.xtramile.tech/predict_satisfaction', json={'id': candidate['description']}, verify=certifi.where(), headers=headers)
        matching = (json.loads(res.text))[u'matching']
        pprint(matching)
        res = requests.post(url='https://personnality-prediction.rd.xtramile.tech/predict_mbti', json={'id': candidate['description']}, verify=certifi.where(), headers=headers)
        info = (json.loads(res.text))
        pprint(info)
        sql = "UPDATE x_ranking_candidates SET matching=%s, info=%s WHERE id=%s"
        pg.dbopen()
        pg.cur.execute(sql, [matching, pg._serialise_dict(info), candidate['id']])
        pg.conn.commit()
        count += 1

#copy x-ranking candidates and jobs from dev to prod
def copy_x_ranking_candidates():
    pg = PgSQLStore(db)
    pg2 = PgSQLStore(db2)
    candidates = pg._get('x_ranking_candidates', None, 'TRUE')
    res = pg2._insert('x_ranking_candidates', candidates)

    jobs = pg._get('x_ranking_jobs', None, 'TRUE')
    res = pg2._insert('x_ranking_jobs', jobs)
    pprint(res)


#kill blocked db queries
def kill_blocked_query():
    pg = PgSQLStore(db)
    sql = "SELECT pid, datname, usename, now() - query_start AS runtime, wait_event, wait_event_type, state, query FROM pg_stat_activity WHERE wait_event is NOT NULL AND state = 'active'"
    logger.info('query: %s' % sql)
    res = pg._getraw(sql, ['pid', 'datename', 'username', 'runtime', 'wait_event', 'wait_even_type', 'state', 'query'])
    logger.info('%s rows' % len(res))
    if res:
        for row in res:
            logger.info('Cancelling query: "%s"' % row['query'])
            try:
                pg.execute("SELECT pg_cancel_backend(%i)" % row['pid'])
                pg.execute("SELECT pg_terminate_backend(%i)" % row['pid'])
            except Exception, ex:
                logger.error(ex)

    sql = "SELECT pid, now() - query_start as \"runtime\", usename, datname, state, query FROM  pg_stat_activity WHERE now() - query_start > '2 minutes'::interval ORDER BY runtime DESC"
    logger.info('query: %s' % sql)
    res = pg._getraw(sql, ['pid', 'runtime', 'username', 'datname', 'state', 'query'])
    logger.info('%s rows' % len(res))
    if res:
        for row in res:
            logger.info('Cancelling query: "%s"' % row['query'])
            try:
                pg.execute("SELECT pg_cancel_backend(%i)" % row['pid'])
                pg.execute("SELECT pg_terminate_backend(%i)" % row['pid'])
            except Exception, ex:
                logger.error(ex)



#update jobs.apply
def update_jobs_apply():
    LIMIT = 10000
    pg = PgSQLStore(db2)
    res = pg._getraw("SELECT count(*) AS count FROM jobs", ['count'])
    number_rows = 0
    updated_rows = 0
    if res:
        number_rows = res[0]['count']
    logger.info('%i rows found' % number_rows)
    while updated_rows < number_rows:
        pg.execute("UPDATE jobs SET apply=0 WHERE id IN (SELECT id FROM jobs WHERE apply ISNULL ORDER BY id LIMIT %i)" % LIMIT)
        res = pg._getraw("SELECT count(*) AS count FROM jobs WHERE apply NOTNULL", ['count'])
        if res:
            updated_rows = int(res[0]['count'])
            logger.info('%i rows have been updated' % updated_rows)


def test_tidy(text):
    from tidylib import tidy_document, tidy_fragment
    document, errors = tidy_fragment(text, options={'numeric-entities':1})
    pprint(document)
    pprint(errors)
    text = '''
    <p class="MsoNormal" style="mso-margin-top-alt: auto; margin-bottom: 17.25pt; text-align: justify; line-height: normal;"><b><u><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Entreprise</span></u></b></p>
    <p>&nbsp;</p>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; margin-bottom: 17.25pt; text-align: justify; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Liebherr-Components Colmar SAS est sp&eacute;cialis&eacute;e dans le d&eacute;veloppement, la conception, l'assemblage et les tests de moteurs Diesel de grande puissance. Ces derniers viennent &eacute;toffer le portefeuille actuel de moteurs Diesel Liebherr avec une plage de puissance plus &eacute;lev&eacute;e.</span></p>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; margin-bottom: 17.25pt; text-align: justify; line-height: normal;"><b><u><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Responsabilit&eacute;s</span></u></b></p>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; margin-bottom: 17.25pt; text-align: justify; line-height: normal;">&nbsp;</p>
    <ul style="margin-top: 0cm;" type="square">
    <li class="MsoNormal" style="margin-bottom: .0001pt; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">D&eacute;velopper des syst&egrave;mes &eacute;lectriques Haute Tension pour les pelles &agrave; entra&icirc;nement &eacute;lectrique</span></li>
    <li class="MsoNormal" style="margin-bottom: .0001pt; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Assurer l&rsquo;int&eacute;gration des syst&egrave;mes &eacute;lectriques sur les machines &agrave; entra&icirc;nement &eacute;lectrique haute tension</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Assurer le suivi de projets de ces pelles avec la vente ainsi que le suivi de s&eacute;rie avec le SAV</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Garantir la conformit&eacute; des conceptions aux normes et r&eacute;glementations applicables</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">D&eacute;velopper en collaboration avec les autres groupes du bureau d&rsquo;&eacute;tudes des concepts pour des syst&egrave;mes innovants d&rsquo;entra&icirc;nement &eacute;lectrique dans la cadre du projet management de l&rsquo;&eacute;nergie</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">D&eacute;velopper des projets ayant trait au sujet stockage d&rsquo;&eacute;nergie et Battery Management et les adapter &agrave;&nbsp;aux machines</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Effectuer une veille technologique sur les innovations concernant les entra&icirc;nements &eacute;lectriques (visite dans des salons/foires)</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Collaboration avec les soci&eacute;t&eacute;s s&oelig;urs en Allemagne pour les projets de d&eacute;veloppement et les projets d&rsquo;innovation</span></li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Des d&eacute;placements ponctuels chez les clients sont &agrave; pr&eacute;voir</span></li>
    </ul>
    <p>&nbsp;</p>
    <p><b style="color: #5e5e66; font-size: 16px; text-align: justify;"><u>Comp&eacute;tences</u></b></p>
    <ul style="margin-top: 0cm;" type="square">
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-size: 10.0pt; mso-bidi-font-size: 11.0pt; font-family: Wingdings; mso-fareast-font-family: Wingdings; mso-bidi-font-family: Wingdings; mso-fareast-language: FR;"><span style="font-variant-numeric: normal; font-variant-east-asian: normal; font-stretch: normal; font-size: 7pt; line-height: normal; font-family: 'Times New Roman';">&nbsp; &nbsp; &nbsp; &nbsp;&nbsp;</span></span><!--[endif]--><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Ing&eacute;nieur (h/f)&nbsp;g&eacute;nie &eacute;lectrique ou G&eacute;nie industriel option &eacute;lectrotechnique</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">&nbsp; &nbsp; &nbsp; Profil exp&eacute;riment&eacute;</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-size: 10.0pt; mso-bidi-font-size: 11.0pt; font-family: Wingdings; mso-fareast-font-family: Wingdings; mso-bidi-font-family: Wingdings; mso-fareast-language: FR;"><span style="font-variant-numeric: normal; font-variant-east-asian: normal; font-stretch: normal; font-size: 7pt; line-height: normal; font-family: 'Times New Roman';">&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;</span></span><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Allemand et anglais : bons niveaux</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">&nbsp; &nbsp; &nbsp; &nbsp;Etre autonome et rigoureux</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">&nbsp; &nbsp; &nbsp; &nbsp;Faire preuve d&rsquo;ouverture d&rsquo;esprit</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">&nbsp; &nbsp; &nbsp; &nbsp;Etre curieux et force de proposition</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-size: 10.0pt; mso-bidi-font-size: 11.0pt; font-family: Wingdings; mso-fareast-font-family: Wingdings; mso-bidi-font-family: Wingdings; mso-fareast-language: FR;"><span style="font-variant-numeric: normal; font-variant-east-asian: normal; font-stretch: normal; font-size: 7pt; line-height: normal; font-family: 'Times New Roman';">&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;&nbsp;</span></span><!--[endif]--><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Avoir un bon sens relationnel</span></p>
    </li>
    <li class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; text-align: justify; line-height: normal; mso-list: l0 level1 lfo1; tab-stops: list 36.0pt;">
    <p class="MsoListParagraphCxSpMiddle" style="text-indent: -18pt; line-height: normal;"><span style="font-size: 10.0pt; mso-bidi-font-size: 11.0pt; font-family: Wingdings; mso-fareast-font-family: Wingdings; mso-bidi-font-family: Wingdings; mso-fareast-language: FR;"><span style="font-variant-numeric: normal; font-variant-east-asian: normal; font-stretch: normal; font-size: 7pt; line-height: normal; font-family: 'Times New Roman';">&nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp; &nbsp;&nbsp;</span></span><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Avoir l&rsquo;esprit d&rsquo;&eacute;quipe</span></p>
    </li>
    </ul>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; line-height: normal;"><b><u><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Notre offre</span></u></b><u></u></p>
    <p>&nbsp;</p>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; mso-margin-bottom-alt: auto; line-height: normal;"><span style="font-family: 'Arial',sans-serif; mso-fareast-font-family: 'Times New Roman'; mso-fareast-language: FR;">Une activit&eacute; int&eacute;ressante et formatrice au sein d'une grande entreprise<u></u></span></p>
    <p><span style="color: #5e5e66; font-family: Arial, sans-serif;"><span style="font-size: 16px;">&nbsp;</span></span></p>
    <p class="MsoNormal" style="mso-margin-top-alt: auto; margin-bottom: 17.25pt; text-align: justify; line-height: normal;">&nbsp;</p>
    <script>qwhquwqwqwwqwqwqwqwqwq</script>
    '''

    text = """<div style="font-family: Arial, sans-serif; font-size: 13.3333px;" data-tn-component="jobHeader">&nbsp;</div>
    <p>Salary: &pound;35,000.00 to &pound;50,000.00 /year</p>
    <table style="font-family: Arial, sans-serif; font-size: 13.3333px;" border="0" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td class="snip" style="font-family: Arial, sans-serif; font-size: 10pt; line-height: 1.3; width: 43em;">
    <p style="margin: 0px 0px 1em;">When leading companies choose Condeco Products it's a huge win for spreading the power of our workspace management technology globally. We have helped customers such as Pepsi, Universal, EY and Barclays see a significant ROI by using Condeco software.</p>
    <p style="margin: 0px 0px 1em;">Condeco&rsquo;s continued success means we are looking to hire an experienced Front End Developer to be based in Newcastle</p>
    <p style="margin: 0px 0px 1em;">The package will be made up of basic, which is up to &pound;50,000 plus benefits package</p>
    <p style="margin: 0px 0px 1em;">They will be expected to:</p>
    <ul>
    <li>Work with an agile team of software engineers in Newcastle, UK to deliver high quality software.</li>
    <li>Work with the UX designer, Lead Software Engineer, Product Managers and Architects to define deliveries and work within schedule.</li>
    <li>Work with the design team to ensure/correct for feasibility of their designs</li>
    <li>Develop compelling new user facing features ensuring consistency and performance</li>
    <li>Build reusable libraries for use</li>
    <li>Work with the Quality Assurance team to ensure that the software is subject to full automation testing, can be performance tested, and meets quality requirements.</li>
    <li>Work with the Engineering Department&rsquo;s processes and tools ensuring best practices are adhered to on every project.</li>
    <li>Regularly participate in development meetings, and occasionally participate in department meetings.</li>
    </ul>
    <p style="margin: 0px 0px 1em;">Essential:</p>
    <ul>
    <li>Previous experience of front-end web development in a commercial software company using HTML, CSS, javascript/jquery, typescript and JSON</li>
    <li>Experience with AngularJS 2.0+</li>
    <li>Understanding of responsive, standards compliant design</li>
    <li>Knowledge of browser compatibility issues</li>
    <li>Expert knowledge of common web services protocols and networking, particularly RESTful web services</li>
    <li>Extensive understanding of software engineering concepts, with recent experience in Agile methodologies, and some experience in traditional Waterfall methodologies (i.e., version control systems, peer coding, code reviews, unit testing, continuous integration, release management, etc.).</li>
    <li>Excellent understanding of testing and quality assurance, including browser testing, virtual test environments, beta tests and early adopter programs, and release management.</li>
    </ul>
    <p style="margin: 0px 0px 1em;">Desirable:</p>
    <ul>
    <li>Experience with gulp, webpack</li>
    <li>Recent experience with jira, git, VSTS.</li>
    <li>Experience with cloud services such as Microsoft Azure, Amazon AWS</li>
    <li>Experience in enterprise software (i.e. ERP, CRM, HR, finance) for at least 5 years.</li>
    <li>Experience with backend systems written in C#, ASP.NET MVC</li>
    </ul>
    <p style="margin: 0px 0px 1em;">Nice to have:</p>
    <ul>
    <li>Experience of cloud based analytics systems such as Crashlytics, Hockeyapp, Azure application insights.</li>
    </ul>
    </td>
    </tr>
    </tbody>
    </table>"""


    import re

    allowed_tags = 'strong|b|em|br|ul|ol|li|p|i|u|div|span'.split('|')
    all_tags_re = re.compile('<.*?>')
    all_tags = all_tags_re.findall(text)
    #pprint(all_tags)
    all_tags = map(lambda i: i.split(' ')[0].replace('<', '').replace('>', '').replace('/', ''), all_tags)
    #pprint(list(set(all_tags)))
    for tag in all_tags:
        if tag not in allowed_tags:
            if tag in ['table', 'tbody', 'thead', 'header', 'footer', 'nav', 'section', 'article', 'aside', 'address', 'figure']:
                text = re.sub("""<%s.*?>""" % (tag, ), '', text)
                text = re.sub("""<\/%s>""" % (tag, ), '', text)
            else:
                text = re.sub("""<%s.*?>.*<\/%s>""" % (tag, tag), '', text)

    pprint(text)

#test datetime
def test_datetime():
    import datetime
    pg = PgSQLStore(db)
    job = pg._get('jobs', None, 'id=%s', [1416517])[0]
    pprint(job['updated_at'])

    date = datetime.datetime(2017, 10, 16, 15, 3, 58, 0)
    pprint(date)

    notifyTime = u'2018-10-17T09:42:25.867602863Z'
    d = map(lambda i: int(i), notifyTime.split('T')[0].split('-'))
    t = map(lambda i: int(i), notifyTime.split('T')[1].split('.')[0].split(':'))

    notifyTime = datetime.datetime(d[0], d[1], d[2], t[0], t[1], t[2], 0)
    pprint(notifyTime)


    if date > job['updated_at']:
        print "date > job['updated_at']"
    elif date < job['updated_at']:
        print "date < job['updated_at']"
    else:
        print "date = job['updated_at']"


#copy jobs.attributes from prod to dev if job is published on Google
def copy_jobs_attributes():
    pg = PgSQLStore(db2)
    jobs = pg._get('jobs', None, "attributes->>'google_index_api' NOTNULL", None)
    pg = PgSQLStore(db)
    pg.dbopen()
    for job in jobs:
        logger.info('update job id=%i' % job['id'])
        attributes = job['attributes']
        sql = 'UPDATE jobs SET attributes=%s WHERE id=%s'
        pg.cur.execute(sql, [pg._serialise_dict(attributes), job['id']])
        pg.conn.commit()
    pg.dbclose()
    logger.info('Done')


def align_jobs_budget_spent():
    sql = '''SELECT t3.job_id, t3.employer_id, t2.spent, t3.budget_spent FROM
            (SELECT  j.id as job_id, j.employer_id, budget_spent FROM jobs j
            WHERE j.created_at >= current_timestamp - INTERVAL '3 month' AND budget_spent>0) t3
            LEFT JOIN
            (SELECT * FROM
                (SELECT job_id_original as job_id, employer_id, sum(cpc) AS spent FROM stats_day
                GROUP BY job_id_original, employer_id) t WHERE t.spent > 0) t2
                ON t2.job_id=t3.job_id ORDER BY job_id'''
    pg = PgSQLStore(db2)
    res = pg._getraw(sql, ['job_id', 'employer_id', 'spent', 'budget_spent'])
    pg.dbopen()
    for row in res:
        sql = "UPDATE jobs SET budget_spent=%s WHERE id=%s"
        logger.info('Processing job id=%s' % row['job_id'])
        pg.cur.execute(sql, [row['spent'], row['job_id']])
        pg.conn.commit()
    pg.dbclose()

def fb_campaign_status():
    if len(sys.argv) < 3:
        print 'Usage: %s %s campaign_id' % (sys.argv[0], sys.argv[1])
        exit()
    url = 'https://fb.xtramile.io/api/v1/jobs/%s/status' % sys.argv[2]
    headers = {'Authorization': '5RJL0GL1BIWByCc0whlmKAU5poOZLJ3tHPOwx1tSXBETfH5J0nRmiNUxsbEkg9WH', 'Content-Type': 'application/x-www-form-urlencoded'}
    res = requests.get(url=url, headers=headers)
    pprint(json.loads(res.text))

def job_fb_status():
    if len(sys.argv) < 3:
        print 'Usage: %s %s job_id' % (sys.argv[0], sys.argv[1])
        exit()
    pg = PgSQLStore(db2)
    jobs = pg._get('jobs', None, 'id=%s', [int(sys.argv[2])])
    if len(jobs) == 0:
        print 'Job with id=%s was not found' % sys.argv[2]
        exit()
    job = jobs[0]
    campaign_id = job['attributes']['fbCampaignId']
    print 'campaign id: %s' % str(campaign_id)
    leadform_id = job['attributes']['fbLeadadFormId']
    print 'leadform_id: %s' % str(leadform_id)
    url = 'https://fb.xtramile.io/api/v1/jobs/%s/status' % str(campaign_id)
    headers = {'Authorization': '5RJL0GL1BIWByCc0whlmKAU5poOZLJ3tHPOwx1tSXBETfH5J0nRmiNUxsbEkg9WH', 'Content-Type': 'application/x-www-form-urlencoded'}
    res = requests.get(url=url, headers=headers)
    pprint(json.loads(res.text))

def rm_minus_from_file():
    if len(sys.argv) < 3:
        print 'Usage: %s %s path/to/file' % (sys.argv[0], sys.argv[1])
        exit()
    f = open(sys.argv[2], 'r')
    lines = f.readlines()
    f.close()
    lines = map(lambda l: l[1:],  lines)
    #print ''.join(lines)
    open(sys.argv[2]+'_', 'w').write(''.join(lines))

def format_swagger_file():
    if len(sys.argv) > 2:
        content = open(sys.argv[2], 'r').read()
        d = json.loads(content)
        with open(sys.argv[2]+'_', 'w') as fo:
            fo.write(json.dumps(d))
    else:
        print 'Usage: %s %s %s %s' % (sys.argv[0], sys.argv[1], '<path/to/swagger.json>')



def main():
    if len(sys.argv) > 1:
        globals()[sys.argv[1]]()
    else:
        print 'Usage: %s function-name [args]' % sys.argv[0]


if __name__ == '__main__':
    main()


