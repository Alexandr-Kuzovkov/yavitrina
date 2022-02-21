from SpiderKeeper.app.vitrina.pg import PgSQLStore
from SpiderKeeper.app.vitrina.my import MySQLStore
from SpiderKeeper.app.vitrina.settings import load_config
from SpiderKeeper.app.vitrina.constants import *
from SpiderKeeper.app.spider.model import Option
import elasticsearch as elastic
import datetime
import time
from pprint import pprint

config = load_config()

db_conf = {
    'dbname': config['DATABASE']['DB_NAME'],
    'dbuser': config['DATABASE']['DB_USER'],
    'dbhost': config['DATABASE']['DB_HOST'],
    'dbport': config['DATABASE']['DB_PORT'],
    'dbpass': config['DATABASE']['DB_PASS']
}
db = PgSQLStore(db_conf)

mysql_db_conf = {
    'dbname': config['MYSQL']['DB_NAME'],
    'dbuser': config['MYSQL']['DB_USER'],
    'dbhost': config['MYSQL']['DB_HOST'],
    'dbport': config['MYSQL']['DB_PORT'],
    'dbpass': config['MYSQL']['DB_PASS']
}

mysql_db = MySQLStore(mysql_db_conf)

def get_dates():
    dates = []
    now = int(time.time())
    date_count = Option.get_option_value('date_count', 'INTEGER')
    for i in range(0, date_count):
        datestr = time.strftime('%Y-%m-%d', time.gmtime(now - i * 86400))
        dates.append(datestr)
    return dates

def get_stat_entity(entity):
    stat = {}
    count = db.get_stat(entity)
    return count

def get_stat():
    stat = {}
    for entity in [
        'category', 'tag', 'image', 'product', 'product_card', 'search_tag',
        'category_tag', 'brocken_product', 'product_with_params', 'product_with_feedback'
    ]:
        stat[entity] = '<span id="{entity}"><img src="/static/img/preload-small.gif"/></span>'.format(entity=entity)
    return stat

def get_count_for_date(datestr, entity):
    return db.get_count_for_date(datestr, entity)

def get_stat_mysql_table(table):
    return mysql_db.get_stat(table)

def get_tables():
    return mysql_db._get_tables_list()
