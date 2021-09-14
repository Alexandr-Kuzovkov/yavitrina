from SpiderKeeper.app.vitrina.pg import PgSQLStore
from SpiderKeeper.app.vitrina.settings import load_config
from SpiderKeeper.app.vitrina.constants import *
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

def get_dates():
    dates = []
    now = int(time.time())
    for i in range(0, 10):
        datestr = time.strftime('%Y-%m-%d', time.gmtime(now - i * 86400))
        dates.append(datestr)
    return dates

def get_stat():
    stat = {}
    for entity in ['category', 'tag', 'image', 'product', 'product_card']:
        count = db.get_stat(entity)
        stat[entity] = count
    return stat

def get_count_for_date(datestr, entity):
    return db.get_count_for_date2(datestr, entity)

