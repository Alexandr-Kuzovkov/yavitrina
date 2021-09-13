from SpiderKeeper.app.vitrina.pg import PgSQLStore
from SpiderKeeper.app.vitrina.settings import load_config
from SpiderKeeper.app.vitrina.constants import *
import elasticsearch as elastic
import datetime
import time

config = load_config()

db_conf = {
    'dbname': config['DATABASE']['DB_NAME'],
    'dbuser': config['DATABASE']['DB_USER'],
    'dbhost': config['DATABASE']['DB_HOST'],
    'dbport': config['DATABASE']['DB_PORT'],
    'dbpass': config['DATABASE']['DB_PASS']
}
db = PgSQLStore(db_conf)

