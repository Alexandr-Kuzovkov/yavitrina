import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import json
from pprint import pprint

class PgSQLStoreBase(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'

    dbname = 'dbname'
    dbhost = 'dbport'
    dbport = 0000
    dbuser = 'dbuser'
    dbpass = 'dbpass'
    pool = None


    def __init__(self, db):
        self.dbname = db.get('dbname')
        self.dbhost = db.get('dbhost')
        self.dbport = db.get('dbport')
        self.dbuser = db.get('dbuser')
        self.dbpass = db.get('dbpass')

        self.pool = ThreadedConnectionPool(3, 20,
                                      user = self.dbuser,
                                      password = self.dbpass,
                                      host = self.dbhost,
                                      port = self.dbport,
                                      database = self.dbname)

    def dbopen(self):
        if self.conn is None:
            self.conn = psycopg2.connect(dbname=self.dbname, user=self.dbuser, password=self.dbpass, host=self.dbhost, port=self.dbport)
            self.cur = self.conn.cursor()

    def dbclose(self):
        if self.cur is not None:
            self.cur.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _get_fld_list(self, table, dbclose=False):
        self.dbopen()
        if '.' in table:
            table = table.split('.').pop()
        self.cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s", (self.schema, table))
        res = self.cur.fetchall()
        if res is not None:
            res = map(lambda i: i[0], res)
        if dbclose:
            self.dbclose()
        return res

    def _get(self, table, field_list=None, where='', data=None):
        self.dbopen()
        if field_list is None:
            field_list = self._get_fld_list(table)
        sql = ' '.join(['SELECT', ','.join(field_list), 'FROM', table, 'WHERE', where, ';'])
        if data is None:
            self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        data = self.cur.fetchall()
        res = []
        for row in data:
            d = {}
            for i in range(len(row)):
                d[field_list[i]] = row[i]
            res.append(d)
        self.dbclose()
        return res

    def _getraw(self, sql, field_list, data=None, close=True):
        self.dbopen()
        if data is None:
            self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        data = self.cur.fetchall()
        res = []
        for row in data:
            if len(field_list) != len(row):
                raise Exception('Number fields in fields list no match number columns in result!')
            d = {}
            for i in range(len(row)):
                d[field_list[i]] = row[i]
            res.append(d)
        if close:
            self.dbclose()
        return res

    def execute(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        self.dbclose()
        return res

    def run(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        output = self.cur.fetchall()
        #self.dbclose()
        return output

    def _getone(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        output = self.cur.fetchone()
        #self.dbclose()
        return output

    def _serialise_dict(self, val):
        if type(val) is dict:
            return json.dumps(val)
        if type(val) is list:
            return '{%s}' % json.dumps(val)[1:-1]
        else:
            return val

    def _insert(self, table, data):
        if type(data) is not list:
            raise Exception('Type of data must be list!')
        self.dbopen()
        #self.cur.execute(' '.join(["SELECT setval('", table+'_id_seq', "', (SELECT max(id) FROM", table, '));']))
        for row in data:
            if type(row) is not dict:
                raise Exception('Type of row must be dict!')
            sql = ' '.join(['INSERT INTO', table, '(', ','.join(row.keys()), ') VALUES (', ','.join(['%s' for i in row.keys()]), ');'])
            try:
                values = map(lambda val: self._serialise_dict(val), row.values())
                self.cur.execute(sql, values)
            except psycopg2.Error, ex:
                self.conn.rollback()
                self.dbclose()
                print ex
                return {'result': False, 'error': ex}
        self.conn.commit()
        return {'result': True}

class PgSQLStore(PgSQLStoreBase):

    def get_stat(self, entity):
        if entity == 'brocken_product':
            res = self._getone("SELECT count(*) AS count FROM product WHERE (description ISNULL OR description='') AND (title ISNULL OR title='')")
        elif entity == 'product_with_params':
            res = self._getone("SELECT count(* ) AS count FROM product WHERE parameters NOTNULL")
        elif entity == 'product_with_feedback':
            res = self._getone("SELECT count(*) AS count FROM product WHERE feedbacks NOTNULL")
        else:
            res = self._getone("SELECT count(*) AS count FROM " + entity)
        if res and len(res):
            return int(res[0])
        else:
            return 0

    def get_count_for_date(self, datestr, entity):
        if entity == 'brocken_product':
            query = "SELECT count(*) AS count FROM product WHERE (description ISNULL OR description='') AND (title ISNULL OR title='') AND date(created_at)='{date}'".format(date=datestr)
        elif entity == 'product_with_params':
            query = "SELECT count(* ) AS count FROM product WHERE parameters NOTNULL AND date(created_at)='{date}'".format(date=datestr)
        elif entity == 'product_with_feedback':
            query = "SELECT count(*) AS count FROM product WHERE feedbacks NOTNULL AND date(created_at)='{date}'".format(date=datestr)
        else:
            query = "SELECT count(*) AS count FROM {entity} WHERE date(created_at)='{date}'".format(entity=entity, date=datestr)
        try:
            result = 0
            connection = self.pool.getconn()
            connection.autocommit = True
            with connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    #if cursor.rownumber > 0:
                    res = cursor.fetchone()
                    if res and len(res):
                        result = int(res[0])
                except (Exception, psycopg2.DatabaseError) as e:
                    raise
        except (Exception, psycopg2.DatabaseError) as error:
            print(e)
        else:
            pass
        finally:
            self.pool.putconn(connection)
            return result
