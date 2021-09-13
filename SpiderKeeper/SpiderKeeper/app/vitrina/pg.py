import psycopg2
import json

class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'

    dbname = 'dbname'
    dbhost = 'dbport'
    dbport = 0000
    dbuser = 'dbuser'
    dbpass = 'dbpass'


    def __init__(self, db):
        self.dbname = db.get('dbname')
        self.dbhost = db.get('dbhost')
        self.dbport = db.get('dbport')
        self.dbuser = db.get('dbuser')
        self.dbpass = db.get('dbpass')

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

