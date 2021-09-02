import psycopg2
import json

class PgSQLBase(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'


    def __init__(self, conf):
        self.dbname = conf.get('dbname')
        self.dbhost = conf.get('dbhost')
        self.dbport = conf.get('dbport')
        self.dbuser = conf.get('dbuser')
        self.dbpass = conf.get('dbpass')

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

    def _get(self, table, field_list=None, tail='', data=None):
        self.dbopen()
        if field_list is None:
            field_list = self._get_fld_list(table)
        sql = ' '.join(['SELECT', ','.join(field_list), 'FROM', table, tail, ';'])
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

    def _getraw(self, sql, field_list, data=None):
        self.dbopen()
        if data is None:
            #print sql
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
        self.dbclose()
        return res

    def _exec(self, sql, data=None):
        self.dbopen()
        if data is None:
            res = self.cur.execute(sql)
        elif type(data) is tuple or type(data) is list:
            res = self.cur.execute(sql, data)
        else:
            raise Exception(self.__class__ + ':data must be tuple or list!')
        self.conn.commit()
        return res

    def _count_rows(self, table):
        sql = ' '.join(['SELECT count(*) AS count FROM', table])
        res = self._getraw(sql, ['count'])
        return int(res[0]['count'])

    def _clear_table(self, table):
        sql = ' '.join(['DELETE FROM', table])
        self.dbopen()
        try:
            self.cur.execute(sql)
        except psycopg2.Error, ex:
            self.conn.rollback()
            self.dbclose()
            raise ex
        else:
            self.conn.commit()
            self.dbclose()

    def _get_tables_list(self):
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type='BASE TABLE'"
        res = self._getraw(sql, ['table_name'])
        return map(lambda i: i['table_name'], res)

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

    def _serialise_dict(self, val):
        if type(val) is dict:
            return json.dumps(val)
        else:
            return val


class PgSQLStore(PgSQLBase):

    def clear_db(self):
        tables = self._get_tables_list()
        for table in tables:
            if table in ['alembic_version']:
                continue
            self._clear_table(table)

    def save_category(self, data):
        self._insert('category', [data])

    def save_tag(self, data):
        self._insert('tag', [data])

    def save_product_card(self, data):
        self._insert('product_card', [data])

    def save_product(self, data):
        self._insert('product', [data])

    def save_image(self, data):
        self._insert('image', [data])









