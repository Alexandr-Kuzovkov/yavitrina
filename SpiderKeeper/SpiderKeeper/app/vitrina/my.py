from mysql import connector
import json
import mysql.connector.pooling

class MySQLBase(object):
    settings = None
    conn = None
    cur = None
    pool = None

    def __init__(self, conf):
        self.dbname = conf.get('dbname')
        self.dbhost = conf.get('dbhost')
        self.dbport = conf.get('dbport')
        self.dbuser = conf.get('dbuser')
        self.dbpass = conf.get('dbpass')
        self.pool = connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=30, pool_reset_session=True, host=self.dbhost, user=self.dbuser, password=self.dbpass, port=self.dbport, database=self.dbname)


    def dbopen(self):
        if self.conn is None:
            self.conn = connector.MySQLConnection(host=self.dbhost, user=self.dbuser, password=self.dbpass, port=self.dbport, database=self.dbname)
            self.cur = self.conn.cursor()
            self.cur.execute('SET NAMES utf8mb4')

    def dbclose(self):
        if self.conn is not None:
            self.conn.close()
            self.cur = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

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

    def _getraw(self, sql, field_list, data=None):
        self.dbopen()
        if data is None:
            #print sql
            print(self.cur)
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

    def _get_fld_list(self, table, dbclose=False):
        self.dbopen()
        if '.' in table:
            table = table.split('.').pop()
        self.cur.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = Database() AND TABLE_NAME = %s", (table,))
        res = self.cur.fetchall()
        if res is not None:
            res = map(lambda i: i[0], res)
        if dbclose:
            self.dbclose()
        return res

    def _get_tables_list(self):
        self.dbopen()
        sql = "SHOW TABLES"
        res = self._getraw(sql, ['Tables_in_{database}'.format(database=self.dbname)])
        return map(lambda i: i['Tables_in_{database}'.format(database=self.dbname)], res)

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

    def _getone(self, sql, data=None):
        self.dbopen()
        if data is None:
            self.cur.execute(sql)
        else:
            self.cur.execute(sql, data)
        data = self.cur.fetchone()
        if data is not None and len(data) > 0:
            return data[0]
        return None

    def _insert(self, table, data, ignore=False):
        if type(data) is not list:
            raise Exception('Type of data must be list!')
        self.dbopen()
        #self.cur.execute(' '.join(["SELECT setval('", table+'_id_seq', "', (SELECT max(id) FROM", table, '));']))
        for row in data:
            if type(row) is not dict:
                raise Exception('Type of row must be dict!')
            if ignore:
                sql = ' '.join(['INSERT IGNORE INTO', table, '(', ','.join(row.keys()), ') VALUES (', ','.join(['%s' for i in row.keys()]), ');'])
            else:
                sql = ' '.join(['INSERT INTO', table, '(', ','.join(row.keys()), ') VALUES (', ','.join(['%s' for i in row.keys()]), ');'])
            try:
                values = map(lambda val: self._serialise_dict(val), row.values())
                self.cur.execute(sql, values)
            except Exception as ex:
                self.conn.rollback()
                self.dbclose()
                print ex
                return {'result': False, 'error': ex}
        self.conn.commit()
        return {'result': True}

    def _update(self, table, data, cond):
        if type(data) is not dict:
            raise Exception('Type of data must be dict!')
        if type(cond) is not dict:
            raise Exception('Type of cond must be dict!')
        self.dbopen()
        #self.cur.execute(' '.join(["SELECT setval('", table+'_id_seq', "', (SELECT max(id) FROM", table, '));']))
        sets = []
        for fld, val in data.items():
            sets.append('{fld}=%s'.format(fld=fld))
        conds = []
        for fld, val in cond.items():
            conds.append('{fld}=%s'.format(fld=fld))
        if len(conds) > 0:
            conds = ' AND '.join(conds)
        else:
            conds = 'TRUE'
        sql = ' '.join(['UPDATE', table, 'SET', ','.join(sets), 'WHERE', conds])
        values = map(lambda val: self._serialise_dict(val), data.values()) + map(lambda val: self._serialise_dict(val), cond.values())
        try:
            self.cur.execute(sql, values)
        except Exception as ex:
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

    def _clear_table(self, table):
        sql = ' '.join(['DELETE FROM', table])
        self.dbopen()
        try:
            self.cur.execute(sql)
        except Exception as ex:
            self.conn.rollback()
            self.dbclose()
            raise ex
        else:
            self.conn.commit()
            self.dbclose()


class MySQLStore(MySQLBase):

    exclude_tables = []
    buffer_size = 200
    buffer = {}

    def clear_db(self):
        tables = self._get_tables_list()
        while len(tables) > 0:
            table = tables[0]
            if table in self.exclude_tables:
                tables.pop(0)
                continue
            try:
                self._clear_table(table)
            except Exception as ex:
                tables.append(tables.pop(0))
            else:
                tables.pop(0)

    def get_latest_time(self, table):
        sql = "SELECT max(created_at) AS last_time FROM {table}".format(table=table)
        res = self._getone(sql)
        return res

    def flush(self):
        for table in self.buffer.keys():
            if len(self.buffer[table]) > 0:
                self._insert(table, self.buffer[table])


    def save_product(self, data):
        table = 'product'
        if table not in self.buffer:
            self.buffer[table] = []
        if len(self.buffer[table]) < self.buffer_size:
            self.buffer[table].append(data)
        else:
            try:
                product_ids = map(lambda i: i['product_id'], self.buffer[table])
                exist_items = self._getraw("SELECT product_id FROM product WHERE product_id IN (%s)" % ','.join(product_ids), ['product_id'], None)
                exist_product_ids = map(lambda i: i['product_id'], exist_items)
                filtered_buffer = filter(lambda i: i['product_id'] not in exist_product_ids, self.buffer[table])
                self._insert(table, filtered_buffer)
                self.buffer[table] = []
            except Exception as ex:
                print(ex)
            finally:
                self.buffer[table].append(data)

    def get_items_chunk(self, table, condition=None, offset=0, limit=100, order='id'):
        if condition is not None and type(condition) is not dict:
            raise Exception('Type of the condition must be dict!')
        fld_lst = self._get_fld_list(table)
        if condition is not None:
            cond = ' AND '.join(map(lambda k: k + ' %s', condition.keys()))
            data = condition.values()
            sql = "SELECT * FROM {table} WHERE {cond} ORDER BY {order}  LIMIT {limit} OFFSET {offset}".format(table=table, cond=cond, order=order, limit=limit, offset=offset)
            res = self._getraw(sql, fld_lst, data)
        else:
            sql = "SELECT * FROM {table} ORDER BY {order} LIMIT {limit} OFFSET {offset} ".format(table=table, order=order, limit=limit, offset=offset)
            res = self._getraw(sql, fld_lst)
        return res

    def get_items_total(self, table, condition=None):
        if condition is not None and type(condition) is not dict:
            raise Exception('Type of the condition must be dict!')
        if condition is not None:
            cond = ' AND '.join(map(lambda k: k + ' %s', condition.keys()))
            data = condition.values()
            sql = "SELECT count(*) AS total FROM {table} WHERE {cond}".format(table=table, cond=cond)
        else:
            data = None
            sql = "SELECT count(*) AS total FROM {table}".format(table=table)
        self.dbopen()
        res = self._getone(sql, data)
        return res


    def get_fld_list(self, table):
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        sql = cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = Database() AND TABLE_NAME = %s", (table,))
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        if res is not None:
            res = map(lambda i: i[0], res)
        return res


    def get_stat(self, table):
        fld_lst = self.get_fld_list(table)
        if 'created_at' in fld_lst:
            sql = "SELECT count(*) AS total, max(created_at) AS last FROM {table} LIMIT 1".format(table=table)
        else:
            sql = "SELECT count(*) AS total, NULL AS last FROM {table} LIMIT 1".format(table=table)
        conn = self.pool.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql)
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        if len(res) > 0:
            return {'last': str(res[0][1]), 'total': res[0][0]}
        return None




