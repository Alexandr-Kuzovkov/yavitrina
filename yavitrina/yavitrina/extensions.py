import psycopg2
import json
from mysql import connector

class PgSQLBase(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'


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

    def _getone(self, sql, data=None):
        self.dbopen()
        if data is None:
            self.cur.execute(sql)
        else:
            self.cur.execute(sql, data)
        data = self.cur.fetchone()
        if len(data) > 0:
            return data[0]
        return None


class PgSQLStore(PgSQLBase):

    exclude_tables = ['alembic_version']

    def clear_db(self):
        tables = self._get_tables_list()
        while len(tables) > 0:
            table = tables[0]
            if table in self.exclude_tables:
                tables.pop(0)
                continue
            try:
                self._clear_table(table)
            except psycopg2.Error, ex:
                tables.append(tables.pop(0))
            else:
                tables.pop(0)

    def save_category(self, data):
        categories = self._get('category', field_list=None, where='url=%s', data=[data['url']])
        if len(categories) > 0:
            return None
        else:
            if 'parent_url' in data:
                res = self._get('category', field_list=['id'], where='url=%s', data=[data['parent_url']])
                if len(res) > 0:
                    data['parent_id'] = res[0]['id']
            res = self._insert('category', [data])
            return res

    def save_tag(self, data):
        if 'id' in data:
            res = self._update('tag', data,  {'id': data['id']})
            return res
        else:
            res = self._get('tag', field_list=None, where='title=%s', data=[data['title']])
            if len(res) > 0:
                tag = res[0]
                if tag['page'] is not None:
                    pages = tag['page'].split(',')
                    pages.append(data['page'])
                    pages = list(set(pages))
                    data['page'] = ','.join(pages)
                    sql = "UPDATE tag SET page=%s WHERE id=%s"
                    self.dbopen()
                    self.cur.execute(sql, [data['page'], tag['id']])
                    self.conn.commit()
                return None
            else:
                res = self._insert('tag', [data])
                return res

    def save_product_card(self, data):
        res = self._get('product_card', field_list=None, where='product_id=%s', data=[data['product_id']])
        if len(res) > 0:
            product_card = res[0]
            if product_card['page'] is not None:
                pages = product_card['page'].split(',')
                pages.append(data['page'])
                pages = list(set(pages))
                data['page'] = ','.join(pages)
                sql = "UPDATE product_card SET page=%s WHERE id=%s"
                self.dbopen()
                self.cur.execute(sql, [data['page'], product_card['id']])
                self.conn.commit()
            return None
        else:
            res = self._insert('product_card', [data])
            return res

    def save_product(self, data):
        res = self._get('category', field_list=['id'], where='url=%s', data=[data['category']])
        category_id = None
        if len(res) > 0:
            category_id = res[0]['id']
            data['category_id'] = category_id
        products = self._get('product', field_list=None, where='product_id=%s', data=[data['product_id']])
        if len(products) > 0:
            product = products[0]
            self._update('product', data, {'id': product['id']})
            if category_id is not None:
                self._insert('product_category', [{'category_id': category_id, 'product_id': product['id']}])
                return None
        else:
            self._insert('product', [data])
            products = self._get('product', field_list=None, where='product_id=%s', data=[data['product_id']])
            if len(products) > 0 and category_id is not None:
                product = products[0]
                res = self._insert('product_category', [{'category_id': category_id, 'product_id': product['id']}])
                return res

    def save_image(self, data):
        res = self._get('image', field_list=['id'], where='url=%s', data=[data['url']])
        if len(res) == 0:
            res = self._insert('image', [data])
            return res
        return None

    def save_search_tag(self, data):
        res = self._get('search_tag', field_list=None, where='title=%s', data=[data['title']])
        if len(res) > 0:
            tag = res[0]
            if tag['page'] is not None:
                pages = tag['page'].split(',')
                pages.append(data['page'])
                pages = list(set(pages))
                data['page'] = ','.join(pages)
                sql = "UPDATE search_tag SET page=%s WHERE id=%s"
                self.dbopen()
                self.cur.execute(sql, [data['page'], tag['id']])
                self.conn.commit()
            return None
        else:
            res = self._insert('search_tag', [data])
            return res

    def save_category_tag(self, data):
        res = self._get('category_tag', field_list=None, where='title=%s', data=[data['title']])
        if len(res) > 0:
            tag = res[0]
            if tag['page'] is not None:
                pages = tag['page'].split(',')
                pages.append(data['page'])
                pages = list(set(pages))
                data['page'] = ','.join(pages)
                sql = "UPDATE category_tag SET page=%s WHERE id=%s"
                self.dbopen()
                self.cur.execute(sql, [data['page'], tag['id']])
                self.conn.commit()
            return None
        else:
            res = self._insert('category_tag', [data])
            return res

    def save_category_description(self, data):
        res = self._get('category', field_list=None, where='url=%s', data=[data['url']])
        if len(res) > 0:
            category = res[0]
            self._update('category', {'description': data['description']}, {'id': category['id']})
        return None

    def save_settings(self, data):
        res = self._get('settings', field_list=None, where='url=%s AND name=%s', data=[data['url'], data['name']])
        if len(res) == 0:
            self._insert('settings', [data])
            return None

    def save_settings_value(self, data):
        res = self._get('settings_value', field_list=None, where='settings_name=%s AND value=%s AND url=%s', data=[data['settings_name'], data['value'], data['url']])
        if len(res) == 0:
            self._insert('settings_value', [data])
            return None

    def get_items_total(self, table, condition=None):
        if condition is not None and type(condition) is not dict and type(condition) is not str:
            raise Exception('Type of the condition must be dict or str!')
        if condition is not None and type(condition) is dict:
            cond = ' AND '.join(map(lambda k: k + ' %s', condition.keys()))
            data = condition.values()
            sql = "SELECT count(*) AS total FROM {table} WHERE {cond}".format(table=table, cond=cond)
        elif condition is not None and type(condition) is str:
            data = None
            cond = condition
            sql = "SELECT count(*) AS total FROM {table} WHERE {cond}".format(table=table, cond=cond)
        else:
            data = None
            sql = "SELECT count(*) AS total FROM {table}".format(table=table)
        self.dbopen()
        res = self._getone(sql, data)
        return res

    def get_items_chunk(self, table, condition=None, offset=0, limit=100, order='id'):
        if condition is not None and type(condition) is not dict and type(condition) is not str:
            raise Exception('Type of the condition must be dict or str!')
        fld_lst = self._get_fld_list(table)
        if condition is not None and type(condition) is dict:
            cond = ' AND '.join(map(lambda k: k + ' %s', condition.keys()))
            data = condition.values()
            sql = "SELECT * FROM {table} WHERE {cond} ORDER BY {order} OFFSET {offset} LIMIT {limit} ".format(table=table, cond=cond, order=order, limit=limit, offset=offset)
            res = self._getraw(sql, fld_lst, data)
        elif condition is not None and type(condition) is str:
            cond = condition
            data = None
            sql = "SELECT * FROM {table} WHERE {cond} ORDER BY {order} OFFSET {offset} LIMIT {limit} ".format(table=table, cond=cond, order=order, limit=limit, offset=offset)
            res = self._getraw(sql, fld_lst, data)
        else:
            sql = "SELECT * FROM {table} ORDER BY {order} OFFSET {offset} LIMIT {limit} ".format(table=table, order=order, limit=limit, offset=offset)
            res = self._getraw(sql, fld_lst)
        return res



class MySQLBase(object):
    settings = None
    conn = None
    cur = None

    def __init__(self, conf):
        self.dbname = conf.get('dbname')
        self.dbhost = conf.get('dbhost')
        self.dbport = conf.get('dbport')
        self.dbuser = conf.get('dbuser')
        self.dbpass = conf.get('dbpass')

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


