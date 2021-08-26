import psycopg2
import json

class PgSQLStore(object):
    settings = None
    conn = None
    cur = None
    schema = 'public'
    employers_table = 'employers'

    dbname = 'dbname'
    dbhost = 'dbhost'
    dbport = 5432
    dbuser = 'dbuser'
    dbpass = 'dbpass'

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
        return list(res)


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

    def _getraw(self, sql, field_list, data=None, dbclose=False):
        self.dbopen()
        if data is None:
            # print sql
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
        if dbclose:
            self.dbclose()
        return res

    def getEmployerFeedSetting(self, employer_id):
        field_list = ['id', 'employer_id', 'root', 'job', 'job_external_id', 'url', 'title', 'city',
                      'state', 'country', 'description', 'job_type', 'category', 'posted_at', 'created_at', 'updated_at']
        employer_feed_settings = self._get('jobs.employer_feed_settings', field_list, 'employer_id=%s', (employer_id,))
        if len(employer_feed_settings) > 0:
            return employer_feed_settings[0]
        else:
            return None

    def getEmployerdByName(self, employer_name, fld_list=None):
        table = '.'.join([self.schema, self.employers_table])
        employers = self._get(table, field_list=fld_list, where='name=%s', data=[employer_name])
        if len(employers) > 0:
            return employers[0]
        return None

    def getEmployers(self, employer_name=None):
        table = '.'.join([self.schema, self.employers_table])
        if employer_name is None:
            employers = self._get(table, field_list=None, where='TRUE')
        else:
            employers = self._get(table, field_list=None, where='name=%s', data=(employer_name,))
        return employers

    def getEmployerByMetadata(self, metadata):
        table = '.'.join([self.schema, self.employers_table])
        self.dbopen()
        if type(metadata) is dict:
            try:
                metadata = json.dumps(metadata)
            except Exception:
                raise Exception('Metadata dict not valid!')
        res = self._get(table, field_list=None, where='metadata=%s', data=[metadata])
        self.dbclose()
        if len(res) > 0:
            return res[0]
        else:
            return None

