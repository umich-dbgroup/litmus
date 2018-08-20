__all__ = ['Database']

import re
import time

import mysql.connector

class Attribute:
    def __init__(self, name, type):
        self.rel = None
        self.name = name
        self.type = type      # text, num

        if type == 'num':
            self.min = None
            self.max = None

    def set_rel(self, rel):
        self.rel = rel

    def set_range(self, min, max):
        self.min = min
        self.max = max

class Relation:
    def __init__(self, name, attrs):
        self.name = name
        self.attrs = attrs

def convert_mysql_type(mysql_type):
    if mysql_type.startswith('int') or mysql_type.endswith('int') or mysql_type.startswith('float') or mysql_type.startswith('double') or mysql_type.startswith('decimal') or mysql_type.startswith('numeric'):
        return 'num'
    elif mysql_type == 'text' or mysql_type.startswith('varchar') or mysql_type.startswith('char') or mysql_type.startswith('enum'):
        return 'text'
    else:
        return mysql_type

class Database:
    # relations to ignore from db
    IGNORE_RELS = ['size', 'history']

    def __init__(self, user, pw, host, db, timeout=15000):
        print("Loading database...")
        start = time.time()
        self.conn = mysql.connector.connect(user=user, password=pw, host=host, database=db)
        self.name = db
        self.load_relations()
        self.set_timeout(timeout)
        print("Done loading database [{}s]".format(time.time()-start))

    def load_relations(self):
        self.relations = {}

        # get relation names
        cursor = self.conn.cursor()
        cursor.execute('SHOW TABLES')
        rel_names = []
        for r in cursor:
            if r[0] not in self.IGNORE_RELS:
                rel_names.append(r[0])
        cursor.close()

        # get attributes for each relation, generate Relations
        for rel_name in rel_names:
            cursor = self.conn.cursor()
            cursor.execute('SHOW COLUMNS FROM {}'.format(rel_name))
            attrs = {}
            for r in cursor:
                attr_name = r[0]
                attr_type = convert_mysql_type(r[1])
                attrs[attr_name] = Attribute(attr_name, attr_type)
            cursor.close()

            for attr_name, attr in attrs.items():
                if attr.type == 'num':
                    min, max = self.get_attr_range(rel_name, attr)
                    attr.set_range(min, max)

            self.relations[rel_name] = Relation(rel_name, attrs)

    def cursor(self):
        return self.conn.cursor()

    def set_timeout(self, timeout):
        cursor = self.cursor()
        cursor.execute('SET SESSION MAX_EXECUTION_TIME={}'.format(timeout))
        cursor.close()

    # given a fragment from a SQL statement (e.g. 'actor_0.name')
    # get the attribute
    def get_attr(self, frag):
        rel_alias, attr_name = frag.split('.')
        m = re.match('([A-Za-z_]+)_[0-9]+', rel_alias)
        if m:
            rel_name = m.group(1)
            return self.relations[rel_name].attrs[attr_name]
        else:
            raise Exception('Attribute not found for [{}].'.format(frag))

    def get_attr_range(self, rel_name, attr):
        cursor = self.cursor()
        cursor.execute('SELECT MIN({}), MAX({}) FROM {}'.format(attr.name, attr.name, rel_name))
        row = cursor.fetchone()
        cursor.close()
        return row[0], row[1]

    def execute(self, query):
        cursor = self.cursor()

        try:
            cursor.execute(query)

            query_tuples = set()
            for result in cursor:
                # disallow nulls
                if None in result:
                    continue

                query_tuples.add(result)
            cursor.close()
            return query_tuples
        except Exception, exc:
            cursor.close()
            if str(exc).startswith('3024'):
                raise Exception('Timeout: Query timed out.')
