__all__ = ['Database']

import os
import pickle
import re
import time

import mysql.connector

class AttributeIntersect(object):
    def __init__(self, type, vals=[], min=None, max=None):
        self.type = type
        if type == 'text':
            self.vals = sorted(vals)
        elif type == 'num':
            self.min = min
            self.max = max

    def __unicode__(self):
        if self.type == 'text':
            return 'text: {}'.format(len(self.vals))
        else:
            return 'num: [{},{}]'.format(self.min, self.max)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def is_empty(self):
        if self.type == 'text':
            return len(self.vals) == 0
        elif self.type == 'num':
            return self.min is None or self.max is None

    def is_all(self):
        return isinstance(self, AllAttributeIntersect)

    def union(self, other):
        if self.is_all():
            return self
        elif other.is_all():
            return other

        if other.is_empty():
            return self
        elif self.is_empty():
            return other

        if self.type != other.type:
            raise Exception('Cannot union two AttributeIntersects of differing type.')

        if self.type == 'num':
            smaller_min = min(self.min, other.min)
            larger_max = max(self.max, other.max)
            return AttributeIntersect('num', min=smaller_min, max=larger_max)
        elif self.type == 'text':
            return AttributeIntersect('text', vals=self.vals + other.vals)

    def within(self, other):
        if other.is_all() or self.is_empty():
            return True

        if self.is_all() or other.is_empty():
            return False

        if self.type == 'num':
            return other.min <= self.min and other.max >= self.max
        elif self.type == 'text':
            return set(self.vals) <= set(other.vals)

# case that all values intersect (e.g. attribute is intersected with self)
class AllAttributeIntersect(AttributeIntersect):
    def __init__(self, type):
        self.type = type
        self.vals = None
        self.min = None
        self.max = None

    def is_all(self):
        return True

    def is_empty(self):
        return False

    def __unicode__(self):
        return '{}: ALL'.format(self.type)

class Attribute(object):
    def __init__(self, name, type):
        self.rel = None
        self.name = name
        self.type = type      # text, num
        self.length = None    # for text only
        self.pk = False
        self.unique_count = None     # unique vals count

        if type == 'num':
            self.min = None
            self.max = None

    def set_rel(self, rel):
        self.rel = rel

    def set_length(self, length):
        self.length = length

    def set_range(self, min, max):
        self.min = min
        self.max = max

    def set_pk(self, pk):
        self.pk = pk

    def __unicode__(self):
        return u'{}.{}'.format(self.rel.name, self.name)

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        return str(self) == str(other)

class AttributeSet(object):
    def __init__(self, attrs):
        self.attrs = sorted(attrs, key=lambda a: str(a))

    def __unicode__(self):
        return u'[{}]'.format(','.join([unicode(a) for a in self.attrs]))

    def __str__(self):
        return unicode(self).encode('utf-8')

    def __iter__(self):
        return iter(self.attrs)

    def __getitem__(self, key):
        return self.attrs[key]

    def __len__(self):
        return len(self.attrs)

    def __hash__(self):
        return hash(tuple(self.attrs))

    def __eq__(self, other):
        return self.attrs == other.attrs

class Relation(object):
    def __init__(self, name, attrs):
        self.name = name
        self.attrs = attrs

def convert_mysql_type(mysql_type):
    if mysql_type.startswith('int') or mysql_type.endswith('int') or mysql_type.startswith('float') or mysql_type.startswith('double') or mysql_type.startswith('decimal') or mysql_type.startswith('numeric'):
        return 'num', None
    elif mysql_type.startswith('varchar'):
        m = re.match('varchar\(([0-9]+)\)', mysql_type)
        return 'text', int(m.group(1))
    elif mysql_type.startswith('char'):
        m = re.match('char\(([0-9]+)\)', mysql_type)
        return 'text', int(m.group(1))
    elif mysql_type == 'text' or mysql_type.startswith('enum'):
        return 'text', None
    else:
        return mysql_type, None

class Database(object):
    # relations to ignore from db
    IGNORE_RELS = ['size', 'history', 'ids']

    def __init__(self, user, pw, host, db, cache_dir, timeout=None, buffer_pool_size=None):
        print("Loading database...")
        start = time.time()
        self.conn = mysql.connector.connect(user=user, password=pw, host=host, database=db)
        self.name = db
        self.cache_path = os.path.join(cache_dir, db + '.cache')

        loaded_from_cache = self.load_relations()
        if timeout is not None:
            self.set_timeout(timeout)
        if buffer_pool_size is not None:
            self.set_buffer_pool_size(buffer_pool_size)
        self.set_packet_size()
        print("Loaded from cache: {}".format(loaded_from_cache))
        print("Done loading database [{}s]".format(time.time()-start))

    def load_cache(self):
        if os.path.exists(self.cache_path):
            self.relations = pickle.load(open(self.cache_path, 'rb'))
            return True
        return False

    def save_cache(self):
        pickle.dump(self.relations, open(self.cache_path, 'wb'))

    def load_relations(self):
        if self.load_cache():
            return True

        self.relations = {}

        # get relation names
        cursor = self.conn.cursor()
        cursor.execute('SHOW TABLES')
        rel_names = []
        for r in cursor:
            if r[0].lower() not in self.IGNORE_RELS:
                rel_names.append(r[0].lower())
        cursor.close()

        # get attributes for each relation, generate Relations
        for rel_name in rel_names:
            cursor = self.conn.cursor()
            cursor.execute('SHOW COLUMNS FROM {}'.format(rel_name))
            attrs = {}
            for r in cursor:
                attr_name = r[0].lower()
                attr_type, attr_length = convert_mysql_type(r[1])
                attrs[attr_name] = Attribute(attr_name, attr_type)

                if attr_length:
                    attrs[attr_name].set_length(attr_length)

                if r[3].startswith('PRI'):
                    attrs[attr_name].set_pk(True)
            cursor.close()

            for attr_name, attr in attrs.items():
                if attr.type == 'num':
                    min, max = self.get_attr_range(rel_name, attr)
                    attr.set_range(min, max)

            self.relations[rel_name] = Relation(rel_name, attrs)

            for attr_name, attr in attrs.items():
                attr.set_rel(self.relations[rel_name])

        # save to cache
        self.save_cache()

        return False

    def cursor(self):
        return self.conn.cursor()

    def set_packet_size(self):
        cursor = self.cursor()
        cursor.execute('SET GLOBAL NET_BUFFER_LENGTH={}'.format(1000000))
        cursor.close()

        cursor = self.cursor()
        cursor.execute('SET GLOBAL MAX_ALLOWED_PACKET={}'.format(1000000000))
        cursor.close()

    def set_timeout(self, timeout):
        cursor = self.cursor()
        cursor.execute('SET SESSION MAX_EXECUTION_TIME={}'.format(timeout))
        cursor.close()

    def set_buffer_pool_size(self, size):
        cursor = self.cursor()
        cursor.execute('SET GLOBAL innodb_buffer_pool_size={}'.format(size))
        cursor.close()

    def get_all_attrs(self):
        return self.get_attrs_of_type('*')

    def get_text_attrs(self):
        return self.get_attrs_of_type('text')

    def get_num_attrs(self):
        return self.get_attrs_of_type('num')

    def get_attrs_of_type(self, type):
        attrs = []
        for rel_name, rel in self.relations.items():
            for attr_name, attr in rel.attrs.items():
                if type == '*' or attr.type == type:
                    attrs.append(attr)
        return attrs

    # given a fragment from a SQL statement (e.g. 'actor_0.name')
    # get the attribute
    def get_attr(self, frag):
        if not (isinstance(frag, str) or isinstance(frag, unicode)) or '.' not in frag:
            return None

        rel_alias, attr_name = frag.lower().split('.')
        m = re.match('([A-Za-z_]+)_[0-9]+', rel_alias)
        if not m:
            m = re.match('([A-Za-z_]+)', rel_alias)
        if m:
            rel_name = m.group(1)
            return self.relations[rel_name].attrs[attr_name]
        else:
            raise Exception('Attribute not found for [{}].'.format(frag))

    def create_tmp_table_for_attr(self, attr):
        tmp_name = 'distinct_{}_{}'.format(attr.rel.name, attr.name)

        index_len = min(15, attr.length)
        cursor = self.cursor()
        query_str = 'CREATE TEMPORARY TABLE IF NOT EXISTS {} (INDEX ({}({}))) SELECT DISTINCT {} FROM {}'.format(tmp_name, attr.name, index_len, attr.name, attr.rel.name)
        cursor.execute(query_str)
        cursor.close()

        return tmp_name

    def find_text_intersects(self, first, second):
        # use first letter of rel as alias
        first_rel_alias = first.rel.name[0]
        first_alias = '{}.{}'.format(first_rel_alias, first.name)

        second_rel_alias = second.rel.name[0]
        if second_rel_alias.lower() == first_rel_alias.lower():
            # extend second rel name if it coincides with first rel
            second_rel_alias = second.rel.name[0:4]
        second_alias = '{}.{}'.format(second_rel_alias, second.name)

        proj_str = first_alias

        pred_str = '{} = {}'.format(first_alias, second_alias)

        # generate temp table to store distinct vals
        first_temp_name = self.create_tmp_table_for_attr(first)
        second_temp_name = self.create_tmp_table_for_attr(second)

        rel_strs = []
        rel_strs.append('{} AS {}'.format(first_temp_name, first_rel_alias))
        rel_strs.append('{} AS {}'.format(second_temp_name, second_rel_alias))

        from_str = ', '.join(rel_strs)

        query = 'SELECT DISTINCT {} FROM {} WHERE {}'.format(proj_str, from_str, pred_str)

        cursor = self.cursor()
        cursor.execute(query)
        values = set()
        for r in cursor:
            if r[0] is not None:
                values.add(r[0])
        cursor.close()

        if values:
            return AttributeIntersect('text', vals=values)
        else:
            return None

    def find_num_intersects(self, first, second):
        larger_min = max(first.min, second.min)
        smaller_max = min(first.max, second.max)

        if larger_min <= smaller_max:
            return AttributeIntersect('num', min=larger_min, max=smaller_max)
        else:
            return None

    def find_intersects(self, first, second):
        if first.type != second.type:
            return None
        if first.type == 'text' and second.type == 'text':
            return self.find_text_intersects(first, second)
        elif first.type == 'num' and second.type == 'num':
            return self.find_num_intersects(first, second)

    def get_attr_range(self, rel_name, attr):
        cursor = self.cursor()
        cursor.execute('SELECT MIN({}), MAX({}) FROM {}'.format(attr.name, attr.name, rel_name))
        row = cursor.fetchone()
        cursor.close()
        return row[0], row[1]

    def get_relations(self):
        return self.relations

    def execute_sql(self, sql):
        cursor = self.cursor()
        try:
            cursor.execute(sql)

            query_tuples = set()
            for result in cursor:
                # disallow nulls
                if None in result:
                    continue

                query_tuples.add(result)
            cursor.close()

            return query_tuples
        except Exception as e:
            cursor.close()
            if str(e).startswith('3024'):
                raise Exception('Timeout: Query timed out.')
            raise e

    def execute(self, cq):
        query_str = cq.constrained()

        if cq.timed_out:
            if cq.tuples:
                return cq.tuples, True
            else:
                # find next incremental tuple
                offset_str = query_str + ' LIMIT 1 OFFSET {}'.format(cq.offset)
                cq.offset += 1
                try:
                    query_tuples = self.execute_sql(offset_str)
                except Exception as e:
                    query_tuples = set()

                if query_tuples:
                    cq.cached = True
                    cq.tuples = query_tuples
                    return query_tuples, False
                else:
                    # if none, treat as invalid CQ
                    cq.cached = True
                    cq.timed_out = False
                    cq.tuples = set()
                    return cq.tuples, False
        elif cq.cached:
            if cq.within_cache_constraints():
                return cq.tuples, True

        try:
            query_tuples = self.execute_sql(query_str)

            cq.cached = True
            cq.cache_constraints = cq.constraints
            cq.tuples = query_tuples

            return query_tuples, False
        except Exception as e:
            if str(e).startswith('Timeout'):
                if cq.timed_out:
                    # if already timed out and this is verification query
                    # treat as invalid CQ
                    cq.cached = True
                    cq.timed_out = False
                    cq.tuples = set()
                    return cq.tuples, False
                else:
                    # reexecute CQ
                    cq.timed_out = True
                    return self.execute(cq)
            else:
                raise e
