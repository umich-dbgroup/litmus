import re

from database import AttributeIntersect
from numbers import Number

class Query(object):
    def __init__(self, cqid, query_str, projs, preds, w=1):
        self.cqid = cqid
        self.query_str = query_str
        self.projs = projs
        self.preds = preds
        self.w = w

        # cache tuples on self.tuples
        self.cached = False
        # self.cache_constraints = None
        self.tuples = None

        # set flag if timed out
        self.timed_out = False
        # offset for selecting next tuple from timed out query
        self.offset = 0

    def set_w(self, w):
        self.w = w

    def get_cost(self, db):
        if hasattr(self, 'cost'):
            return self.cost
        else:
            cursor = db.cursor()
            cursor.execute('EXPLAIN ' + self.query_str)
            cost = 1
            for row in cursor.fetchall():
                if row[9]:
                    cost *= row[9]
            self.cost = cost
            return self.cost

    def empty_cache(self):
        self.cached = False
        self.tuples = None

    @staticmethod
    def tuple_in_query(db, t, query):
        if len(t) != len(query.projs):
            return False

        query_str = query.query_str

        if 'where' not in query_str.lower():
            query_str += u' WHERE '
        else:
            query_str += u' AND '

        # stores rel_alias_name -> attr_name for projs.
        # if multiple attrs for single rel, keeps the last one
        proj_alias_to_attr = {}
        preds = []
        for i, proj in enumerate(query.projs):
            if isinstance(t[i], unicode) or isinstance(t[i], str):
                preds.append(u"{} = '{}'".format(proj, t[i].replace("'", "''")))
            else:
                preds.append(u"{} = {}".format(proj, t[i]))

            attr = db.get_attr(proj)

            tuple_type = None
            if isinstance(t[i], basestring):
                tuple_type = 'text'
            elif isinstance(t[i], Number):
                tuple_type = 'num'

            if tuple_type is not None and attr.type != tuple_type:
                return False

            if not attr.pk:
                alias, attr_name = proj.split('.')
                proj_alias_to_attr[alias] = attr_name

        query_str += u' AND '.join(preds)
        query_str = re.sub(r'SELECT.*FROM', 'SELECT 1 FROM', query_str)

        for alias, attr_name in proj_alias_to_attr.items():
            index_regex = 'AS ({})'.format(alias)
            query_str = re.sub(index_regex, 'AS \g<1> USE INDEX ({})'.format(attr_name), query_str)

        query_str += ' LIMIT 1'

        try:
            cursor = db.cursor()
            cursor.execute(query_str)
            result = cursor.fetchone() is not None
            cursor.close()
        except Exception as e:
            cursor.close()
            if str(e).startswith('3024'):
                return False
            else:
                print(query_str.encode('utf-8'))
                return False
        return result
