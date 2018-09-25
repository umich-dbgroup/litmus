__all__ = ['SQLParser']

import os
import pickle
import re
import time
import traceback

from moz_sql_parser import parse
from tqdm import tqdm

class SQLParser(object):
    def __init__(self, cache_path):
        self.cache_path = cache_path
        self.load_cache()

    def load_cache(self):
        if os.path.exists(self.cache_path):
            self.cache = pickle.load(open(self.cache_path, 'rb'))
        else:
            self.cache = {}

    def update_cache(self, query):
        self.cache[query.query_str] = query
        pickle.dump(self.cache, open(self.cache_path, 'wb'))

    def parse_one(self, cqid, query_str):
        if query_str in self.cache:
            return self.cache[query_str], True

        parsed = parse(query_str)

        projs = []
        if isinstance(parsed['select'], dict):
            parsed['select'] = [parsed['select']]
        for sel in parsed['select']:
            if 'distinct' in sel['value']:
                projs.append(sel['value']['distinct'])
            else:
                projs.append(sel['value'])

        preds = []
        if 'where' in parsed:
            for op, vals in parsed['where'].items():
                preds.append((op, vals))

        query = Query(cqid, query_str, projs, preds)

        self.update_cache(query)
        return query, False

    def parse_many(self, query_strs):
        print("Parsing queries...")
        start = time.time()

        bar = tqdm(total=len(query_strs), desc='Parsing queries')

        queries = {}
        from_cache = 0
        errors = []
        for cqid, query_str in query_strs.items():
            try:
                query, cached = self.parse_one(cqid, query_str)
                if cached:
                    from_cache += 1
                queries[cqid] = query
            except Exception as e:
                print(query_str)
                print(traceback.format_exc())
                errors.append(cqid)
            bar.update(1)
        bar.close()
        parse_time = time.time() - start
        print("From cache: {}/{}".format(from_cache, len(query_strs)))
        print("Parse errors: {}/{}".format(len(errors), len(query_strs)))
        print("Done parsing [{}s]".format(parse_time))
        return queries, parse_time

class Query(object):
    def __init__(self, cqid, query_str, projs, preds):
        self.cqid = cqid
        self.query_str = query_str
        self.projs = projs
        self.preds = preds

    def constrain(self, qig):
        query_str = self.query_str

        v = qig.get_vertex(self.cqid)

        constraints = []

        for pos, proj in enumerate(self.projs):
            pos_constraints = []
            for adj in v.get_adjacent():
                e = v.get_edge(adj)

                intersect = e.meta[pos]['intersect']

                if not intersect.is_empty():
                    if intersect.type == 'num':
                        pos_constraints.append(u'({} >= {} AND {} <= {})'.format(proj, intersect.min, proj, intersect.max))
                    elif intersect.type == 'text':
                        if intersect.is_all():
                            # if any intersects are ALL, just skip the rest
                            pos_constraints = []
                            break
                        else:
                            pos_constraints.append(u"({} IN ('{}'))".format(proj, u"','".join([v.replace("'", "''") for v in intersect.vals])))

                if pos_constraints:
                    constraints.append("({})".format(" OR ".join(pos_constraints)))

        if constraints:
            if 'where' not in query_str.lower():
                query_str += u' WHERE '
            else:
                query_str += u' AND '

            query_str += u' AND '.join(constraints)
        return query_str

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
        return result

    # @staticmethod
    # def narrow_all(db, part_set, colnum, top_n_overlaps, queries):
    #     cur_queries = dict(queries)
    #     results = {}
    #     attrs = ProjPartition.attrs_from_overlaps(top_n_overlaps)
    #     if not attrs:
    #         raise Exception('Error! No attrs found in column {}'.format(colnum))
    #     for attr in attrs:
    #         cq_infos = part_set.attrs_to_cqs[colnum][attr]
    #         for cqid, proj in cq_infos:
    #             if cqid in cur_queries:
    #                 orig = cur_queries[cqid]
    #                 new_query = Query.narrow_to_overlaps(db, orig, proj, top_n_overlaps)
    #
    #                 results[cqid] = new_query
    #     return results

    # @staticmethod
    # def narrow_to_overlaps(db, query, proj, overlaps):
    #     query_str = query.query_str
    #
    #     attr = db.get_attr(proj)
    #
    #     # if any equality predicates in query for attr, skip
    #     for pred in query.preds:
    #         for val in pred[1]:
    #             if pred[0] == 'eq' and db.get_attr(val) == attr:
    #                 return query
    #
    #     # TODO: if there are range preds in query that are disjoint with overlaps, skip
    #
    #     limited_strs = []
    #     for ov in overlaps:
    #         if isinstance(ov, NumInterval):
    #             limited_strs.append(u'({} >= {} AND {} <= {})'.format(proj, ov.min, proj, ov.max))
    #         elif isinstance(ov, TextIntersect):
    #             if ov.values is not None:
    #                 ov.values = sorted(ov.values)
    #                 limited_strs.append(u"{} IN ('{}')".format(proj, u"','".join([v.replace("'", "''") for v in ov.values])))
    #
    #     limited_strs = sorted(limited_strs)
    #
    #     if limited_strs:
    #         if 'where' not in query_str.lower():
    #             query_str += u' WHERE '
    #         else:
    #             query_str += u' AND '
    #         limited_str = u" OR ".join(limited_strs)
    #         query_str += u'({})'.format(limited_str)
    #
    #     return Query(query_str, query.projs, query.preds)
