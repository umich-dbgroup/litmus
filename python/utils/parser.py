__all__ = ['SQLParser']

import os
import pickle
import time
import traceback

from moz_sql_parser import parse
from tqdm import tqdm

from overlap_types import NumInterval
from text_intersect import TextIntersect
from partition_set import ProjPartition

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

    def parse_one(self, query_str):
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

        query = Query(query_str, projs, preds)

        self.update_cache(query)
        return query, False

    def parse_many(self, query_strs):
        print("Parsing queries...")
        start = time.time()

        bar = tqdm(total=len(query_strs), desc='Parsing queries')

        queries = {}
        from_cache = 0
        errors = []
        for query_id, query_str in query_strs.items():
            try:
                query, cached = self.parse_one(query_str)
                if cached:
                    from_cache += 1
                queries[query_id] = query
            except Exception as e:
                print(query_str)
                print(traceback.format_exc())
                errors.append(query_id)
            bar.update(1)
        bar.close()
        parse_time = time.time() - start
        print("From cache: {}/{}".format(from_cache, len(query_strs)))
        print("Parse errors: {}/{}".format(len(errors), len(query_strs)))
        print("Done parsing [{}s]".format(parse_time))
        return queries, parse_time

class Query(object):
    def __init__(self, query_str, projs, preds):
        self.query_str = query_str
        self.projs = projs
        self.preds = preds

    @staticmethod
    def narrow_all(db, part_set, colnum, top_n_overlaps, queries):
        cur_queries = dict(queries)
        results = {}
        attrs =  ProjPartition.attrs_from_overlaps(top_n_overlaps)
        if not attrs:
            raise Exception('Error! No attrs found in column {}'.format(colnum))
        if len(attrs) == 1:
            # no need to narrow if only one attribute
            return queries
        for attr in attrs:
            cq_infos = part_set.attrs_to_cqs[colnum][attr]
            for cqid, proj in cq_infos:
                if cqid in cur_queries:
                    orig = cur_queries[cqid]
                    new_query = Query.narrow_to_overlaps(db, orig, proj, top_n_overlaps)

                    results[cqid] = new_query
        return results

    @staticmethod
    def narrow_to_overlaps(db, query, proj, overlaps):
        limited_str = query.query_str

        attr = db.get_attr(proj)

        # if any equality predicates in query for attr, skip
        for pred in query.preds:
            for val in pred[1]:
                if pred[0] == 'eq' and db.get_attr(val) == attr:
                    return query

        # TODO: if there are range preds in query that are disjoint with overlaps, skip

        limited_strs = []
        for ov in overlaps:
            if isinstance(ov, NumInterval):
                limited_strs.append(u'({} >= {} AND {} <= {})'.format(proj, ov.min, proj, ov.max))
            elif isinstance(ov, TextIntersect):
                limited_strs.append(u"{} IN ('{}')".format(proj, u"','".join(ov.values)))

        limited_str = u" OR ".join(limited_strs)
        new_query_str = u'{} AND ({})'.format(query.query_str, limited_str)

        return Query(new_query_str, query.projs, query.preds)
