__all__ = ['SQLParser']

import os
import pickle
import time

from moz_sql_parser import parse
from progress.bar import ChargingBar

class SQLParser:
    def __init__(self, cache_path = 'cache/parser.cache'):
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
        for sel in parsed['select']:
            if 'distinct' in sel['value']:
                projs.append(sel['value']['distinct'])
            else:
                projs.append(sel['value'])

        preds = []
        for op, vals in parsed['where'].items():
            preds.append((op, vals))

        query = Query(query_str, projs, preds)

        self.update_cache(query)
        return query, False

    def parse_many(self, query_strs):
        print("Parsing queries...")
        start = time.time()

        bar = ChargingBar('Parsing queries', max=len(query_strs), suffix='%(index)d/%(max)d (%(percent)d%%)')

        queries = {}
        from_cache = 0
        for query_id, query_str in query_strs.items():
            query, cached = self.parse_one(query_str)
            if cached:
                from_cache += 1
            queries[query_id] = query
            bar.next()
        bar.finish()
        parse_time = time.time() - start
        print("Done parsing [{}s] (From cache: {}/{})".format(parse_time, from_cache, len(query_strs)))
        return queries, parse_time

class Query:
    def __init__(self, query_str, projs, preds):
        self.query_str = query_str
        self.projs = projs
        self.preds = preds

    @staticmethod
    def limit_by_interval(db, query, proj, intervals):
        limited_str = query.query_str

        attr = db.get_attr(proj)

        # if any equality predicates in query, skip
        for pred in query.preds:
            for val in pred[1]:
                if pred[0] == 'eq' and db.get_attr(val) == attr:
                    print('Skipping {}'.format(query.query_str))
                    return query

        # TODO: if there are range preds in query disjoint with intervals, skip

        for interval in intervals:
            limited_str += ' AND {} >= {} AND {} <= {}'.format(proj, interval.min, proj, interval.max)

        return Query(limited_str, query.projs, query.preds)
