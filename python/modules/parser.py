__all__ = ['SQLParser']

import os
import pickle
import time
import traceback

from moz_sql_parser import parse
from tqdm import tqdm

from query import Query

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
        copy = Query(query.cqid, query.query_str, query.projs, query.preds)
        self.cache[query.query_str] = copy
        pickle.dump(self.cache, open(self.cache_path, 'wb'))

    def parse_one(self, cqid, query_str):
        if query_str in self.cache:
            cached = self.cache[query_str]
            cached.cqid = cqid
            return cached, True

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
