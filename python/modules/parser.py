__all__ = ['SQLParser']

import os
import pickle
import re
import time
import traceback

from moz_sql_parser import parse
from tqdm import tqdm

from query import Query

class SQLParser(object):
    def __init__(self, db_name, cache_dir):
        self.db_name = db_name
        self.cache_dir = cache_dir
        self.cache = {}

    def cache_path(self, qid):
        return os.path.join(self.cache_dir, '{}.{}.cache'.format(self.db_name, qid))

    def load_cache(self, qid):
        if os.path.exists(self.cache_path(qid)):
            with open(self.cache_path(qid), 'rb') as f:
                self.cache = pickle.load(f)
        else:
            self.cache = {}

    def update_cache(self, query):
        copy = Query(query.cqid, query.query_str, query.projs, query.preds)
        self.cache[query.query_str] = copy

    def flush_cache(self, qid):
        pickle.dump(self.cache, open(self.cache_path(qid), 'wb'))

    def parse_one(self, qid, cqid, query_str):
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

    def parse_many(self, qid, query_strs):
        print("Parsing CQs...")
        start = time.time()

        self.load_cache(qid)

        bar = tqdm(total=len(query_strs), desc='Parsing CQs')

        queries = {}
        from_cache = 0
        errors = []
        for cqid, query_str in query_strs.items():
            try:
                query, cached = self.parse_one(qid, cqid, query_str)
                if cached:
                    from_cache += 1
                queries[cqid] = query
            except Exception as e:
                print(query_str)
                print(traceback.format_exc())
                errors.append(cqid)
            bar.update(1)
        bar.close()

        print('Flushing cache...')
        self.flush_cache(qid)
        print('Done flushing cache.')

        parse_time = time.time() - start
        print("From cache: {}/{}".format(from_cache, len(query_strs)))
        print("Parse errors: {}/{}".format(len(errors), len(query_strs)))
        print("Done parsing [{}s]".format(parse_time))
        return queries
