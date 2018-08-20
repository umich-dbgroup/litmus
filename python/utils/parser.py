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

    def update_cache(self, query, parsed):
        self.cache[query] = parsed
        pickle.dump(self.cache, open(self.cache_path, 'wb'))

    def parse_one(self, query):
        if query in self.cache:
            return self.cache[query], True

        parsed = parse(query)

        projs = []
        for sel in parsed['select']:
            if 'distinct' in sel['value']:
                projs.append(sel['value']['distinct'])
            else:
                projs.append(sel['value'])

        preds = []
        for op, vals in parsed['where'].items():
            preds.append((op, vals))

        self.update_cache(query, (projs, preds))
        return (projs, preds), False

    def parse_many(self, queries):
        print("Parsing queries...")
        start = time.time()

        bar = ChargingBar('Parsing queries', max=len(queries), suffix='%(index)d/%(max)d (%(percent)d%%)')

        query_parsed = {}
        from_cache = 0
        for query_id, query in queries.items():
            parsed, cached = self.parse_one(query)
            if cached:
                from_cache += 1
            query_parsed[query_id] = (query, parsed)
            bar.next()
        bar.finish()
        parse_time = time.time() - start
        print("Done parsing [{}s] (From cache: {}/{})".format(parse_time, from_cache, len(queries)))
        return query_parsed, parse_time
