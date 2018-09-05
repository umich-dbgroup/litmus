from __future__ import division, print_function

import random
import sys
import time
import traceback

from collections import OrderedDict
from tqdm import tqdm

sys.path.append('..')
from utils.overlap_types import ColumnNumIntervals, ColumnTextIntersects
from utils.parser import Query
from utils.partition_set import PartitionSet

__all__ = ['Base', 'Partition', 'Overlap', 'Exhaustive']

TOP_DISTS = 5

class Base(object):
    def __init__(self, db, parser=None, tidb=None):
        self.db = db
        self.parser = parser
        self.tidb = tidb

    def execute(self, cqs):
        result_meta = {
            'dist': 0,
            'total_cq': len(cqs),
            'exec_cq': 0,
            'valid_cq': 0,
            'timeout_cq': 0,
            'error_cq': 0,
            'parse_time': 0,
            'query_time': 0,
            'comp_time': 0
        }
        return None, None, result_meta

    def run_cqs(self, cqs, msg_append=''):
        valid_cqs = []
        timed_out = []
        sql_errors = []
        tuples = {}

        bar = tqdm(total=len(cqs), desc='Running CQs{}'.format(msg_append))

        start = time.time()
        for cqid, cq in cqs.items():
            try:
                # unparsed vs. parsed
                query_str = cq
                if isinstance(cq, Query):
                    query_str = cq.query_str

                cq_tuples = self.db.execute(query_str)

                if len(cq_tuples) > 0:
                    valid_cqs.append(cqid)

                    for t in cq_tuples:
                        if t not in tuples:
                            tuples[t] = []
                        tuples[t].append(cqid)
            except Exception as e:
                if str(e).startswith('Timeout'):
                    timed_out.append(cqid)
                else:
                    print(query_str.encode('utf-8')[:1000])
                    print(traceback.format_exc())
                    sql_errors.append(cqid)
            bar.update(1)
        bar.close()
        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        return tuples, valid_cqs, timed_out, sql_errors, query_time

    def dist(self, cqs, t, cqids):
        Q_len = len(cqs)
        C_t = len(cqids)
        # TODO: when probabilities are unequal, p_t should be changed
        p_t = C_t / Q_len

        return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t));

    def calc_dists(self, cqs, tuples):
        print("Calculating dist values...")
        start = time.time()

        tuple_dists = {}
        for t, cqids in tuples.items():
            tuple_dists[t] = self.dist(cqs, t, cqids)

        sorted_tuple_dists = OrderedDict(sorted(tuple_dists.items(), key=lambda t: t[1], reverse=True))
        dist_time = time.time() - start
        print("Done calculating dists [{}s]".format(dist_time))
        return sorted_tuple_dists, dist_time

    def max_dist_tuples(self, cqs, tuples, sorted_dists, check_queries, k):
        print('Evaluating distinguishability, including unexecuted queries (exec time not counted for runtime)...')
        start = time.time()
        for t, dist in sorted_dists.items()[0:k]:
            cqids = tuples[t]

            # check if t exists in any queries not yet executed/timed out
            for check_cqid in check_queries:
                if Query.tuple_in_query(self.db, t, cqs[check_cqid]):
                    cqids.append(check_cqid)

            # recalculate dist for this
            sorted_dists[t] = self.dist(cqs, t, cqids)
        print('Done evaluating distinguishability. [{}s]'.format(time.time() - start))
        return sorted_dists

    def print_stats(self, cq_count, timeout_count, sql_errors, valid_count):
        print('Executed CQs: {}'.format(cq_count))
        print('Timed Out CQs: {}'.format(timeout_count))
        print('SQL Error CQs: {}'.format(sql_errors))
        print('Valid CQs: {}'.format(valid_count))

    def print_top_dists(self, sorted_dists, tuples, k):
        print("Top {} tuples:".format(k))
        for t, dist_val in sorted_dists.items()[0:k]:
            print("{}, Dist: {}, # CQs: {}".format(t, dist_val, len(tuples[t])))

class Partition(Base):
    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print("Partitioning CQs by type...")
        start = time.time()
        part_set = PartitionSet(self.db, cqs_parsed)
        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        total_exec_cqs = 0
        total_valid_cqs = 0
        total_timeout_cqs = 0
        total_error_cqs = 0
        total_query_time = 0
        for type, part in part_set:
            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(part.cqs, msg_append=' ' + str(type))
            self.print_stats(len(part.cqs), len(timed_out), len(sql_errors), len(valid_cqs))

            total_exec_cqs += len(part.cqs)
            total_valid_cqs += len(valid_cqs)
            total_timeout_cqs += len(timed_out)
            total_error_cqs += len(sql_errors)
            total_query_time += query_time

            if not tuples:
                print('No tuples found, executing next partition...')
                continue
            else:
                break

        max_dist = 0
        max_tuple = None
        max_tuple_cqids = None
        dist_time = 0
        if tuples:
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)
            sorted_dists = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, timed_out, TOP_DISTS)
            self.print_top_dists(sorted_dists, tuples, TOP_DISTS)
            max_tuple, max_dist = sorted_dists.items()[0]
            max_tuple_cqids = tuples[max_tuple]

        comp_time = partition_time + dist_time

        result_meta = {
            'dist': max_dist,
            'total_cq': len(cqs),
            'exec_cq': total_exec_cqs,
            'valid_cq': total_valid_cqs,
            'timeout_cq': total_timeout_cqs,
            'error_cq': total_error_cqs,
            'parse_time': parse_time,
            'query_time': total_query_time,
            'comp_time': comp_time
        }
        return max_tuple, max_tuple_cqids, result_meta

class Overlap(Base):
    def execute(self, cqs):
        type_parts = {}       # each type-based partition

        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print("Partitioning CQs by type...")
        start = time.time()
        part_set = PartitionSet(self.db, cqs_parsed)
        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        print("Find overlaps...")
        start = time.time()
        part_set.find_overlaps(self.tidb)
        overlap_time = time.time() - start
        print("Done finding overlaps [{}s]".format(overlap_time))

        total_interval_time = 0
        total_query_time = 0

        tuples = None
        total_exec_cqs = 0
        total_timeout_cqs = 0
        total_error_cqs = 0
        total_valid_cqs = 0

        for type, part in part_set:
            print('Executing partition {}...'.format(type))

            all_timed_out = []
            all_sql_errors = []
            for n in range(1, min(4, part.max_overlap_count())):
                print('Rewriting queries for top-{} overlaps...'.format(n))
                start = time.time()
                cur_cqs = dict(part.cqs)

                for cqid in all_timed_out:
                    cur_cqs.pop(cqid, None)

                for cqid in all_sql_errors:
                    cur_cqs.pop(cqid, None)

                if not cur_cqs:
                    print('No CQs to execute...')
                    break

                for colnum, coltype in enumerate(type):
                    top_n = part.top_n_col_overlaps(n, colnum)
                    if top_n is not None:
                        cur_cqs = Query.narrow_all(self.db, part_set, colnum, top_n, cur_cqs)
                interval_time = time.time() - start
                print('Done rewriting queries [{}s]'.format(interval_time))

                total_interval_time += interval_time

                tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cur_cqs, msg_append=' ' + str(type))
                self.print_stats(len(cur_cqs), len(timed_out), len(sql_errors), len(valid_cqs))

                all_timed_out.extend(timed_out)
                all_sql_errors.extend(sql_errors)

                total_exec_cqs += len(cur_cqs)
                total_timeout_cqs += len(timed_out)
                total_error_cqs += len(sql_errors)
                total_valid_cqs += len(valid_cqs)
                total_query_time += query_time

                if tuples:
                    break
                else:
                    print('No tuples found, expanding overlaps...')

            if tuples:
                break
            else:
                print('No tuples found, executing next partition...')

        dist_time = 0
        max_tuple = None
        max_dist = 0
        if tuples:
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)
            sorted_dists = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, all_timed_out, TOP_DISTS)
            self.print_top_dists(sorted_dists, tuples, TOP_DISTS)
            max_tuple, max_dist = sorted_dists.items()[0]
            max_tuple_cqids = tuples[max_tuple]

        comp_time = partition_time + overlap_time + total_interval_time + dist_time

        result_meta = {
            'dist': max_dist,
            'total_cq': len(cqs),
            'exec_cq': total_exec_cqs,
            'valid_cq': total_valid_cqs,
            'timeout_cq': total_timeout_cqs,
            'error_cq': total_error_cqs,
            'parse_time': parse_time,
            'query_time': total_query_time,
            'comp_time': comp_time
        }
        return max_tuple, max_tuple_cqids, result_meta

class Exhaustive(Base):
    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cqs_parsed)
        self.print_stats(len(cqs), len(timed_out), len(sql_errors), len(valid_cqs))

        max_tuple = None
        max_tuple_cqids = None
        max_dist = 0
        dist_time = 0
        if tuples:
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)
            sorted_dists = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, timed_out, TOP_DISTS)
            self.print_top_dists(sorted_dists, tuples, TOP_DISTS)
            max_tuple, max_dist = sorted_dists.items()[0]
            max_tuple_cqids = tuples[max_tuple]

        result_meta = {
            'dist': max_dist,
            'total_cq': len(cqs),
            'exec_cq': len(cqs),
            'valid_cq': len(valid_cqs),
            'timeout_cq': len(timed_out),
            'error_cq': len(sql_errors),
            'parse_time': parse_time,
            'query_time': query_time,
            'comp_time': dist_time
        }
        return max_tuple, max_tuple_cqids, result_meta

class Random(Base):
    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        exec_cqs = []
        valid_cqs = []
        timed_out = []
        sql_errors = []

        print("Executing random CQs until 1 random tuple found...")
        r_keys = list(cqs_parsed.keys())
        random.shuffle(r_keys)

        result = None
        start = time.time()
        while r_keys:
            cqid = r_keys.pop()
            cq = cqs_parsed[cqid]
            try:
                exec_cqs.append(cqid)
                print(cq.query_str)
                cq_tuples = self.db.execute(cq.query_str)

                if len(cq_tuples) > 0:
                    valid_cqs.append(cqid)
                    result = random.choice(list(cq_tuples))
                    break
            except Exception as e:
                if str(e).startswith('Timeout'):
                    timed_out.append(cqid)
                else:
                    print(cq.query_str.encode('utf-8')[:1000])
                    print(traceback.format_exc())
                    sql_errors.append(cqid)

        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        self.print_stats(len(exec_cqs), len(timed_out), len(sql_errors), len(valid_cqs))

        max_tuple = None
        max_tuple_cqids = None
        max_dist = 0
        dist_time = 0
        if result:
            tuples = {}
            tuples[result] = list(valid_cqs)
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)

            # need to check not-yet-executed queries and any timed-out queries
            check_queries = list(r_keys)
            check_queries.extend(timed_out)

            sorted_dists = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, check_queries, TOP_DISTS)
            self.print_top_dists(sorted_dists, tuples, TOP_DISTS)
            max_tuple, max_dist = sorted_dists.items()[0]
            max_tuple_cqids = tuples[max_tuple]

        result_meta = {
            'dist': max_dist,
            'total_cq': len(cqs),
            'exec_cq': len(exec_cqs),
            'valid_cq': len(valid_cqs),
            'timeout_cq': len(timed_out),
            'error_cq': len(sql_errors),
            'parse_time': parse_time,
            'query_time': query_time,
            'comp_time': dist_time
        }
        return max_tuple, max_tuple_cqids, result_meta
