from __future__ import division, print_function

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

class Base(object):
    def __init__(self, db, parser=None, tidb=None):
        self.db = db
        self.parser = parser
        self.tidb = tidb

    def execute(self, cqs):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cqs)

        sorted_tuples = OrderedDict(sorted(tuples.items(), key=lambda t: len(t[1]), reverse=True))

        self.print_stats(len(cqs), timed_out, sql_errors, len(valid_cqs))

        if valid_cqs > 0:
            print('Avg. tuples per CQ: {}'.format(len(tuples)/len(valid_cqs)))
            k = 5
            print('Tuples with most CQ overlap: ', end='')
            for tuple, count in sorted_tuples.items()[0:k]:
                print(count, end='; ')
            print()
        return 0, query_time, 0

    def run_cqs(self, cqs, msg_append=''):
        valid_cqs = []
        timed_out = 0
        sql_errors = 0
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
                    timed_out += 1
                else:
                    print(query_str.encode('utf-8'))
                    print(traceback.format_exc())
                    sql_errors += 1
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

        for type, part in part_set:
            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(part.cqs, msg_append=' ' + str(type))
            self.print_stats(len(part.cqs), timed_out, sql_errors, len(valid_cqs))

            if not tuples:
                print('No tuples found, executing next partition...')
                continue
            else:
                break

        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_top_dists(sorted_dists, tuples, 5)

        comp_time = partition_time + dist_time

        return parse_time, query_time, comp_time

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

        for type, part in part_set:
            print('Executing partition {}...'.format(type))
            for n in range(1, part.max_overlap_count()):
                print('Rewriting queries for top-{} overlaps...'.format(n))
                start = time.time()
                cur_cqs = dict(part.cqs)
                for colnum, coltype in enumerate(type):
                    top_n = part.top_n_col_overlaps(n, colnum)
                    cur_cqs = Query.narrow_all(self.db, part_set, colnum, top_n, cur_cqs)
                interval_time = time.time() - start
                print('Done rewriting queries [{}s]'.format(interval_time))

                total_interval_time += interval_time

                tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cur_cqs, msg_append=' ' + str(type))
                self.print_stats(len(cur_cqs), timed_out, sql_errors, len(valid_cqs))
                total_query_time += query_time

                if timed_out == len(cur_cqs):
                    print('All queries timed out...')
                    break
                if tuples:
                    break
                else:
                    print('No tuples found, expanding overlaps...')

            if tuples:
                break
            else:
                print('No tuples found, executing next partition...')

        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_top_dists(sorted_dists, tuples, 5)

        comp_time = partition_time + overlap_time + total_interval_time + dist_time

        return parse_time, total_query_time, comp_time

class Exhaustive(Base):
    def execute(self, cqs):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cqs)
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_stats(len(cqs), timed_out, sql_errors, len(valid_cqs))
        self.print_top_dists(sorted_dists, tuples, 5)

        return 0, query_time, dist_time
