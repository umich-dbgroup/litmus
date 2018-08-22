from __future__ import division, print_function

import sys
import time

from collections import OrderedDict
from progress.bar import ChargingBar

sys.path.append('..')
from utils.parser import Query
from utils.interval import ColumnIntervals

__all__ = ['Base', 'Partition', 'Overlap', 'Exhaustive']

class Base(object):
    def __init__(self, db, parser = None):
        self.db = db
        self.parser = parser

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

        bar = ChargingBar('Running CQs{}'.format(msg_append), max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

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
                    sql_errors += 1
            bar.next()
        bar.finish()
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
        print('Total CQs: {}'.format(cq_count))
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

        type_parts = {}
        for cqid, cq in cqs_parsed.items():
            proj_types = ()
            for proj in cq.projs:
                attr_type = None
                if isinstance(proj, dict):
                    attr_type = 'aggr'
                else:
                    attr_type = self.db.get_attr(proj).type
                proj_types = proj_types + (attr_type,)

            if proj_types not in type_parts:
                type_parts[proj_types] = {}
            type_parts[proj_types][cqid] = cq

        # Assume the largest type is most likely to produce your result
        sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
        type, type_cqs = sorted_type_parts.items()[0]

        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(type_cqs, msg_append=' ' + str(type))
        self.print_stats(len(cqs), timed_out, sql_errors, len(valid_cqs))
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_top_dists(sorted_dists, tuples, 5)

        comp_time = partition_time + dist_time

        return parse_time, query_time, comp_time

class Overlap(Base):
    def execute(self, cqs):
        type_parts = {}       # each type-based partition
        intervals = {}        # ColumnIntervals for each colnum
        attrs_added = {}      # attributes already added for each colnum
        attrs_to_cqs = {}     # stores attrs -> [(cqid, frag)] for each colnum

        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print("Partitioning CQs by type...")
        start = time.time()
        for cqid, cq in cqs_parsed.items():
            proj_types = ()
            for colnum, proj in enumerate(cq.projs):
                attr = self.db.get_attr(proj)
                if isinstance(proj, dict):
                    attr_type = 'aggr'
                    continue
                else:
                    attr_type = attr.type
                proj_types = proj_types + (attr_type,)

                if colnum not in attrs_added:
                    attrs_added[colnum] = set()
                if colnum not in attrs_to_cqs:
                    attrs_to_cqs[colnum] = {}
                if attr not in attrs_to_cqs[colnum]:
                    attrs_to_cqs[colnum][attr] = []
                attrs_to_cqs[colnum][attr].append((cqid, proj))

                if attr.type == 'num':
                    if attr in attrs_added[colnum]:
                        continue

                    if colnum not in intervals:
                        intervals[colnum] = ColumnIntervals(colnum)

                    intervals[colnum].add_attr(attr)
                    attrs_added[colnum].add(attr)

            if proj_types not in type_parts:
                type_parts[proj_types] = {}
            type_parts[proj_types][cqid] = cq

        for colnum, ci in intervals.items():
            ci.update()

        # Assume the largest type is most likely to produce your result.
        sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
        type, type_cqs = sorted_type_parts.items()[0]

        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        total_interval_time = 0
        total_query_time = 0
        total_dist_time = 0

        max_intrvls = ColumnIntervals.max_interval_count(intervals.values())
        for n in range(0, max_intrvls):
            print('Rewriting queries for top-{} intervals...'.format(n+1))
            start = time.time()
            narrowed_cqs = dict(type_cqs)
            after_partition_cqs = {}
            for colnum, coltype in enumerate(type):
                if coltype == 'num':
                    top_n = intervals[colnum].top_n(n)

                    for attr in ColumnIntervals.get_all_attrs(top_n):
                        cq_infos = attrs_to_cqs[colnum][attr]
                        for cqid, proj in cq_infos:
                            if cqid in narrowed_cqs:
                                orig = narrowed_cqs[cqid]
                                new_query = Query.limit_by_interval(self.db, orig, proj, top_n)

                                after_partition_cqs[cqid] = new_query

                    narrowed_cqs = after_partition_cqs
            interval_time = time.time() - start
            print('Done rewriting queries [{}s]'.format(interval_time))

            total_interval_time += interval_time

            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(narrowed_cqs, msg_append=' ' + str(type))
            self.print_stats(len(cqs), timed_out, sql_errors, len(valid_cqs))
            sorted_dists, dist_time = self.calc_dists(cqs, tuples)
            self.print_top_dists(sorted_dists, tuples, 5)
            print()

            total_query_time += query_time
            total_dist_time += dist_time

            if tuples:
                break

        comp_time = partition_time + total_interval_time + total_dist_time

        return parse_time, query_time, comp_time

class Exhaustive(Base):
    def execute(self, cqs):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(cqs)
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_stats(len(cqs), timed_out, sql_errors, len(valid_cqs))
        self.print_top_dists(sorted_dists, tuples, 5)

        return 0, query_time, dist_time
