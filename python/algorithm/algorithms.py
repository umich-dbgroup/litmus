from __future__ import division, print_function

import time

from collections import OrderedDict
from progress.bar import ChargingBar

__all__ = ['Base', 'ByType', 'ByTypeRange', 'Exhaustive']

class Base:
    def __init__(self, db, parser = None):
        self.db = db
        self.parser = parser

    def execute(self, cqs):
        tuples, valid_cqs, timed_out, query_time = self.run_cqs(cqs)

        sorted_tuples = OrderedDict(sorted(tuples.items(), key=lambda t: len(t[1]), reverse=True))

        self.print_stats(len(cqs), timed_out, len(valid_cqs))

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
        tuples = {}

        bar = ChargingBar('Running CQs{}'.format(msg_append), max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

        start = time.time()
        for cqid, cq in cqs.items():
            try:
                cq_tuples = self.db.execute(cq)

                if len(cq_tuples) > 0:
                    valid_cqs.append(cqid)

                    for t in cq_tuples:
                        if t not in tuples:
                            tuples[t] = []
                        tuples[t].append(cqid)
            except Exception, exc:
                if str(exc).startswith('Timeout'):
                    timed_out += 1
            bar.next()
        bar.finish()
        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        return tuples, valid_cqs, timed_out, query_time

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

    def print_stats(self, cq_count, timeout_count, valid_count):
        print('Total CQs: {}'.format(cq_count))
        print('Timed Out CQs: {}'.format(timeout_count))
        print('Valid CQs: {}'.format(valid_count))

    def print_top_dists(self, sorted_dists, tuples, k):
        print("Top {} tuples:".format(k))
        for t, dist_val in sorted_dists.items()[0:k]:
            print("{}, Dist: {}, # CQs: {}".format(t, dist_val, len(tuples[t])))

class ByType(Base):
    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print("Partitioning CQs by type...")
        start = time.time()

        type_parts = {}
        for cqid, cq_proj in cqs_parsed.items():
            cq, parsed = cq_proj
            projs, preds = parsed

            proj_types = ()
            for proj in projs:
                proj_types = proj_types + (self.db.get_attr(proj).type,)

            if proj_types not in type_parts:
                type_parts[proj_types] = {}
            type_parts[proj_types][cqid] = cq

        # Assume the largest type is most likely to produce your result
        sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
        type, type_cqs = sorted_type_parts.items()[0]

        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        tuples, valid_cqs, timed_out, query_time = self.run_cqs(type_cqs, msg_append=' ' + str(type))
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_top_dists(sorted_dists, tuples, 5)

        comp_time = partition_time + dist_time

        return parse_time, query_time, comp_time

class ByTypeRange(Base):
    def execute(self, cqs):
        # assume all projections have the same length (for now)
        type_parts = {}

        attrs_added = set()

        # stores "transitions" to be able to find overlapping intervals later
        numeric_transitions = {}

        # for each colnum, stores attrs -> [(cqid, frag)]
        attrs_to_cqs = {}

        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print("Partitioning CQs by type and numeric range...")
        start = time.time()
        for cqid, cq_proj in cqs_parsed.items():
            cq, parsed = cq_proj
            projs, preds = parsed

            proj_types = ()
            for colnum, proj in enumerate(projs):
                attr = self.db.get_attr(proj)
                proj_types = proj_types + (attr.type,)

                if colnum not in attrs_to_cqs:
                    attrs_to_cqs[colnum] = {}
                if attr not in attrs_to_cqs[colnum]:
                    attrs_to_cqs[colnum][attr] = []
                attrs_to_cqs[colnum][attr].append((cqid, proj))

                if attr.type == 'num':
                    if attr in attrs_added:
                        continue
                    if colnum not in numeric_transitions:
                        numeric_transitions[colnum] = []

                    # each transition is (val, 1/-1 for enter/exit, attr)
                    numeric_transitions[colnum].append((attr.min,1,attr))
                    numeric_transitions[colnum].append((attr.max,-1,attr))
                    attrs_added.add(attr)

            if proj_types not in type_parts:
                type_parts[proj_types] = {}
            type_parts[proj_types][cqid] = cq

        partition_counts = []
        numeric_intervals = {}
        for colnum, transitions in numeric_transitions.items():
            print('COLUMN {}'.format(colnum))
            numeric_intervals[colnum] = self.find_overlap_intervals(transitions)
            partition_counts.append(len(numeric_intervals[colnum]))

        # Assume the largest type is most likely to produce your result.
        sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
        type, type_cqs = sorted_type_parts.items()[0]

        # Execute one case: best partition for each col. TODO: generalize?
        narrowed_cqs = type_cqs
        after_partition_cqs = {}
        for colnum, coltype in enumerate(type):
            if coltype == 'num':
                # get best interval
                best_interval, best_attrs = numeric_intervals[colnum][0]

                # get cqs / transform
                for attr in best_attrs:
                    cq_infos = attrs_to_cqs[colnum][attr]
                    for cqid, proj in cq_infos:
                        if cqid in narrowed_cqs:
                            cq = narrowed_cqs[cqid]
                            cq += ' AND {} >= {} AND {} <= {}'.format(proj, best_interval[0], proj, best_interval[1])

                            after_partition_cqs[cqid] = cq

                narrowed_cqs = after_partition_cqs

        partition_time = time.time() - start
        print("Done partitioning [{}s]".format(partition_time))

        tuples, valid_cqs, timed_out, query_time = self.run_cqs(narrowed_cqs, msg_append=' ' + str(type))
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_top_dists(sorted_dists, tuples, 5)

        comp_time = partition_time + dist_time

        return parse_time, query_time, comp_time


    def find_overlap_intervals(self, transitions):
        # https://stackoverflow.com/questions/18373509/efficiently-finding-overlapping-intervals-from-a-list-of-intervals
        sorted_transitions = sorted(transitions, key=lambda x: (x[0], -x[1]))

        # contains ((min, max), [attrs])
        partitions = []

        cur_attrs = []
        cur_min = sorted_transitions[0][0]
        last_pos = sorted_transitions[0][0]
        last_entry = sorted_transitions[0][1]
        for t in sorted_transitions:
            # two types of transitions: new pos, or same pos, entry to exit
            if last_pos != t[0] or last_entry != t[1]:
                # save interval
                partitions.append(((cur_min, t[0]), list(cur_attrs)))
                cur_min = t[0]

            if t[1] == 1:   # entry case
                cur_attrs.append(t[2])
            else:           # exit case
                cur_attrs.remove(t[2])

            last_pos = t[0]
            last_entry = t[1]

        sorted_partitions = sorted(partitions, key=lambda p: -len(p[1]))

        for p in sorted_partitions:
            print('Part: {}, Attrs: {}'.format(p[0], len(p[1])))

        return sorted_partitions

class Exhaustive(Base):
    def execute(self, cqs):
        tuples, valid_cqs, timed_out, query_time = self.run_cqs(cqs)
        sorted_dists, dist_time = self.calc_dists(cqs, tuples)
        self.print_stats(len(cqs), timed_out, len(valid_cqs))
        self.print_top_dists(sorted_dists, tuples, 5)

        return 0, query_time, dist_time
