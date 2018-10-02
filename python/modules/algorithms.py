from __future__ import division, print_function

import random
import sys
import time
import traceback

from collections import OrderedDict
from tqdm import tqdm

from query import Query
from partitions import PartSet
from qig import QIGByType, QIGByRange

TOP_DISTS = 5

class Base(object):
    def __init__(self, db, parser=None, aig=None, part_func=None, part_sort=None, constrain=False, greedy=False):
        self.db = db
        self.parser = parser
        self.aig = aig
        self.part_func = part_func
        self.part_sort = part_sort
        self.constrain = constrain
        self.greedy = greedy

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

    def run_cqs(self, cqs, msg_append='', qig=None):
        valid_cqs = []
        timed_out = []
        sql_errors = []
        tuples = {}
        cached = []

        bar = tqdm(total=len(cqs), desc='Running CQs{}'.format(msg_append))

        start = time.time()
        for cqid, cq in cqs.items():
            try:
                if not isinstance(cq, Query):
                    raise Exception('CQ should be a Query object.')

                if qig:
                    cq.constrain(qig)
                else:
                    cq.unconstrain()

                cq_tuples, was_cached = self.db.execute(cq)
                if was_cached:
                    cached.append(cqid)
                if cq.timed_out:
                    timed_out.append(cqid)

                if cq_tuples:
                    valid_cqs.append(cqid)

                    for t in cq_tuples:
                        if t not in tuples:
                            tuples[t] = set()
                        tuples[t].add(cqid)
            except Exception as e:
                print(traceback.format_exc())
                sql_errors.append(cqid)
            bar.update(1)
        bar.close()
        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        self.print_stats(len(cqs), len(timed_out), len(sql_errors), len(valid_cqs), len(cached))

        return tuples, valid_cqs, timed_out, sql_errors, query_time

    def dist_by_count(self, Q_len, C_t):
        p_t = C_t / Q_len

        return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t))

    def dist(self, cqs, t, cqids):
        Q_len = len(cqs)
        C_t = len(cqids)
        # TODO: when probabilities are unequal, p_t should be changed
        p_t = C_t / Q_len

        return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t))

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

    def max_dist_tuples(self, cqs, tuples, sorted_dists, check_queries):
        print('Evaluating distinguishability, including timed out queries...')
        start = time.time()
        for t, dist in sorted_dists.items():
            cqids = tuples[t]

            # check if t exists in any queries not yet executed/timed out
            for check_cqid in check_queries:
                if Query.tuple_in_query(self.db, t, cqs[check_cqid]):
                    cqids.add(check_cqid)

            # recalculate dist for this
            sorted_dists[t] = self.dist(cqs, t, cqids)

            if sorted_dists[t] == 0:
                # remove this tuple from all CQ caches
                for cqid in cqids:
                    cqs[cqid].tuples.discard(t)

            else:
                break

        # resort
        sorted_dists = OrderedDict(sorted(sorted_dists.items(), key=lambda t: t[1], reverse=True))

        max_dist_time = time.time() - start
        print('Done evaluating distinguishability. [{}s]'.format(max_dist_time))
        return sorted_dists, max_dist_time

    def print_stats(self, cq_count, timeout_count, sql_errors, valid_count, cached_count):
        print('Executed CQs: {}'.format(cq_count))
        print('From Cache: {}'.format(cached_count))
        print('Timed Out CQs: {}'.format(timeout_count))
        print('SQL Error CQs: {}'.format(sql_errors))
        print('Valid CQs: {}'.format(valid_count))

    def print_dist(self, tuple, dist_val, cqids):
        print("{}, Dist: {}, # CQs: {}".format(tuple, dist_val, len(cqids)))

    def print_top_dists(self, sorted_dists, tuples, k):
        print("Top {} tuples:".format(k))
        for t, dist_val in sorted_dists.items()[0:k]:
            self.print_dist(t, dist_val, tuples[t])

class Partition(Base):
    def part_max_dist(self, part_set, part):
        total_cqs = part_set.cq_count()
        if len(part) <= (total_cqs / 2):
            return self.dist_by_count(total_cqs, len(part))
        else:
            return self.dist_by_count(total_cqs, int(total_cqs / 2))

    def calc_future_part_max_dist(self, part_set, part):
        future_part_max_dist = 0

        i = part_set.index(part)
        for j in range(i+1, len(part_set)):
            max_dist = self.part_max_dist(part_set, part_set[j][1])
            if max_dist > future_part_max_dist:
                future_part_max_dist = max_dist
        return future_part_max_dist

    def need_check_future_part(self, part_set, part, cqids):
        i = part_set.index(part)
        for j in range(i+1, len(part_set)):
            if all(cqid in part.cqs.keys() for cqid in cqids):
                diff = set(part.cqs.keys()) - set(cqids)
                if any(cqid not in part_set.executed for cqid in diff):
                    return True
        return False

    def find_part_max_dist(self, part_set, part, tuples, timed_out):
        max_dist = 0
        result = []

        for t, cqids in tuples.items():
            # exclude if requires executing another partition
            if self.need_check_future_part(part_set, part, cqids):
                continue

            d = self.dist(part_set.cqs, t, cqids)

            if d > max_dist:
                # check if t exists in any queries timed out
                for check_cqid in timed_out:
                    if Query.tuple_in_query(self.db, t, part.cqs[check_cqid]):
                        cqids.append(check_cqid)
                d = self.dist(part_set.cqs, t, cqids)

            if d > max_dist:
                max_dist = d
                result = [(t, cqids, d)]
            elif d == max_dist:
                result.append((t, cqids, d))

        return result

    def find_optimal_tuples(self, part_set, part, max_tuples):
        future_part_max_dist = self.calc_future_part_max_dist(part_set, part)

        max_dist = 0
        result = []
        for t, cqids, d in max_tuples:
            # exclude if not exceed future partition max dist
            if d < future_part_max_dist:
                continue
            if d > max_dist:
                max_dist = d
                result = [(t, cqids, d)]
            elif d == max_dist:
                result.append((t, cqids, d))

        return result

    def run_part(self, part_key, part, constrain=False, qig=None):
        if constrain:
            return self.run_cqs(part.cqs, msg_append=' ' + part_key, qig=qig)
        else:
            return self.run_cqs(part.cqs, msg_append=' ' + part_key)

    def print_partitions(self, part_set):
        print('\n=== PARTITIONS ===')
        for k, v in part_set.parts.items():
            if self.part_func == 'range':
                print('{}, Count: {}, CQs: {}'.format(k, len(v), v.meta['cqids']))
            else:
                print('{}, Count: {}'.format(k, len(v)))
        print()

    def get_max_tuples(self, T):
        max_dist = 0
        results = []
        for t, cqids, d in T:
            if d > max_dist:
                max_dist = d
                results = [(t, cqids, d)]
            elif d == max_dist:
                results.append((t, cqids, d))
        return results

    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        print('Constructing QIG with information: {}'.format(self.part_func))
        start = time.time()
        if hasattr(self, 'qig'):
            self.qig.update(cqs_parsed)
        else:
            if self.part_func == 'type':
                self.qig = QIGByType(self.db, cqs_parsed)
            elif self.part_func == 'range':
                self.qig = QIGByRange(self.db, cqs_parsed, self.aig)
        qig_time = time.time() - start
        print("Done constructing QIG [{}s]".format(qig_time))

        print('Partitioning (maximal cliques + sort)...')
        start = time.time()
        part_set = self.qig.find_partition_set()
        part_set.sort(self.part_sort)
        partition_time = time.time() - start
        print('Done partitioning [{}s]'.format(partition_time))

        total_exec_cqs = 0
        total_valid_cqs = 0
        total_timeout_cqs = 0
        total_error_cqs = 0
        total_query_time = 0

        self.print_partitions(part_set)

        tuple_find_time = 0
        T_prev = None
        for part_key, part in part_set:
            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_part(part_key, part, constrain=self.constrain, qig=self.qig)
            part_set.update_executed(part.cqs.keys())

            total_exec_cqs += len(part.cqs)
            total_valid_cqs += len(valid_cqs)
            total_timeout_cqs += len(timed_out)
            total_error_cqs += len(sql_errors)
            total_query_time += query_time

            start = time.time()
            T_max = self.find_part_max_dist(part_set, part, tuples, timed_out)
            tuple_find_time += time.time() - start

            if T_prev:
                T_max.extend(T_prev)

            if T_max:
                if self.greedy:
                    print('Returning found tuples with Greedy approach...')
                    T_prev = T_max
                    break

                start = time.time()
                T_opt = self.find_optimal_tuples(part_set, part, T_max)
                tuple_find_time += time.time() - start

                if T_opt:
                    T_prev = T_opt
                    break

                start = time.time()
                T_prev = self.get_max_tuples(T_max)
                tuple_find_time += time.time() - start

            print('Optimal tuples not found, executing next partition...')

        max_dist = 0
        max_tuple = None
        max_tuple_cqids = None
        dist_time = 0
        if T_prev:
            for t, cqids, d in T_prev[0:TOP_DISTS]:
                self.print_dist(t, d, cqids)

            max_tuple, max_tuple_cqids, max_dist = T_prev[0]

        comp_time = qig_time + partition_time + tuple_find_time

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

        max_tuple = None
        max_tuple_cqids = None
        max_dist = 0
        dist_time = 0
        max_dist_time = 0
        if tuples:
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)
            sorted_dists, max_dist_time = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, timed_out)
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
            'comp_time': dist_time + max_dist_time
        }
        return max_tuple, max_tuple_cqids, result_meta

class Random(Base):
    def execute(self, cqs):
        cqs_parsed, parse_time = self.parser.parse_many(cqs)

        exec_cqs = []
        valid_cqs = []
        timed_out = []
        sql_errors = []
        cached = []

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
                cq.unconstrain()
                cq_tuples, was_cached = self.db.execute(cq)

                if was_cached:
                    cached.append(cqid)

                if len(cq_tuples) > 0:
                    valid_cqs.append(cqid)
                    result = random.choice(list(cq_tuples))
                    break
            except Exception as e:
                if str(e).startswith('Timeout'):
                    timed_out.append(cqid)
                else:
                    print(traceback.format_exc())
                    sql_errors.append(cqid)

        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        self.print_stats(len(exec_cqs), len(timed_out), len(sql_errors), len(valid_cqs), len(cached))

        max_tuple = None
        max_tuple_cqids = None
        max_dist = 0
        dist_time = 0
        max_dist_time = 0
        if result:
            tuples = {}
            tuples[result] = list(valid_cqs)
            sorted_dists, dist_time = self.calc_dists(cqs_parsed, tuples)

            # need to check not-yet-executed queries and any timed-out queries
            check_queries = list(r_keys)
            check_queries.extend(timed_out)

            sorted_dists, max_dist_time = self.max_dist_tuples(cqs_parsed, tuples, sorted_dists, check_queries)
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
            'comp_time': dist_time + max_dist_time
        }
        return max_tuple, max_tuple_cqids, result_meta
