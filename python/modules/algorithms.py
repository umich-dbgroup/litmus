from __future__ import division, print_function

import random
import sys
import time
import traceback

from collections import OrderedDict
from itertools import combinations
from Queue import PriorityQueue
from tqdm import tqdm

from query import Query
from partitions import PartSet
from qig import QIGByType, QIGByRange

TOP_TUPLES = 5

class Base(object):
    def __init__(self, db, aig=None, info=None, constrain=False):
        self.db = db
        self.aig = aig
        self.info = info
        self.constrain = constrain

    def execute(self, cqs):
        result_meta = {
            'objective': 0,
            'total_cq': len(cqs),
            'query_time': 0,
            'comp_time': 0
        }
        return None, None, result_meta

    def run_cqs(self, cqs, msg_append='', qig=None, constrain=False):
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

                if constrain:
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

    def objective(self, Q, S):
        S_dict = {}
        diff = {}
        for cqid, cq in Q.items():
            if cqid in S:
                S_dict[cqid] = cq
            else:
                diff[cqid] = cq
        return abs(sum(q.w for q in S_dict.values()) - sum(q.w for q in diff.values()))

    def calc_objectives(self, Q, T):
        print("Calculating objective values...")
        start = time.time()

        objectives = {}
        for t, S in T.items():
            objectives[t] = self.objective(Q, S)

        sorted_objectives = OrderedDict(sorted(objectives.items(), key=lambda t: t[1]))
        objective_time = time.time() - start
        print("Done calculating objectives [{}s]".format(objective_time))
        return sorted_objectives, objective_time

    def min_objective_tuples(self, Q, T, objectives, check_Q):
        print('Finding min objective tuples, including timed out queries...')
        start = time.time()
        for t, objective in objectives.items():
            S = T[t]

            # check if t exists in any queries not yet executed/timed out
            for cqid in check_Q:
                if Query.tuple_in_query(self.db, t, Q[cqid]):
                    S.add(cqid)

            # recalculate objective for this
            objectives[t] = self.objective(Q, S)

            if S == Q.keys():
                # remove this tuple from all CQ caches
                for cqid in S:
                    Q[cqid].tuples.discard(t)
            else:
                break

        # resort
        objectives = OrderedDict(sorted(objectives.items(), key=lambda t: t[1]))

        min_obj_time = time.time() - start
        print('Done finding min objective tuples. [{}s]'.format(min_obj_time))
        return objectives, min_obj_time

    def print_tuple(self, Q, t, S):
        print("{}, Objective: {}, # CQs: {}".format(t, self.objective(Q, S), len(S)))

    def print_best_tuples(self, Q, objectives, T, k):
        print("Top {} tuples:".format(k))
        for t, objective in objectives.items()[0:k]:
            self.print_tuple(Q, t, T[t])

    # def dist_by_count(self, Q_len, C_t):
    #     p_t = C_t / Q_len
    #
    #     return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t))
    #
    # def dist(self, cqs, t, cqids):
    #     Q_len = len(cqs)
    #     C_t = len(cqids)
    #     # TODO: when probabilities are unequal, p_t should be changed
    #     p_t = C_t / Q_len
    #
    #     return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t))

    # def calc_dists(self, cqs, tuples):
    #     print("Calculating dist values...")
    #     start = time.time()
    #
    #     tuple_dists = {}
    #     for t, cqids in tuples.items():
    #         tuple_dists[t] = self.dist(cqs, t, cqids)
    #
    #     sorted_tuple_dists = OrderedDict(sorted(tuple_dists.items(), key=lambda t: t[1], reverse=True))
    #     dist_time = time.time() - start
    #     print("Done calculating dists [{}s]".format(dist_time))
    #     return sorted_tuple_dists, dist_time

    # def max_dist_tuples(self, cqs, tuples, sorted_dists, check_queries):
    #     print('Evaluating distinguishability, including timed out queries...')
    #     start = time.time()
    #     for t, dist in sorted_dists.items():
    #         cqids = tuples[t]
    #
    #         # check if t exists in any queries not yet executed/timed out
    #         for check_cqid in check_queries:
    #             if Query.tuple_in_query(self.db, t, cqs[check_cqid]):
    #                 cqids.add(check_cqid)
    #
    #         # recalculate dist for this
    #         sorted_dists[t] = self.dist(cqs, t, cqids)
    #
    #         if sorted_dists[t] == 0:
    #             # remove this tuple from all CQ caches
    #             for cqid in cqids:
    #                 cqs[cqid].tuples.discard(t)
    #
    #         else:
    #             break
    #
    #     # resort
    #     sorted_dists = OrderedDict(sorted(sorted_dists.items(), key=lambda t: t[1], reverse=True))
    #
    #     max_dist_time = time.time() - start
    #     print('Done evaluating distinguishability. [{}s]'.format(max_dist_time))
    #     return sorted_dists, max_dist_time

    def print_stats(self, cq_count, timeout_count, sql_errors, valid_count, cached_count):
        print('Executed CQs: {}'.format(cq_count))
        print('From Cache: {}'.format(cached_count))
        print('Timed Out CQs: {}'.format(timeout_count))
        print('SQL Error CQs: {}'.format(sql_errors))
        print('Valid CQs: {}'.format(valid_count))

    # def print_dist(self, tuple, dist_val, cqids):
    #     print("{}, Dist: {}, # CQs: {}".format(tuple, dist_val, len(cqids)))
    #
    # def print_top_dists(self, sorted_dists, tuples, k):
    #     print("Top {} tuples:".format(k))
    #     for t, dist_val in sorted_dists.items()[0:k]:
    #         self.print_dist(t, dist_val, tuples[t])

# class Partition(Base):
#     def part_max_dist(self, part_set, part):
#         total_cqs = part_set.cq_count()
#         if len(part) <= (total_cqs / 2):
#             return self.dist_by_count(total_cqs, len(part))
#         else:
#             return self.dist_by_count(total_cqs, int(total_cqs / 2))
#
#     def calc_future_part_max_dist(self, part_set, part):
#         future_part_max_dist = 0
#
#         i = part_set.index(part)
#         for j in range(i+1, len(part_set)):
#             max_dist = self.part_max_dist(part_set, part_set[j][1])
#             if max_dist > future_part_max_dist:
#                 future_part_max_dist = max_dist
#         return future_part_max_dist
#
#     def need_check_future_part(self, part_set, part, cqids):
#         i = part_set.index(part)
#         for j in range(i+1, len(part_set)):
#             if all(cqid in part.cqs.keys() for cqid in cqids):
#                 diff = set(part.cqs.keys()) - set(cqids)
#                 if any(cqid not in part_set.executed for cqid in diff):
#                     return True
#         return False
#
#     def find_part_max_dist(self, part_set, part, tuples, timed_out):
#         max_dist = 0
#         result = []
#
#         for t, cqids in tuples.items():
#             # exclude if requires executing another partition
#             if self.need_check_future_part(part_set, part, cqids):
#                 continue
#
#             d = self.dist(part_set.cqs, t, cqids)
#
#             if d > max_dist:
#                 # check if t exists in any queries timed out
#                 for check_cqid in timed_out:
#                     if Query.tuple_in_query(self.db, t, part.cqs[check_cqid]):
#                         cqids.add(check_cqid)
#                 d = self.dist(part_set.cqs, t, cqids)
#
#             if d > max_dist:
#                 max_dist = d
#                 result = [(t, cqids, d)]
#             elif d == max_dist:
#                 result.append((t, cqids, d))
#
#         return result
#
#     def find_optimal_tuples(self, part_set, part, max_tuples):
#         future_part_max_dist = self.calc_future_part_max_dist(part_set, part)
#
#         max_dist = 0
#         result = []
#         for t, cqids, d in max_tuples:
#             # exclude if not exceed future partition max dist
#             if d < future_part_max_dist:
#                 continue
#             if d > max_dist:
#                 max_dist = d
#                 result = [(t, cqids, d)]
#             elif d == max_dist:
#                 result.append((t, cqids, d))
#
#         return result
#
#     def run_part(self, part_key, part, constrain=False, qig=None):
#         if constrain:
#             return self.run_cqs(part.cqs, msg_append=' ' + part_key, qig=qig)
#         else:
#             return self.run_cqs(part.cqs, msg_append=' ' + part_key)
#
#     def print_partitions(self, part_set):
#         print('\n=== PARTITIONS ===')
#         for k, v in part_set.parts.items():
#             if self.info == 'range':
#                 print('{}, Count: {}, CQs: {}'.format(k, len(v), v.meta['cqids']))
#             else:
#                 print('{}, Count: {}'.format(k, len(v)))
#         print()
#
#     def get_max_tuples(self, T):
#         max_dist = 0
#         results = []
#         for t, cqids, d in T:
#             if d > max_dist:
#                 max_dist = d
#                 results = [(t, cqids, d)]
#             elif d == max_dist:
#                 results.append((t, cqids, d))
#         return results
#
#     def execute(self, cqs):
#         print('Constructing QIG with information: {}'.format(self.info))
#         start = time.time()
#         if hasattr(self, 'qig'):
#             self.qig.update(cqs)
#         else:
#             if self.info == 'type':
#                 self.qig = QIGByType(self.db, cqs)
#             elif self.info == 'range':
#                 self.qig = QIGByRange(self.db, cqs, self.aig)
#         qig_time = time.time() - start
#         print("Done constructing QIG [{}s]".format(qig_time))
#
#         print('Partitioning (maximal cliques + sort)...')
#         start = time.time()
#         part_set = self.qig.find_partition_set()
#         part_set.sort(self.part_sort)
#         partition_time = time.time() - start
#         print('Done partitioning [{}s]'.format(partition_time))
#
#         total_exec_cqs = 0
#         total_valid_cqs = 0
#         total_timeout_cqs = 0
#         total_error_cqs = 0
#         total_query_time = 0
#
#         self.print_partitions(part_set)
#
#         tuple_find_time = 0
#         T_prev = None
#         for part_key, part in part_set:
#             tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_part(part_key, part, constrain=self.constrain, qig=self.qig)
#             part_set.update_executed(part.cqs.keys())
#
#             total_exec_cqs += len(part.cqs)
#             total_valid_cqs += len(valid_cqs)
#             total_timeout_cqs += len(timed_out)
#             total_error_cqs += len(sql_errors)
#             total_query_time += query_time
#
#             start = time.time()
#             T_max = self.find_part_max_dist(part_set, part, tuples, timed_out)
#             tuple_find_time += time.time() - start
#
#             if T_prev:
#                 T_max.extend(T_prev)
#
#             if T_max:
#                 if self.greedy:
#                     print('Returning found tuples with Greedy approach...')
#                     T_prev = T_max
#                     break
#
#                 start = time.time()
#                 T_opt = self.find_optimal_tuples(part_set, part, T_max)
#                 tuple_find_time += time.time() - start
#
#                 if T_opt:
#                     T_prev = T_opt
#                     break
#
#                 start = time.time()
#                 T_prev = self.get_max_tuples(T_max)
#                 tuple_find_time += time.time() - start
#
#             print('Optimal tuples not found, executing next partition...')
#
#         max_dist = 0
#         max_tuple = None
#         max_tuple_cqids = None
#         dist_time = 0
#         if T_prev:
#             for t, cqids, d in T_prev[0:TOP_TUPLES]:
#                 self.print_dist(t, d, cqids)
#
#             max_tuple, max_tuple_cqids, max_dist = T_prev[0]
#
#         comp_time = qig_time + partition_time + tuple_find_time
#
#         result_meta = {
#             'dist': max_dist,
#             'total_cq': len(cqs),
#             'exec_cq': total_exec_cqs,
#             'valid_cq': total_valid_cqs,
#             'timeout_cq': total_timeout_cqs,
#             'error_cq': total_error_cqs,
#             'query_time': total_query_time,
#             'comp_time': comp_time
#         }
#         return max_tuple, max_tuple_cqids, result_meta

class GreedyAll(Base):
    def execute(self, Q):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(Q)

        t_hat = None
        t_hat_cqids = None
        min_objective = 0
        comp_time = 0
        if tuples:
            objectives, objective_time = self.calc_objectives(Q, tuples)
            comp_time += objective_time
            objectives, min_objective_time = self.min_objective_tuples(Q, tuples, objectives, timed_out)
            comp_time += min_objective_time
            self.print_best_tuples(Q, objectives, tuples, TOP_TUPLES)

            t_hat, min_objective = objectives.items()[0]
            t_hat_cqids = tuples[t_hat]

            # if we are working with timed out queries, just run the iteration again until you come up with a tuple that distinguishes at least one CQ
            if t_hat_cqids == Q.keys() and timed_out:
                t_hat, t_hat_cqids, r_meta = self.execute(Q)
                query_time += r_meta['query_time']
                comp_time += r_meta['comp_time']
                min_objective = r_meta['objective']

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'query_time': query_time,
            'comp_time': comp_time
        }
        return t_hat, t_hat_cqids, result_meta

class GreedyBB(GreedyAll):
    def branch(self, Q, C, tuples):
        results = []
        for cqids in set(frozenset(item) for item in tuples.values()):
            X = set(cqids)
            for C_i in C:
                if cqids < C_i:
                    X |= C_i
            results.append((self.bound(Q, cqids), set(cqids), X))
        return results

    def bound(self, Q, S):
        S_dict = {}
        diff = {}
        for cqid, cq in Q.items():
            if cqid in S:
                S_dict[cqid] = cq
            else:
                diff[cqid] = cq

        S_w = sum(q.w for q in S_dict.values())
        diff_w = sum(q.w for q in diff.values())
        if diff_w >= S_w:
            return diff_w - S_w
        else:
            return min(self.bound(Q, C) for C in combinations(S, len(S) - 1))

    def tuples_in_all_cqs(self, tuples, S):
        results = {}
        for tuple, cqids in tuples.items():
            if cqids == S:
                results[tuple] = cqids
        return results

    def construct_qig(self, Q):
        print('Constructing QIG with information: {}'.format(self.info))
        start = time.time()
        if hasattr(self, 'qig'):
            self.qig.update(Q)
        else:
            if self.info == 'type':
                self.qig = QIGByType(self.db, Q)
            elif self.info == 'range':
                self.qig = QIGByRange(self.db, Q, self.aig)
        qig_time = time.time() - start
        print("Done constructing QIG [{}s]".format(qig_time))
        return qig_time

    def find_maximal_cliques(self):
        print('Finding maximal cliques...')
        start = time.time()
        C = self.qig.find_maximal_cliques()
        clique_time = time.time() - start
        print('Done finding maximal cliques [{}s]'.format(clique_time))
        return C, clique_time

    def set_to_dict(self, Q, S):
        S_dict = {}
        for s in S:
            S_dict[s] = Q[s]
        return S_dict

    def execute(self, Q):
        qig_time = self.construct_qig(Q)
        P = PriorityQueue()
        C, clique_time = self.find_maximal_cliques()

        for i, c in enumerate(C):
            print('Clique {}: {}'.format(i,c))
            P.put((self.bound(Q, c), c, c))

        T_hat = {}
        v_hat = 99999999999

        total_query_time = 0

        while not P.empty():
            (B, S, X) = P.get()
            if B > v_hat:
                continue

            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, X), qig=self.qig, constrain=self.constrain)
            total_query_time += query_time

            T = self.tuples_in_all_cqs(tuples, S)
            if T:
                T_hat = T
                v_hat = self.objective(Q, S)

            print('Branching..')
            for item in self.branch(Q, C, tuples):
                P.put(item)
            continue

        min_objective = 0
        t_hat = None
        t_hat_cqids = None
        dist_time = 0
        if T:
            for t, S in T.items():
                self.print_tuple(Q, t, S)

            t_hat, t_hat_cqids = T.items()[0]
            min_objective = self.objective(Q, t_hat_cqids)

        comp_time = qig_time + clique_time + total_query_time

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'query_time': total_query_time,
            'comp_time': comp_time
        }
        return t_hat, t_hat_cqids, result_meta

class GreedyGuess(GreedyBB):
    def tuples_not_in_future_cliques(self, C, tuples, i):
        results = {}
        for t, cqids in tuples.items():
            exclude = False
            for j in range(i + 1, len(C)):
                if cqids <= C[j]:
                    exclude = True
                    break
            if not exclude:
                results[t] = cqids
        return results

    def execute(self, Q):
        qig_time = self.construct_qig(Q)
        C, clique_time = self.find_maximal_cliques()

        C_list = []

        for i, c in enumerate(C):
            print('Clique {}: {}'.format(i, c))
            C_list.append((self.bound(Q, c), c))
        C_list.sort()

        total_query_time = 0

        for i, C_i in enumerate(C_list):
            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, C_i), qig=self.qig, constrain=self.constrain)
            total_query_time += query_time

            T = self.tuples_not_in_future_cliques(C, tuples, i)
            if T:
                objectives, calc_objective_time = self.calc_objectives(Q, T)
                objectives, min_objective_time = self.min_objective_tuples(Q, T, objectives, timed_out)

                t_hat, min_objective = objectives.items()[0]
                t_hat_cqids = tuples[t_hat]

                result_meta = {
                    'objective': min_objective,
                    'total_cq': len(Q),
                    'query_time': total_query_time,
                    'comp_time': calc_objective_time + min_objective_time
                }
                return t_hat, t_hat_cqids, result_meta


class Random(Base):
    def execute(self, Q):
        exec_cqs = []
        valid_cqs = []
        timed_out = []
        sql_errors = []
        cached = []

        print("Executing random CQs until 1 random tuple found...")
        r_keys = list(Q.keys())
        random.shuffle(r_keys)

        result = None
        start = time.time()
        while r_keys:
            cqid = r_keys.pop()
            cq = Q[cqid]
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

        t_hat = None
        t_hat_cqids = None
        min_objective = 0
        dist_time = 0
        min_objective_time = 0
        if result:
            tuples = {}
            tuples[result] = list(valid_cqs)
            objectives, calc_objective_time = self.calc_objectives(Q, tuples)

            # need to check not-yet-executed queries and any timed-out queries
            check_queries = list(r_keys)
            check_queries.extend(timed_out)

            objectives, min_objective_time = self.min_objective_tuples(Q, tuples, objectives, check_queries)
            self.print_best_tuples(Q, objectives, tuples, TOP_TUPLES)
            t_hat, min_objective = objectives.items()[0]
            t_hat_cqids = tuples[t_hat]

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'query_time': query_time,
            'comp_time': calc_objective_time + min_objective_time
        }
        return t_hat, t_hat_cqids, result_meta
