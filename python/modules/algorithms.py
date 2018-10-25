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
BOUND_LIMIT = 15

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
        S_w = 0
        diff_w = 0
        for cqid, cq in Q.items():
            if cqid in S:
                S_w += cq.w
            else:
                diff_w += cq.w
        return abs(S_w - diff_w)

    def calc_objectives(self, Q, T):
        print("Calculating objective values...")
        start = time.time()

        objectives = {}
        memo = {}
        for t, S in T.items():
            S_key = frozenset(S)
            if S_key in memo:
                objectives[t] = memo[S_key]
            else:
                objectives[t] = self.objective(Q, S)
                memo[S_key] = objectives[t]

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
            
    def print_stats(self, cq_count, timeout_count, sql_errors, valid_count, cached_count):
        print('Executed CQs: {}'.format(cq_count))
        print('From Cache: {}'.format(cached_count))
        print('Timed Out CQs: {}'.format(timeout_count))
        print('SQL Error CQs: {}'.format(sql_errors))
        print('Valid CQs: {}'.format(valid_count))

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
            results.append((self.bound(Q, cqids, {}), set(cqids), X))
        return results

    # M is a dictionary for memoizing intermediate results
    def bound(self, Q, S, M):
        S_w = 0
        diff_w = 0
        for cqid, cq in Q.items():
            if cqid in S:
                S_w += cq.w
            else:
                diff_w += cq.w

        S_key = frozenset(S)

        if diff_w >= S_w:
            M[S_key] = diff_w - S_w
        else:
            if len(S) > BOUND_LIMIT:
                return 0

            vals = []
            for C in combinations(S, len(S) - 1):
                C_key = frozenset(C)
                if C_key in M:
                    vals.append(M[C_key])
                else:
                    vals.append(self.bound(Q, C, M))
            M[S_key] = min(vals)
        return M[S_key]

    def tuples_in_all_and_less_cqs(self, tuples, S):
        all_cqs = {}
        less_cqs = {}

        for tuple, cqids in tuples.items():
            if cqids == S:
                all_cqs[tuple] = cqids
            if cqids < S:
                less_cqs[tuple] = cqids
        return all_cqs, less_cqs

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
        P_dups = set()
        C, clique_time = self.find_maximal_cliques()

        for i, c in enumerate(C):
            print('Clique {}: {}'.format(i,c))
            P.put((self.bound(Q, c, {}), c, c))
            P_dups.add(frozenset(c))

        T_hat = {}
        v_hat = 99999999999

        total_query_time = 0
        total_objective_time = 0
        total_branch_time = 0

        while not P.empty():
            (B, S, X) = P.get()

            if B >= v_hat:
                continue

            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, X), qig=self.qig, constrain=self.constrain)
            total_query_time += query_time

            start = time.time()
            T, U = self.tuples_in_all_and_less_cqs(tuples, S)
            if T:
                T_hat = T
                v_hat = self.objective(Q, S)
            total_objective_time += time.time() - start

            if U:
                start = time.time()
                print('Branching..')
                for item in self.branch(Q, C, U):
                    if frozenset(item[1]) not in P_dups:
                        P.put(item)
                        P_dups.add(frozenset(item[1]))
                total_branch_time += time.time() - start

        min_objective = 0
        t_hat = None
        t_hat_cqids = None
        dist_time = 0
        if T_hat:
            for t, S in T_hat.items()[0:TOP_TUPLES]:
                self.print_tuple(Q, t, S)

            t_hat, t_hat_cqids = T_hat.items()[0]
            min_objective = self.objective(Q, t_hat_cqids)

        comp_time = qig_time + clique_time + total_objective_time + total_branch_time

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
                if cqids <= C[j][1]:
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
            C_list.append((self.bound(Q, c, {}), c))
        C_list.sort()

        total_query_time = 0
        future_check_time = 0

        all_tuples = {}

        for i, C_info in enumerate(C_list):
            B, C_i = C_info
            tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, C_i), qig=self.qig, constrain=self.constrain)
            total_query_time += query_time

            for t, cqids in tuples.items():
                if t in all_tuples:
                    all_tuples[t].update(cqids)
                else:
                    all_tuples[t] = cqids

            start = time.time()
            T = self.tuples_not_in_future_cliques(C_list, all_tuples, i)
            future_check_time += time.time() - start

            if T:
                objectives, calc_objective_time = self.calc_objectives(Q, T)
                objectives, min_objective_time = self.min_objective_tuples(Q, T, objectives, timed_out)

                t_hat, min_objective = objectives.items()[0]
                t_hat_cqids = all_tuples[t_hat]

                self.print_tuple(Q, t_hat, t_hat_cqids)

                result_meta = {
                    'objective': min_objective,
                    'total_cq': len(Q),
                    'query_time': total_query_time,
                    'comp_time': calc_objective_time + min_objective_time + future_check_time
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

        tuples = {}
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
                    t = random.choice(list(cq_tuples))
                    tuples[t] = set([cqid])
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
        if tuples:
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
