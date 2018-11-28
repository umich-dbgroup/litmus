from __future__ import division, print_function

import random
import time
import traceback

from collections import OrderedDict
from itertools import combinations
from Queue import PriorityQueue

from query import Query
from qig import QIGByType, QIGByRange

TOP_TUPLES = 5
BOUND_LIMIT = 15

class Base(object):
    def __init__(self, db, aig=None, info=None):
        self.db = db
        self.aig = aig
        self.info = info

    def execute(self, cqs):
        result_meta = {
            'objective': 0,
            'total_cq': len(cqs),
            'exec_cq': 0,
            'query_time': 0,
            'comp_time': 0
        }
        return None, None, result_meta

    def sort_by_cost(self, cqs):
        # perform ROUGH cost estimation
        by_cost = []
        for cqid, cq in cqs.items():
            by_cost.append((cqid, cq.get_cost(self.db)))

        by_cost.sort(key=lambda x: x[1])
        return by_cost

    def run_cqs(self, cqs, msg_append='', qig=None, tuples=None):
        if tuples is None:
            tuples = {}
        valid_cqs = []
        timed_out = []
        sql_errors = []
        cached = []
        timeout_encountered = False

        start = time.time()
        for cqid, cost in self.sort_by_cost(cqs):
            cq = cqs[cqid]

            if timeout_encountered:
                # all CQs with higher cost are automatically assumed timed out
                cq.timed_out = True

            try:
                cq_tuples, was_cached = self.db.execute(cq)
                if was_cached:
                    cached.append(cqid)

                if cq.timed_out:
                    timeout_encountered = True
                    timed_out.append(cqid)
                elif cq.tuples:
                    valid_cqs.append(cqid)

                if cq.tuples:
                    for t in cq_tuples:
                        if t not in tuples:
                            tuples[t] = set()
                        tuples[t].add(cqid)
            except Exception:
                print(traceback.format_exc())
                sql_errors.append(cqid)

        query_time = time.time() - start
        print("Done executing CQs [{}s]".format(query_time))

        self.print_stats(cqs.keys(), timed_out, sql_errors, valid_cqs, cached)

        return tuples, valid_cqs, timed_out, sql_errors, query_time

    def incremental_exec(self, Q, tuples, timed_out):
        print('Running incremental execution for timed out queries...')
        start = time.time()

        # sort timed out cqs by descending weight
        by_weight = sorted(filter(lambda x: x[0] in timed_out, Q.items()), key=lambda x: -x[1].w)

        # incremental execution of each CQ
        while True:
            no_tuple_count = 0
            found = False
            for cqid, cq in by_weight:
                print('Running CQ {} incremental.'.format(cqid))
                t = self.db.execute_incremental(cq)

                if not t:
                    print('No tuple found.')
                    no_tuple_count += 1
                    continue

                if t not in tuples:
                    tuples[t] = set()
                tuples[t].add(cqid)

                for other_cqid in (set(timed_out) - set([cqid])):
                    print('Checking if it belongs to {}...'.format(other_cqid))
                    if Query.tuple_in_query(self.db, t, Q[other_cqid]):
                        tuples[t].add(other_cqid)

                if tuples[t] != set(Q.keys()):
                    print('Found incremental tuple.')
                    found = True
                    break
                else:
                    print('Belongs to all CQs, skip.')
                    del tuples[t]
                    cq.empty_cache()

            if no_tuple_count == len(by_weight):
                break

            if found:
                break

        incr_time = time.time() - start
        print('Done running incremental execution [{}s]'.format(incr_time))

        return tuples, incr_time

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

        objective_time = time.time() - start
        print("Done calculating objectives [{}s]".format(objective_time))
        return objectives, objective_time

    def min_objective_tuples(self, Q, T, objectives, check_Q):
        # operates on "minimal intervention policy"
        print('Finding min objective tuples, including timed out queries...')
        start = time.time()

        objectives = OrderedDict(sorted(objectives.items(), key=lambda t: t[1]))
        to_delete = []
        for t, objective in objectives.items():
            S = T[t]

            # check if t exists in any queries not yet executed/timed out
            for cqid in check_Q:
                if Query.tuple_in_query(self.db, t, Q[cqid]):
                    S.add(cqid)

            # recalculate objective for this
            objectives[t] = self.objective(Q, S)

            if S == set(Q.keys()):
                to_delete.append(t)
            else:
                # only execute up to the first
                break

        for t in to_delete:
            del objectives[t]
            del T[t]

        min_obj_time = time.time() - start
        print('Done finding min objective tuples. [{}s]'.format(min_obj_time))
        return objectives, min_obj_time

    def return_tuple(self, Q, t, cqids, result_meta):
        # remove t from all CQ caches in Q before returning
        for cqid, cq in Q.items():
            if cq.tuples:
                cq.tuples.discard(t)

                if not cq.tuples:
                    # if it's the only tuple, then empty cache so it runs again next time for incremental exec
                    cq.empty_cache()

        return t, cqids, result_meta

    def print_tuple(self, t, objective, S):
        print("{}, Objective: {}, # CQs: {}, CQs: {}".format(t, objective, len(S), S))

    def print_best_tuples(self, Q, objectives, T, k):
        print("Top {} tuples:".format(k))
        for t, objective in objectives.items()[0:k]:
            self.print_tuple(t, objective, T[t])

    def print_stats(self, exec_cqs, timed_out, sql_errors, valid_cqs, cached):
        print('Executed CQs ({}): {}'.format(len(exec_cqs), exec_cqs))
        print('From Cache ({}): {}'.format(len(cached), cached))
        print('Timed Out CQs ({}): {}'.format(len(timed_out), timed_out))
        print('SQL Error CQs ({}): {}'.format(len(sql_errors), sql_errors))
        print('Valid CQs ({}): {}'.format(len(valid_cqs), valid_cqs))

class L1S(Base):
    def informative_tuples(self, T):
        start = time.time()
        print('Finding informative tuples...')
        result = {}
        observed = set()
        for t, S in T.items():
            if frozenset(S) not in observed:
                result[t] = S
                observed.add(frozenset(S))
        inf_time = time.time() - start
        print('Done finding {} informative tuples [{}s].'.format(len(result), inf_time))
        return result, inf_time

    def find_best_entropy_tuple(self, Q, T, timed_out):
        print('Finding best entropy tuple...')
        start = time.time()

        # calculate entropies for each tuple
        u_plus_checks = {}
        u_minuses = {}
        entropies = {}
        entropy_set = set()
        for i, item in enumerate(T.items()):
            t, S = item
            if t not in u_plus_checks:
                u_plus_checks[t] = [(t, S)]
            if t not in u_minuses:
                u_minuses[t] = 1
            for j in range(i+1, len(T)):
                t2, S2 = T.items()[j]
                if S == S2:
                    if t2 not in u_plus_checks:
                        u_plus_checks[t2] = [(t2, S2)]

                    u_plus_checks[t].append((t2, S2))
                    u_plus_checks[t2].append((t,S))

                    if t2 not in u_minuses:
                        u_minuses[t2] = 0
                    u_minuses[t] += 1
                    u_minuses[t2] += 1
                elif S < S2:
                    u_plus_checks[t].append((t2, S2))
                    if t2 not in u_minuses:
                        u_minuses[t2] = 0
                    u_minuses[t2] += 1
                else:
                    if t2 not in u_plus_checks:
                        u_plus_checks[t2] = [(t2, S2)]
                    u_plus_checks[t2].append((t, S))
                    u_minuses[t] += 1

            u_plus = 0
            for t2, S2 in u_plus_checks[t]:
                for t3, S3 in u_plus_checks[t]:
                    if S2 < S3:
                        u_plus += 1
                        break

            entropy = (min(u_plus, u_minuses[t]), max(u_plus, u_minuses[t]))
            entropies[t] = entropy
            entropy_set.add(entropy)

        m = 0
        skyline = set()
        for e in entropy_set:
            is_skyline = True
            if min(e) > m:
                m = min(e)
            for e2 in entropy_set:
                if e != e2 and e[0] <= e2[0] and e[1] <= e2[1]:
                    is_skyline = False
                    break
            if is_skyline:
                skyline.add(e)

        t_hat = None
        e_hat = None
        for t, e in entropies.items():
            if e not in skyline:
                continue
            if t_hat is None:
                # have a default response in case timeout messes with things
                t_hat = t
                e_hat = e
            if min(e) == m:
                # check if t exists in any queries timed out
                for cqid in timed_out:
                    if Query.tuple_in_query(self.db, t, Q[cqid]):
                        T[t].add(cqid)

                # if it belongs to all, continue
                if T[t] != set(Q.keys()):
                    t_hat = t
                    e_hat = e
                    break

        e_time = time.time() - start
        print('Done finding best entropy tuple [{}s].'.format(e_time))
        return t_hat, e_hat, e_time

    def execute(self, Q):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(Q)

        total_incr_time = 0
        comp_time = 0
        t_hat = None
        t_hat_cqids = None
        min_objective = 0
        while not t_hat:
            if not tuples and timed_out:
                tuples, incr_time = self.incremental_exec(Q, tuples, timed_out)
                total_incr_time += incr_time
            inf_T, inf_time = self.informative_tuples(tuples)
            comp_time += inf_time
            t_hat, e_hat, e_time = self.find_best_entropy_tuple(Q, inf_T, timed_out)
            comp_time += e_time

            if t_hat:
                t_hat_cqids = tuples[t_hat]

        self.print_tuple(t_hat, e_hat, t_hat_cqids)

        result_meta = {
            'objective': e_hat,
            'total_cq': len(Q),
            'exec_cq': len(Q),
            'query_time': query_time + total_incr_time,
            'comp_time': comp_time
        }
        return self.return_tuple(Q, t_hat, t_hat_cqids, result_meta)

class GreedyAll(Base):
    def execute(self, Q):
        tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(Q)

        total_incr_time = 0
        comp_time = 0
        t_hat = None
        t_hat_cqids = None
        min_objective = 0
        while not t_hat:
            if not tuples and timed_out:
                tuples, incr_time = self.incremental_exec(Q, tuples, timed_out)
                total_incr_time += incr_time
            objectives, objective_time = self.calc_objectives(Q, tuples)
            comp_time += objective_time
            objectives, min_objective_time = self.min_objective_tuples(Q, tuples, objectives, timed_out)
            comp_time += min_objective_time

            if objectives:
                t_hat, min_objective = objectives.items()[0]
                t_hat_cqids = tuples[t_hat]

        self.print_best_tuples(Q, objectives, tuples, TOP_TUPLES)

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'exec_cq': len(Q),
            'query_time': query_time + total_incr_time,
            'comp_time': comp_time
        }
        return self.return_tuple(Q, t_hat, t_hat_cqids, result_meta)

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
            vals.append(S_w - diff_w)   # base case
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

    def find_maximal_cliques(self, Q):
        print('Finding maximal cliques...')
        start = time.time()

        if hasattr(self, 'cliques'):
            new_cliques = []
            for C_i in self.cliques:
                new_C_i = C_i & set(Q.keys())
                if new_C_i:
                    new_cliques.append(new_C_i)
            self.cliques = new_cliques
        else:
            self.cliques = self.qig.find_maximal_cliques()
        clique_time = time.time() - start
        print('Done finding maximal cliques [{}s]'.format(clique_time))
        return self.cliques, clique_time

    def set_to_dict(self, Q, S):
        S_dict = {}
        for s in S:
            S_dict[s] = Q[s]
        return S_dict

    def execute(self, Q):
        qig_time = self.construct_qig(Q)
        P = PriorityQueue()
        P_dups = set()
        C, clique_time = self.find_maximal_cliques(Q)

        for i, c in enumerate(C):
            print('Clique {}: {}'.format(i,c))
            P.put((self.bound(Q, c, {}), c, c))
            P_dups.add(frozenset(c))

        T_hat = {}
        v_hat = 99999999999

        total_query_time = 0
        total_objective_time = 0
        total_branch_time = 0

        tuples = {}
        executed = set()

        while not P.empty():
            (B, S, X) = P.get()

            if B >= v_hat:
                continue

            if not X <= executed:
                tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, X), qig=self.qig, tuples=tuples)
                executed |= X
                total_query_time += query_time

                if not tuples and timed_out:
                    tuples, incr_time = self.incremental_exec(Q, tuples, timed_out)
                    total_query_time += incr_time

            start = time.time()
            T, U = self.tuples_in_all_and_less_cqs(tuples, S)
            if T:
                T_hat = T
                v_hat = self.objective(Q, S)
            total_objective_time += time.time() - start

            if U:
                start = time.time()
                for item in self.branch(Q, C, U):
                    print('BRANCHING')
                    if frozenset(item[1]) not in P_dups:
                        P.put(item)
                        P_dups.add(frozenset(item[1]))
                total_branch_time += time.time() - start

        min_objective = 0
        t_hat = None
        t_hat_cqids = None
        if T_hat:
            for t, S in T_hat.items()[0:TOP_TUPLES]:
                self.print_tuple(t, self.objective(Q,S), S)

            t_hat, t_hat_cqids = T_hat.items()[0]
            min_objective = self.objective(Q, t_hat_cqids)

        comp_time = qig_time + clique_time + total_objective_time + total_branch_time

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'exec_cq': len(executed),
            'query_time': total_query_time,
            'comp_time': comp_time
        }
        return self.return_tuple(Q, t_hat, t_hat_cqids, result_meta)

class GreedyFirst(GreedyBB):
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
        C, clique_time = self.find_maximal_cliques(Q)

        C_list = []

        for i, c in enumerate(C):
            print('Clique {}: {}'.format(i, c))
            C_list.append((self.bound(Q, c, {}), c))
        C_list.sort()

        total_query_time = 0
        future_check_time = 0

        tuples = {}
        executed = set()

        for i, C_info in enumerate(C_list):
            B, C_i = C_info

            if not C_i <= executed:
                tuples, valid_cqs, timed_out, sql_errors, query_time = self.run_cqs(self.set_to_dict(Q, C_i), qig=self.qig, tuples=tuples)
                executed |= C_i
                total_query_time += query_time

                if not tuples and timed_out:
                    tuples, incr_time = self.incremental_exec(Q, tuples, timed_out)
                    total_query_time += incr_time

            start = time.time()
            T = self.tuples_not_in_future_cliques(C_list, tuples, i)
            future_check_time += time.time() - start

            if T:
                objectives, calc_objective_time = self.calc_objectives(Q, T)
                objectives, min_objective_time = self.min_objective_tuples(Q, T, objectives, timed_out)

                t_hat, min_objective = objectives.items()[0]
                t_hat_cqids = tuples[t_hat]

                self.print_tuple(t_hat, min_objective, t_hat_cqids)

                result_meta = {
                    'objective': min_objective,
                    'total_cq': len(Q),
                    'exec_cq': len(executed),
                    'query_time': total_query_time,
                    'comp_time': qig_time + clique_time + calc_objective_time + min_objective_time + future_check_time
                }
                return self.return_tuple(Q, t_hat, t_hat_cqids, result_meta)


class TopW(Base):
    # credit: https://gist.github.com/9thbit/1559670/4ee195bdbec43aff58a65b148b11c2ac7d246c11
    def cmp_w_randomize_ties(self, a, b):
        diff = cmp(a, b)
        return diff if diff else (random.randint(0, 1) * 2 - 1)

    def execute(self, Q):
        exec_cqs = []
        valid_cqs = []
        timed_out = []
        sql_errors = []
        cached = []

        print("Executing CQs in weighted order and select 1 random tuple...")
        sorted_cqs = OrderedDict(sorted(Q.items(), key=lambda x: -x[1].w, cmp=self.cmp_w_randomize_ties))

        timeout_cost = None
        tuples = {}

        total_query_time = 0
        start = time.time()
        for cqid, cq in sorted_cqs.items():
            try:
                if timeout_cost and cq.get_cost(self.db) >= timeout_cost:
                    cq.timed_out = True
                    timed_out.append(cqid)
                    continue

                exec_cqs.append(cqid)
                print(cq.query_str)
                cq_tuples, was_cached = self.db.execute(cq)

                if was_cached:
                    cached.append(cqid)

                if cq.timed_out:
                    timed_out.append(cqid)
                    timeout_cost = cq.get_cost(self.db)

                if cq.tuples:
                    valid_cqs.append(cqid)

                    tuple_list = list(cq_tuples)
                    random.shuffle(tuple_list)

                    found = False

                    for t in tuple_list:
                        tuples = { t: set([ int(cqid) ]) }

                        # need to check not-yet-executed queries and any timed-out queries
                        check_queries = list(set(Q.keys()) - set(exec_cqs))
                        check_queries.extend(timed_out)

                        for other_cqid in check_queries:
                            if Query.tuple_in_query(self.db, t, Q[other_cqid]):
                                tuples[t].add(other_cqid)

                        if tuples[t] != set(Q.keys()):
                            found = True
                            break
                        else:
                            cq.tuples = None
                            tuples = {}
                    if found:
                        break
            except Exception:
                print(traceback.format_exc())
                sql_errors.append(cqid)

        self.print_stats(exec_cqs, timed_out, sql_errors, valid_cqs, cached)

        if not tuples and timed_out:
            tuples, incr_time = self.incremental_exec(Q, tuples, timed_out)

        total_query_time += time.time() - start

        t_hat = None
        t_hat_cqids = None
        min_objective = 0
        if tuples:
            objectives, calc_objective_time = self.calc_objectives(Q, tuples)
            objectives, min_objective_time = self.min_objective_tuples(Q, tuples, objectives, [])
            self.print_best_tuples(Q, objectives, tuples, TOP_TUPLES)
            t_hat, min_objective = objectives.items()[0]
            t_hat_cqids = tuples[t_hat]

        result_meta = {
            'objective': min_objective,
            'total_cq': len(Q),
            'exec_cq': len(exec_cqs),
            'query_time': total_query_time,
            'comp_time': calc_objective_time + min_objective_time
        }
        return self.return_tuple(Q, t_hat, t_hat_cqids, result_meta)
