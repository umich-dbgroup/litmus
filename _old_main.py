from __future__ import division
from moz_sql_parser import parse
import json
import mysql.connector
import operator
import time

def dist_tuple(t1, t2):
    if q(t1) == q(t2):
        return False

    match = 0

    # exclude the query id column from the check
    exclude_id_len = len(t1) - 1
    for i in range(0, exclude_id_len):
        if t1[i] == t2[i]:
            match += 1
    return exclude_id_len - match == 1

def dist_set(T):
    dist = 0
    for i, t1 in enumerate(T):
        for j in range(i+1, len(T)):
            t2 = T[j]
            if dist_tuple(t1, t2):
                dist += 1

    dist = dist * 2 / (len(T) * len(T))
    return dist

def q(t):
    return t[len(t) - 1]

def retrieve_tuples(conn, Q, filter):
    U = []
    print("Running queries...")
    start = time.time()
    for query_id, query in enumerate(Q):
        parsed = parse(query)

        projs = []
        for sel in parsed['select']:
            if 'distinct' in sel['value']:
                projs.append(sel['value']['distinct'])
            else:
                projs.append(sel['value'])

        filter_q = query
        for i, filt_attr in enumerate(filter):
            if filt_attr:
                if isinstance(filt_attr, str):
                    filter_q += " AND {} = '{}'".format(projs[i], filt_attr)
                else:
                    filter_q += " AND {} = {}".format(projs[i], filt_attr)

        print("Running query {}...".format(filter_q))
        cursor = conn.cursor()
        cursor.execute(filter_q)

        for result in cursor:
            result = result + (query_id,)
            U.append(result)

        print("Loaded {} tuples so far.".format(len(U)))
        cursor.close()
    print("Done running queries. Elapsed time: {}s".format(time.time() - start))
    return U

def calc_pair_count(U):
    print("Calculating dist pairs for each tuple...")
    start = time.time()
    D = {}
    for i, t_i in enumerate(U):
        if t_i not in D:
            D[t_i] = 0
        for j in range(i+1, len(U)):
            t_j = U[j]
            if dist_tuple(t_i, t_j):
                D[t_i] += 1
                if t_j not in D:
                    D[t_j] = 0
                D[t_j] += 1
    print("Done calculating dist pairs for each tuple. Elapsed time: {}s".format(time.time() - start))
    return D

def sort_tuples_by_dist_pair_count(D):
    print("Sorting tuples by dist pairs...")
    start = time.time()
    D_sorted = sorted(D.items(), key=operator.itemgetter(1),reverse=True)
    print("Done sorting tuples by dist pairs. Elapsed time: {}s".format(time.time() - start))
    return D_sorted

def add_highest_tuple_per_CQ(T,D_sorted,Q,U):
    print("Getting tuple with highest dist pairs for each CQ...")
    start = time.time()
    Q_r = set(range(0,len(Q)))     # remaining CQs
    for (t,d) in D_sorted:
        if q(t) in Q_r:
            Q_r.discard(q(t))
            T.append(t)
            U.remove(t)
        if not Q_r:
            break
    print("Found tuple with highest dist pair for each CQ. Elapsed time: {}s".format(time.time() - start))

def add_highest_tuple_for_CQ(T,D_sorted,query_id,U):
    print("Getting tuple with highest dist pairs for CQ {}...".format(query_id))
    start = time.time()
    for (t,d) in D_sorted:
        if q(t) == query_id:
            T.append(t)
            U.remove(t)
            break
    print("Found tuple with highest dist pair for CQ {}. Elapsed time: {}s".format(query_id, time.time() - start))

def calc_pair_count_to_T(U,T):
    print("Calculating dist pairs for remaining tuples to T...")
    start = time.time()
    D = {}
    for t_i in U:
        pair_count = 0
        for t_j in T:
            if dist_tuple(t_i, t_j):
                pair_count += 1
        D[t_i] = pair_count
    print("Done calculating dist pairs for remaining tuples to T. Elapsed time: {}s".format(time.time() - start))
    return D

# filter is a tuple the same length as the projection
#   - any None values means that attribute is not filtered
def greedy_dist(conn, Q, k, filter):
    U = retrieve_tuples(conn, Q, filter)
    D = calc_pair_count(U)
    D_sorted = sort_tuples_by_dist_pair_count(D)

    T = []   # final set to return
    add_highest_tuple_per_CQ(T,D_sorted,Q,U)

    D_prime = calc_pair_count_to_T(U, T)
    D_prime_sorted = sort_tuples_by_dist_pair_count(D_prime)

    Y = []
    for i in range(0,k-len(T)):
        Y.append(D_prime_sorted[i][0])
        U.remove(D_prime_sorted[i][0])

    T = T + Y

    print("Best k tuples; dist: {}".format(dist_set(T)))
    for t in T:
        print(t)

    # with open('tuples.out', 'w') as f:
    #     for (e1, e2, e3, interp) in T:
    #         f.write("{}\t{}\t{}\t{}\n".format(e1.encode('utf-8'), e2.encode('utf-8'), e3, interp))

def round_robin_dist(conn, Q, k, filter):
    U = retrieve_tuples(conn, Q, filter)
    D = calc_pair_count(U)
    D_sorted = sort_tuples_by_dist_pair_count(D)

    T = []   # final set to return
    add_highest_tuple_per_CQ(T,D_sorted,Q,U)

    q = 0
    while len(T) < k:
        D_prime = calc_pair_count_to_T(U, T)
        D_prime_sorted = sort_tuples_by_dist_pair_count(D_prime)
        add_highest_tuple_for_CQ(T,D_prime_sorted,q,U)

        q += 1
        if q == len(Q):
            q = 0

    print("Best k tuples; dist: {}".format(dist_set(T)))
    for t in T:
        print(t)

def main():
    conn = mysql.connector.connect(user='root', password='', host='127.0.0.1', database='imdb')

    Q = []
    with open('queries.in', 'r') as f:
        for line in f:
            Q.append(line.strip())

    k = 10

    # filter = ('Morgan Freeman', None, None, None)
    # filter = (None, 'Steven Spielberg', None, None)
    filter = ('Tom Cruise', None, None, None)
    # greedy_dist(conn, Q, k, filter)
    round_robin_dist(conn, Q, k, filter)
    conn.close()


if __name__ == '__main__':
    main()
