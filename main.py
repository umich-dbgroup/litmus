from __future__ import division, print_function

import argparse
from collections import OrderedDict
from email.mime.text import MIMEText
import json
import smtplib
import time

from moz_sql_parser import parse
from progress.bar import ChargingBar

from database import Database

def send_email(to_email, subject, message):
    from_email = 'cannoliemailer@gmail.com'
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(from_email, 'cannoli123')
    s.sendmail(from_email, [to_email], msg.as_string())
    s.quit()

def execute_query(db, cq):
    cursor = db.cursor()

    try:
        cursor.execute(cq)

        cq_tuples = set()
        for result in cursor:
            # disallow nulls
            if None in result:
                continue

            cq_tuples.add(result)
        cursor.close()
        return cq_tuples
    except Exception, exc:
        cursor.close()
        if str(exc).startswith('3024'):
            raise Exception('Timeout: Query timed out.')

def print_stats(cq_count, timeout_count, valid_count):
    print('Total CQs: {}'.format(cq_count))
    print('Timed Out CQs: {}'.format(timeout_count))
    print('Valid CQs: {}'.format(valid_count))

def stats(db, cqs):
    tuples, valid_cqs, timed_out = run_cqs(db, cqs)

    sorted_tuples = OrderedDict(sorted(tuples.items(), key=lambda t: len(t[1]), reverse=True))

    print_stats(len(cqs), timed_out, len(valid_cqs))

    if valid_cqs > 0:
        print('Avg. tuples per CQ: {}'.format(len(tuples)/len(valid_cqs)))
        k = 5
        print('Tuples with most CQ overlap: ', end='')
        for tuple, count in sorted_tuples.items()[0:k]:
            print(count, end='; ')
        print()

def dist(cqs, t, cqids):
    Q_len = len(cqs)
    C_t = len(cqids)
    # TODO: when probabilities are unequal, p_t should be changed
    p_t = C_t / Q_len

    return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t));

def calc_dists(cqs, tuples):
    print("Calculating dist values...")
    start = time.time()
    tuple_dists = {}
    for t, cqids in tuples.items():
        tuple_dists[t] = dist(cqs, t, cqids)

    sorted_tuple_dists = OrderedDict(sorted(tuple_dists.items(), key=lambda t: t[1], reverse=True))
    print("Done calculating dists [{}s]".format(time.time() - start))
    return sorted_tuple_dists

def run_cqs(db, cqs, msg_append=''):
    valid_cqs = []
    timed_out = 0
    tuples = {}

    bar = ChargingBar('Running CQs{}'.format(msg_append), max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

    start = time.time()
    for cqid, cq in cqs.items():
        try:
            cq_tuples = execute_query(db, cq)

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
    print("Done executing CQs [{}s]".format(time.time() - start))

    return tuples, valid_cqs, timed_out

def print_top_dists(sorted_dists, tuples, k):
    print("Top {} tuples:".format(k))
    for t, dist_val in sorted_dists.items()[0:k]:
        print("{}, Dist: {}, # CQs: {}".format(t, dist_val, len(tuples[t])))

def exhaustive(db, cqs):
    tuples, valid_cqs, timed_out = run_cqs(db, cqs)

    print_stats(len(cqs), timed_out, len(valid_cqs))
    sorted_dists = calc_dists(cqs, tuples)
    print_top_dists(sorted_dists, tuples, 5)

def parse_projections(cq):
    parsed = parse(cq)

    projs = []
    for sel in parsed['select']:
        if 'distinct' in sel['value']:
            projs.append(sel['value']['distinct'])
        else:
            projs.append(sel['value'])

    return projs

def parse_projections_many(cqs):
    print("Parsing queries...")
    start = time.time()

    bar = ChargingBar('Parsing CQs', max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

    cq_projs = {}
    for cqid, cq in cqs.items():
        projs = parse_projections(cq)
        cq_projs[cqid] = (cq, projs)
        bar.next()
    bar.finish()
    print("Done parsing [{}s]".format(time.time() - start))
    return cq_projs

def by_type(db, cqs):
    cqs_projs = parse_projections_many(cqs)

    print("Partitioning CQs by type...")
    start = time.time()

    type_parts = {}
    for cqid, cq_proj in cqs_projs.items():
        cq, projs = cq_proj

        proj_types = ()
        for proj in projs:
            proj_types = proj_types + (db.get_attr(proj).type,)

        if proj_types not in type_parts:
            type_parts[proj_types] = {}
        type_parts[proj_types][cqid] = cq
    print("Done partitioning [{}s]".format(time.time() - start))

    # Assume the largest type is most likely to produce your result.
    sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
    type, type_cqs = sorted_type_parts.items()[0]

    tuples, valid_cqs, timed_out = run_cqs(db, type_cqs, msg_append=' ' + str(type))
    sorted_dists = calc_dists(cqs, tuples)
    print_top_dists(sorted_dists, tuples, 5)

def find_overlap_intervals(transitions):
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

def by_type_range(db, cqs):
    # assume all projections have the same length (for now)
    type_parts = {}

    attrs_added = set()

    # stores "transitions" to be able to find overlapping intervals later
    numeric_transitions = {}

    # for each colnum, stores attrs -> [(cqid, frag)]
    attrs_to_cqs = {}

    cqs_projs = parse_projections_many(cqs)

    print("Partitioning CQs by type and numeric range...")
    start = time.time()
    for cqid, cq_proj in cqs_projs.items():
        cq, projs = cq_proj

        proj_types = ()
        for colnum, proj in enumerate(projs):
            attr = db.get_attr(proj)
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
        numeric_intervals[colnum] = find_overlap_intervals(transitions)
        partition_counts.append(len(numeric_intervals[colnum]))

    print("Done partitioning [{}s]".format(time.time() - start))

    # Assume the largest type is most likely to produce your result.
    sorted_type_parts = OrderedDict(sorted(type_parts.items(), key=lambda t: len(t[1]), reverse=True))
    type, type_cqs = sorted_type_parts.items()[0]

    # Execute one case: best partition for each col. TODO: generalize?
    candidate_cqs = type_cqs
    after_partition_cqs = {}
    for colnum, coltype in enumerate(type):
        if coltype == 'num':
            # get best interval
            best_interval, best_attrs = numeric_intervals[colnum][0]

            # get cqs / transform
            for attr in best_attrs:
                cq_infos = attrs_to_cqs[colnum][attr]
                for cqid, proj in cq_infos:
                    if cqid in candidate_cqs:
                        cq = candidate_cqs[cqid]
                        # TODO: need to transform cq here into range query
                        cq += ' AND {} >= {} AND {} <= {}'.format(proj, best_interval[0], proj, best_interval[1])
                        print(cq)

                        after_partition_cqs[cqid] = cq

            candidate_cqs = after_partition_cqs

    tuples, valid_cqs, timed_out = run_cqs(db, candidate_cqs, msg_append=' ' + str(type))
    sorted_dists = calc_dists(cqs, tuples)
    print_top_dists(sorted_dists, tuples, 5)

def execute_mode(mode, db, qid, cqs):
    print("QUERY {}: {}".format(qid, mode))
    start = time.time()
    if mode == 'stats':
        stats(db, cqs)
    elif mode == 'exhaustive':
        exhaustive(db, cqs)
    elif mode == 'by_type':
        by_type(db, cqs)
    elif mode == 'by_type_range':
        by_type_range(db, cqs)
    print("DONE [{}s]".format(time.time() - start))
    print()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode', choices=['stats', 'exhaustive', 'by_type', 'by_type_range'])
    parser.add_argument('--qid', type=int)
    parser.add_argument('--timeout', type=int, default=15000)
    parser.add_argument('--email')
    args = parser.parse_args()

    print("Loading database...")
    start = time.time()
    db = Database('root', '', '127.0.0.1', 'imdb')
    db.set_timeout(args.timeout)
    print("Done loading database [{}s]".format(time.time()-start))

    # load dataset
    with open(args.db + '.json') as f:
        data = json.load(f)

    tasks = {}
    for qid, cqs in data.items():
        cq_dict = {}
        for cqid, cq in enumerate(cqs):
            cq_dict[cqid] = cq
        tasks[int(qid)] = cq_dict

    if args.qid is not None:
        # if executing single query
        execute_mode(args.mode, db, args.qid, tasks[args.qid])
    else:
        # if executing all queries
        sorted_tasks = OrderedDict(sorted(tasks.items(), key=lambda t: t[0]))
        for qid, cqs in sorted_tasks.items():
            execute_mode(args.mode, db, qid, cqs)

    if args.email is not None:
        send_email(args.email, 'Done {}'.format(args.db), 'Done')

if __name__ == '__main__':
    main()
