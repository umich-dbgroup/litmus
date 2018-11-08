from __future__ import print_function

import argparse
import ConfigParser
from collections import OrderedDict
import json
import os
import pickle
import random
import traceback

from beautifultable import BeautifulTable
from multiprocessing import Pool

from modules.aig import AIG
from modules.algorithms import Base, GreedyAll, GreedyBB, GreedyFirst, GuessAndVerify
from modules.database import Database
from modules.excludes import find_excludes
from modules.logger import Logger
from modules.mailer import Mailer
from modules.parser import SQLParser

def print_result(qid, result):
    table = BeautifulTable()
    table.column_headers = ['METRIC', 'VALUE']
    table.column_alignments['METRIC'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Dist', '{:.3f}'.format(result['dist'])])
    table.append_row(['Total CQs', '{}'.format(result['total_cq'])])
    table.append_row(['Executed CQs', '{}'.format(result['exec_cq'])])
    table.append_row(['Valid CQs', '{}'.format(result['valid_cq'])])
    table.append_row(['Timeout CQs', '{}'.format(result['timeout_cq'])])
    table.append_row(['Error CQs', '{}'.format(result['error_cq'])])
    table.append_row(['Parse Time', '{}'.format(result['parse_time'])])
    table.append_row(['Query Time', '{}'.format(result['query_time'])])
    table.append_row(['Computation Time', '{}'.format(result['comp_time'])])
    print(':: QUERY {} ::'.format(qid))
    print(table)

def user_feedback(cand_cqs, tuple_cqids, ans):
    new_cqs = {}

    if any(a in tuple_cqids for a in ans):
        # positive feedback on tuple
        for cqid, cq in cand_cqs.items():
            if cqid in tuple_cqids:
                new_cqs[cqid] = cq
    else:
        # negative feedback on tuple
        for cqid, cq in cand_cqs.items():
            if cqid not in tuple_cqids:
                new_cqs[cqid] = cq

    return new_cqs

def set_weights(Q, tqid, tq_rank):
    if tq_rank == 'random':
        return

    tq_idx = None
    if tq_rank == '1':
        tq_idx = 0
    elif tq_rank == 'q1':
        tq_idx = int(len(Q) / 4) - 1
    elif tq_rank == 'half':
        tq_idx = int(len(Q) / 2) - 1
    elif tq_rank == 'q3':
        tq_idx = int(3 * len(Q) / 4) - 1
    elif tq_rank == 'n':
        tq_idx = len(Q) - 1
    else:
        raise Exception('Unrecognized tq_rank: {}'.format(tq_rank))

    cqids = Q.keys()
    cqids.remove(tqid)

    # shuffle ranks of non-target query CQs
    random.shuffle(cqids)

    # insert the target query at the location
    cqids.insert(tq_idx, tqid)

    # assign weights for each CQ according to weighting scheme
    for i, cqid in enumerate(cqids):
        Q[cqid].set_w(len(Q) - i)

def run_task(mode, db, parser, qid, task, info, aig, tq_rank):
    print("QUERY {}: {}".format(qid, mode))

    algorithm = None

    if mode == 'gav':
        algorithm = GuessAndVerify(db)
    elif mode == 'greedyall':
        algorithm = GreedyAll(db)
    elif mode == 'greedybb':
        algorithm = GreedyBB(db, info=info, aig=aig)
    elif mode == 'greedyfirst':
        algorithm = GreedyFirst(db, info=info, aig=aig)

    Q = parser.parse_many(qid, task['cqs'].copy())

    set_weights(Q, task['ans'][0], tq_rank)

    result_metas = []
    iters = 0
    while len(Q) > len(task['ans']):
        print('Running Iteration {}, CQ #: {}, TQID: {}'.format(iters + 1, len(Q), task['ans'][0]))
        tuple, tuple_cqids, meta = algorithm.execute(Q)
        iters += 1

        print(meta)
        result_metas.append(meta)

        if not tuple:
            iters = None
            break

        Q = user_feedback(Q, tuple_cqids, task['ans'])

    if iters is None or len(Q) == 0:
        # if couldn't find tuple or no cand cqs left
        iters = None
        print('FAILED to find intended query(s).')
    elif len(Q) == len(task['ans']) and all(k in task['ans'] for k in Q.keys()):
        print('SUCCESS in finding intended query(s).')
        print('TOTAL ITERATIONS: {}'.format(iters))
    else:
        raise Exception('Failed in finding intended query(s).')

    print()

    result = {
        'total_cqs': len(task['cqs']),
        'iters': iters,
        'meta': result_metas
    }
    return result

def start_thread(mode, db_name, qid, task, info, tq_rank, log_dir=None):
    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), db_name, config.get('database', 'cache_dir'), timeout=config.get('database', 'timeout'), buffer_pool_size=config.get('database', 'buffer_pool_size'))
    parser = SQLParser(db_name, config.get('parser', 'cache_dir'))

    # only load aig if info includes range
    aig = None
    if (mode == 'greedybb' or mode == 'greedyfirst') and info == 'range':
        aig = AIG(db, os.path.join(config.get('aig', 'dir'), db_name + '.aig'))

    data = json.loads(task)
    task_cleaned = {
        'cqs': {},
        'ans': []
    }
    for cqid, cq in data['cqs'].items():
        task_cleaned['cqs'][int(cqid)] = cq
    for cqid in data['ans']:
        task_cleaned['ans'].append(int(cqid))
    task = task_cleaned

    if log_dir:
        log_path = os.path.join(log_dir, str(qid) + '.log')

        with Logger(log_path):
            return run_task(mode, db, parser, qid, task, info, aig, tq_rank)
    else:
        return run_task(mode, db, parser, qid, task, info, aig, tq_rank)

def load_tasks(data_dir, db_name):
    with open(os.path.join(data_dir, db_name + '.json')) as f:
        data = json.load(f)

    tasks = {}
    for qid, task in data.items():
        cq_dict = {}
        for cqid, cq in task['cqs'].items():
            cq_dict[int(cqid)] = cq

        tasks[int(qid)] = {
            'cqs': cq_dict,
            'ans': [int(a) for a in task['ans']]
        }

    return tasks

# def load_cache(path):
#     if os.path.exists(path):
#         return pickle.load(open(path, 'rb'))
#     else:
#         return {}

# def save_cache(results, path):
#     pickle.dump(results, open(path, 'wb'))

def save_results(results, out_dir, prefix):
    idx = 0
    out_path = os.path.join(out_dir, '{}_{}.pkl'.format(prefix, idx))
    while os.path.exists(out_path):
        idx += 1
        out_path = os.path.join(out_dir, '{}_{}.pkl'.format(prefix, idx))
    pickle.dump(results, open(out_path, 'wb'))

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('db')
    argparser.add_argument('mode', choices=['gav', 'greedyall', 'greedybb', 'greedyfirst'])
    # argparser.add_argument('--constrain', action='store_true')
    argparser.add_argument('--tq_rank', choices=['random', '1', 'q1', 'half', 'q3', 'n'], default='random')
    argparser.add_argument('--qid', type=int)
    argparser.add_argument('--info', choices=['type', 'range'], default='range')
    argparser.add_argument('--email')
    args = argparser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')
    tasks = load_tasks(config.get('main', 'data_dir'), args.db)

    if args.mode == 'greedybb' or args.mode == 'greedyfirst':
        file_prefix = '{}_{}_{}'.format(args.db, args.mode, args.info)
    else:
        file_prefix = '{}_{}'.format(args.db, args.mode)

    file_prefix += '_tq' + args.tq_rank

    cache_path = os.path.join(config.get('main', 'cache_dir'), file_prefix + '.pkl')
    log_dir = os.path.join(config.get('main', 'log_dir'), file_prefix)

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # results = load_cache(cache_path)
    results = {}

    # load qids to exclude
    excludes = find_excludes(args.db)

    try:
        if args.qid is not None:
            # if executing single query
            if args.qid in results:
                print('QUERY {}: Skipping, already in cache.'.format(args.qid))
            else:
                results[args.qid] = start_thread(args.mode, args.db, args.qid, json.dumps(tasks[args.qid]), args.info, args.tq_rank)
                # save_cache(results, cache_path)
            # print_result(args.qid, results[args.qid])
        else:
            # if executing all queries
            pool = Pool()

            sorted_tasks = OrderedDict(sorted(tasks.items(), key=lambda t: t[0]))

            responses = {}
            for qid, task in sorted_tasks.items():
                if qid in excludes:
                    print('QUERY {}: Skipping non-SPJ query.'.format(qid))
                    continue
                if qid in results:
                    print('QUERY {}: Skipping, already in cache.'.format(qid))
                else:
                    responses[qid] = pool.apply_async(start_thread, (args.mode, args.db, qid, json.dumps(task), args.info, args.tq_rank, log_dir))
                    # results[qid] = execute_mode(args.mode, db, parser, qid, task, args.info, aig, args.tq_rank)
                    # save_cache(results, cache_path)
                # print_result(qid, results[qid])

            responses = OrderedDict(sorted(responses.items(), key=lambda r: r[0]))
            for qid, res in responses.items():
                results[qid] = res.get()
                print('Finished query {}.'.format(qid))

            # save all results when finished
            save_results(results, config.get('main', 'results_dir'), file_prefix)
        if args.email is not None:
            mailer = Mailer()
            mailer.send(args.email, 'Done {}'.format(args.db), 'Done')
    except Exception as e:
        stacktrace = str(traceback.format_exc())
        if args.email is not None:
            mailer = Mailer()
            mailer.send(args.email, 'Error {}'.format(args.db), stacktrace)
        print(stacktrace)

if __name__ == '__main__':
    main()
