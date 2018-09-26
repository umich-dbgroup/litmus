from __future__ import print_function

import argparse
import ConfigParser
from collections import OrderedDict
import json
import os
import pickle
import traceback

from beautifultable import BeautifulTable

from modules.aig import AIG
from modules.algorithms import Base, Partition, Exhaustive, Random
from modules.database import Database
from modules.find_spj import find_excludes
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

def execute_mode(mode, db, parser, qid, task, part_func, aig, greedy):
    print("QUERY {}: {}".format(qid, mode))

    algorithm = None

    if mode == 'random':
        algorithm = Random(db, parser)
    elif mode == 'exhaustive':
        algorithm = Exhaustive(db, parser)
    elif mode == 'partition':
        algorithm = Partition(db, parser, part_func=part_func, aig=aig, greedy=greedy)
    elif mode == 'constrain':
        algorithm = Partition(db, parser, part_func=part_func, aig=aig, constrain=True, greedy=greedy)

    cand_cqs = task['cqs'].copy()
    result_metas = []
    iters = 0
    while len(cand_cqs) > len(task['ans']):
        print('Running Iteration {}, CQ #: {}, Ans #: {}'.format(iters + 1, len(cand_cqs), len(task['ans'])))
        tuple, tuple_cqids, meta = algorithm.execute(cand_cqs)
        iters += 1

        result_metas.append(meta)

        if not tuple:
            iters = None
            break

        cand_cqs = user_feedback(cand_cqs, tuple_cqids, task['ans'])

    if iters is None or len(cand_cqs) == 0:
        # if couldn't find tuple or no cand cqs left
        iters = None
        print('FAILED to find intended query(s).')
    elif len(cand_cqs) == len(task['ans']) and all(k in task['ans'] for k in cand_cqs.keys()):
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

def load_cache(path):
    if os.path.exists(path):
        return pickle.load(open(path, 'rb'))
    else:
        return {}

def save_cache(results, path):
    pickle.dump(results, open(path, 'wb'))

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
    argparser.add_argument('mode', choices=['random', 'exhaustive', 'partition', 'constrain'])
    argparser.add_argument('--qid', type=int)
    argparser.add_argument('--data_dir', default='../data')
    argparser.add_argument('--part_func', choices=['type', 'range'], default='range')
    argparser.add_argument('--greedy', action='store_true')
    argparser.add_argument('--email')
    args = argparser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=config.get('database', 'timeout'), buffer_pool_size=config.get('database', 'buffer_pool_size'))
    parser = SQLParser(config.get('parser', 'cache_path'))

    # only load aig if mode is partition
    aig = None
    if args.mode == 'constrain' \
      or (args.mode == 'partition' and args.part_func == 'range'):
        aig = AIG(db, os.path.join(config.get('aig', 'dir'), args.db + '.aig'))

    tasks = load_tasks(args.data_dir, args.db)

    if args.mode == 'partition':
        file_prefix = '{}_{}_{}'.format(args.db, args.mode, args.part_func)
    else:
        file_prefix = '{}_{}'.format(args.db, args.mode)
    if args.greedy:
        file_prefix += '_greedy'

    cache_path = os.path.join(config.get('main', 'cache_dir'), file_prefix + '.pkl')
    results = load_cache(cache_path)

    # load qids to exclude
    excludes = find_excludes(args.db)

    try:
        if args.qid is not None:
            # if executing single query
            if args.qid in results:
                print('QUERY {}: Skipping, already in cache.'.format(args.qid))
            else:
                results[args.qid] = execute_mode(args.mode, db, parser, args.qid, tasks[args.qid], args.part_func, aig, args.greedy)
                save_cache(results, cache_path)
            # print_result(args.qid, results[args.qid])
        else:
            # if executing all queries
            sorted_tasks = OrderedDict(sorted(tasks.items(), key=lambda t: t[0]))
            for qid, task in sorted_tasks.items():
                if qid in excludes:
                    print('QUERY {}: Skipping non-SPJ query.'.format(qid))
                    continue
                if qid in results:
                    print('QUERY {}: Skipping, already in cache.'.format(qid))
                else:
                    results[qid] = execute_mode(args.mode, db, parser, qid, task, args.part_func, aig, args.greedy)
                    save_cache(results, cache_path)
                # print_result(qid, results[qid])

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
