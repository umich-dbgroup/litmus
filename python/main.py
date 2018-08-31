from __future__ import print_function

import argparse
import ConfigParser
from collections import OrderedDict
import json
import os
import pickle
import traceback

from beautifultable import BeautifulTable

from algorithm.algorithms import Base, Partition, Overlap, Exhaustive, Random
from utils.database import Database
from utils.mailer import Mailer
from utils.parser import SQLParser
from utils.text_intersect import TextIntersectDatabase

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

def execute_mode(mode, db, tidb, parser, qid, cqs):
    print("QUERY {}: {}".format(qid, mode))

    algorithm = None

    if mode == 'random':
        algorithm = Random(db, parser)
    elif mode == 'exhaustive':
        algorithm = Exhaustive(db, parser)
    elif mode == 'partition':
        algorithm = Partition(db, parser)
    elif mode == 'overlap':
        algorithm = Overlap(db, parser, tidb)

    return algorithm.execute(cqs)

def load_tasks(data_dir, db_name):
    with open(os.path.join(data_dir, db_name + '.json')) as f:
        data = json.load(f)

    tasks = {}
    for qid, cqs in data.items():
        cq_dict = {}
        for cqid, cq in enumerate(cqs):
            cq_dict[cqid] = cq
        tasks[int(qid)] = cq_dict

    return tasks

def load_cache(path):
    if os.path.exists(path):
        return pickle.load(open(path, 'rb'))
    else:
        return {}

def save_results(results, path):
    pickle.dump(results, open(path, 'wb'))

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('db')
    argparser.add_argument('mode', choices=['random', 'exhaustive', 'partition', 'overlap'])
    argparser.add_argument('--qid', type=int)
    argparser.add_argument('--data_dir', default='../data')
    argparser.add_argument('--email')
    args = argparser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_path'), timeout=config.get('database', 'timeout'))
    parser = SQLParser(config.get('parser', 'cache_path'))

    # only load tidb if mode is overlap
    if args.mode == 'overlap':
        tidb = TextIntersectDatabase.from_file(db, os.path.join(config.get('tidb', 'dir'), args.db + '.tidb'))
    else:
        tidb = None

    tasks = load_tasks(args.data_dir, args.db)

    cache_path = os.path.join(config.get('main', 'cache_dir'), args.db + '_' + args.mode + '.pkl')
    out_path = os.path.join(config.get('main', 'results_dir'), args.db + '_' + args.mode + '.pkl')
    results = load_cache(cache_path)

    try:
        if args.qid is not None:
            # if executing single query
            if args.qid in results:
                print('QUERY {}: Skipping, already in cache.'.format(args.qid))
            else:
                results[args.qid] = execute_mode(args.mode, db, tidb, parser, args.qid, tasks[args.qid])
                save_results(results, cache_path)
            print_result(args.qid, results[args.qid])
        else:
            # if executing all queries
            sorted_tasks = OrderedDict(sorted(tasks.items(), key=lambda t: t[0]))
            for qid, cqs in sorted_tasks.items():
                if qid in results:
                    print('QUERY {}: Skipping, already in cache.'.format(qid))
                else:
                    results[qid] = execute_mode(args.mode, db, tidb, parser, qid, cqs)
                    save_results(results, cache_path)
                print_result(qid, results[qid])

            # save all results when finished
            save_results(results, out_path)
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
