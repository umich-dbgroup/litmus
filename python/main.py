from __future__ import print_function

import argparse
import ConfigParser
from collections import OrderedDict
import json
import os

from algorithm.algorithms import Base, Partition, Overlap, Exhaustive
from utils.database import Database
from utils.mailer import Mailer
from utils.parser import SQLParser

def execute_mode(mode, db, parser, qid, cqs):
    print("QUERY {}: {}".format(qid, mode))

    algorithm = None

    if mode == 'stats':
        algorithm = Base(db)
    elif mode == 'exhaustive':
        algorithm = Exhaustive(db)
    elif mode == 'partition':
        algorithm = Partition(db, parser)
    elif mode == 'overlap':
        algorithm = Overlap(db, parser)

    pt, qt, ct = algorithm.execute(cqs)

    print("DONE [Parse Time: {}s, Query Time: {}s, Computation Time: {}s]".format(pt, qt, ct))
    print()

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

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('db')
    argparser.add_argument('mode', choices=['stats', 'exhaustive', 'partition', 'overlap'])
    argparser.add_argument('--qid', type=int)
    argparser.add_argument('--data_dir', default='../data')
    argparser.add_argument('--timeout', type=int, default=15000)
    argparser.add_argument('--email')
    args = argparser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, timeout=args.timeout)
    parser = SQLParser()
    tasks = load_tasks(args.data_dir, args.db)

    if args.qid is not None:
        # if executing single query
        execute_mode(args.mode, db, parser, args.qid, tasks[args.qid])
    else:
        # if executing all queries
        sorted_tasks = OrderedDict(sorted(tasks.items(), key=lambda t: t[0]))
        for qid, cqs in sorted_tasks.items():
            execute_mode(args.mode, db, parser, qid, cqs)

    if args.email is not None:
        mailer = Mailer()
        mailer.send(args.email, 'Done {}'.format(args.db), 'Done')

if __name__ == '__main__':
    main()
