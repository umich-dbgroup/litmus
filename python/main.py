from __future__ import print_function

import argparse
from collections import OrderedDict
import json
import os

from algorithm.algorithms import Base, ByType, ByTypeRange, Exhaustive
from utils.parser import SQLParser
from utils.database import Database

def execute_mode(mode, db, parser, qid, cqs):
    print("QUERY {}: {}".format(qid, mode))

    algorithm = None

    if mode == 'stats':
        algorithm = Base(db)
    elif mode == 'exhaustive':
        algorithm = Exhaustive(db)
    elif mode == 'by_type':
        algorithm = ByType(db, parser)
    elif mode == 'by_type_range':
        algorithm = ByTypeRange(db, parser)

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
    argparser.add_argument('mode', choices=['stats', 'exhaustive', 'by_type', 'by_type_range'])
    argparser.add_argument('--qid', type=int)
    argparser.add_argument('--data_dir', default='../data')
    argparser.add_argument('--timeout', type=int, default=15000)
    argparser.add_argument('--email')
    args = argparser.parse_args()

    db = Database('root', '', '127.0.0.1', 'imdb', timeout=args.timeout)
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
