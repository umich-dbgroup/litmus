from __future__ import division, print_function

import argparse
import ConfigParser
import os
import pickle
import re

from tqdm import tqdm

from main import load_tasks

from modules.database import Database
from modules.excludes import find_excludes
from modules.parser import SQLParser

def tqc(ans_count, intersect_counts):
    denom = 0
    for cqid, count in intersect_counts.items():
        denom += count / ans_count

    return 1 - (1 / denom)

def tqc_for_task(db, parser, qid, task):
    assert(len(task['ans']) == 1)

    ans_id = task['ans'][0]
    tq_str = task['cqs'][ans_id]
    tq, cached = parser.parse_one(ans_id, tq_str)

    # save target query to temp table
    print('Creating temporary table...')
    tq_types = ()
    tmp = 'CREATE TEMPORARY TABLE tq {}'.format(tq.query_str)
    for i, proj in enumerate(tq.projs):
        tmp = re.sub('(SELECT.*)({})(?! AS)(.*FROM)'.format(proj), '\g<1>\g<2> AS p{}\g<3>'.format(i), tmp, count=1)
        attr = db.get_attr(proj)
        tq_types = tq_types + (attr.type,)
    cursor = db.cursor()
    cursor.execute(tmp)
    cursor.close()
    print('Done creating temporary table.')

    # count how many results in answer
    cursor = db.cursor()
    cursor.execute('SELECT COUNT(*) FROM tq')
    row = cursor.fetchone()
    cursor.close()
    ans_count = row[0]
    print('TQ Count: {}'.format(ans_count))

    intersect_counts = {}

    bar = tqdm(total=len(task['cqs']), desc='Calc intersects')

    for cqid, cq_str in task['cqs'].items():
        if cqid == ans_id:
            intersect_counts[cqid] = ans_count
            bar.update(1)
            continue

        cq, cached = parser.parse_one(cqid, cq_str)

        check = re.sub('(SELECT) (.*) (FROM)', '\g<1> COUNT(\g<2>) \g<3> tq,', cq_str, count=1)
        if 'where' not in check.lower():
            check += ' WHERE '
        else:
            check += ' AND '
        check_constraints = []
        cq_types = ()
        for i, proj in enumerate(cq.projs):
            check_constraints.append('tq.p{} = {}'.format(i, proj))
            attr = db.get_attr(proj)
            cq_types = cq_types + (attr.type,)
        check += ' AND '.join(check_constraints)

        if cq_types != tq_types:
            intersect_counts[cqid] = 0
            bar.update(1)
            continue

        cursor = db.cursor()
        try:
            cursor.execute(check)
            row = cursor.fetchone()
            cursor.close()
            intersect_counts[cqid] = row[0]
        except Exception as e:
            if str(e).startswith('3024'):
                cursor.close()
                intersect_counts[cqid] = 0
            else:
                raise e
        bar.update(1)
    bar.close()

    # calculate TQC
    result = tqc(ans_count, intersect_counts)
    print('Intersects: {}'.format(intersect_counts))

    cursor = db.cursor()
    cursor.execute('DROP TABLE tq')
    cursor.close()

    return result

def tqc_path(config, db):
    return os.path.join(config.get('tqc', 'dir'), db + '.tqc')

def load_tqc_cache(config, db):
    cache_path = tqc_path(config, db)
    if os.path.exists(cache_path):
        return pickle.load(open(cache_path, 'rb'))
    else:
        return {}

def save_tqc_cache(config, db, tqcs):
    cache_path = tqc_path(config, db)
    pickle.dump(tqcs, open(cache_path, 'wb'))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('--qid', type=int)
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    timeout = 100000
    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=timeout, buffer_pool_size=config.get('database', 'buffer_pool_size'))
    parser = SQLParser(config.get('parser', 'cache_path'))

    tasks = load_tasks(config.get('main', 'data_dir'), args.db)

    tqcs = load_tqc_cache(config, args.db)

    # load qids to exclude
    excludes = find_excludes(args.db)

    if args.qid:
        tqcs[args.qid] = tqc_for_task(db, parser, args.qid, tasks[args.qid])
    else:
        for qid, task in tasks.items():
            print('QUERY {}'.format(qid))
            if qid in excludes:
                print('Skipping non-SPJ query.')
                print()
                continue
            elif qid in tqcs:
                print('Loaded from cache.')
            else:
                tqcs[qid] = tqc_for_task(db, parser, qid, task)
                save_tqc_cache(config, args.db, tqcs)
            print('TQ Confusion: {}'.format(tqcs[qid]))
            print()

if __name__ == '__main__':
    main()
