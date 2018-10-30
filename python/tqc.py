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

def create_temp_table_for_tq(db, tq):
    print('Creating temporary table...')
    tq_types = ()
    tmp = 'CREATE TEMPORARY TABLE tq {}'.format(tq.query_str)
    for i, proj in enumerate(tq.projs):
        tmp = re.sub('(SELECT.*)({})(?! AS)(.*FROM)'.format(proj), '\g<1>\g<2> AS p{}\g<3>'.format(i), tmp, count=1)
        attr = db.get_attr(proj)
        tq_types = tq_types + (attr.type,)
    cursor = db.cursor()
    tmp = re.sub('SELECT (.*)', 'SELECT DISTINCT \g<1>', tmp)
    cursor.execute(tmp)
    cursor.close()
    print('Done creating temporary table.')
    return tq_types

def get_tq_count(db):
    cursor = db.cursor()
    cursor.execute('SELECT COUNT(*) FROM tq')
    row = cursor.fetchone()
    cursor.close()
    return row[0]

def cq_tq_intersects(db, cqid, tqid, cq, tq, tq_count, tq_types):
    if cqid == tqid:
        return tq_count

    check = re.sub('(SELECT) (DISTINCT )?(.*) (FROM)', '\g<1> /*+ MAX_EXECUTION_TIME(100000) */ COUNT(DISTINCT \g<3>) \g<4> tq,', cq.query_str, count=1)
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
        return 0

    cursor = db.cursor()
    try:
        cursor.execute(check)
        row = cursor.fetchone()
        cursor.close()
        return row[0]
    except Exception as e:
        if str(e).startswith('3024'):
            cursor.close()
            return 0
        else:
            raise e

def tqc_for_task(db, parser, qid, task):
    assert(len(task['ans']) == 1)

    if len(task['cqs']) == 1:
        print('Only one CQ.')
        return 0

    ans_id = task['ans'][0]
    tq_str = task['cqs'][ans_id]
    tq, cached = parser.parse_one(qid, ans_id, tq_str)

    tq_types = create_temp_table_for_tq(db, tq)
    ans_count = get_tq_count(db)

    intersect_counts = {}

    bar = tqdm(total=len(task['cqs']), desc='Calc intersects')

    for cqid, cq_str in task['cqs'].items():
        cq, cached = parser.parse_one(qid, cqid, cq_str)

        intersect_counts[cqid] = cq_tq_intersects(db, cqid, ans_id, cq, tq, ans_count, tq_types)
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

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=config.get('database', 'timeout'), buffer_pool_size=config.get('database', 'buffer_pool_size'))
    parser = SQLParser(args.db, config.get('parser', 'cache_dir'))

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
        easy = 0
        hard = 0
        for qid, tqc in tqcs.items():
            if tqc <= 0.5:
                easy += 1
            else:
                hard += 1
        print('TQC <= 0.5: {}'.format(easy))
        print('TQC > 0.5: {}'.format(hard))

if __name__ == '__main__':
    main()
