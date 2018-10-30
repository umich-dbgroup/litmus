import argparse
import json
import operator
import time

from collections import OrderedDict
import ConfigParser
from tqdm import tqdm

from modules.database import Database
from modules.excludes import find_excludes
from modules.mailer import Mailer
from modules.parser import SQLParser
from tqc import create_temp_table_for_tq, get_tq_count, cq_tq_intersects

MAX_Q_TIME = 900    # 900s = 15 min

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('db')
    argparser.add_argument('--email')
    args = argparser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=config.get('database', 'timeout'), buffer_pool_size=config.get('database', 'buffer_pool_size'))

    parser = SQLParser(args.db, config.get('parser', 'cache_dir'))

    data = json.load(open('../data/{}.json'.format(args.db), 'r'))
    data = OrderedDict(sorted(data.items(), key=lambda x: x[0]))

    excludes = find_excludes(args.db)

    for qid, task in data.items():
        print('\nExamining Task {}...'.format(qid))
        if int(qid) in excludes:
            print('Skipping excluded query.')
            continue

        assert(len(task['ans']) == 1)

        tqid = task['ans'][0]

        tq, cached = parser.parse_one(qid, tqid, task['cqs'][tqid])
        tq_types = create_temp_table_for_tq(db, tq)
        tq_count = get_tq_count(db)

        cq_infos = []  # list of (cqid, negative of query time, intersects w/tq)
        bar = tqdm(total=len(task['cqs']), desc='Checking CQs')
        for cqid, cq_str in task['cqs'].items():
            cq, cached = parser.parse_one(qid, cqid, cq_str)

            start = time.time()
            try:
                tups = db.execute_sql(cq.query_str)
            except Exception as e:
                if not str(e).startswith('Timeout'):
                    raise e
            q_time = time.time() - start

            x_count = cq_tq_intersects(db, cqid, tqid, cq, tq, tq_count, tq_types)

            cq_infos.append((cqid, -1 * q_time, x_count))
            bar.update(1)
        bar.close()

        cq_infos.sort(key=operator.itemgetter(2, 1))

        total_q_time = sum(-1 * i[1] for i in cq_infos)
        while total_q_time > MAX_Q_TIME:
            cur_cq_info = cq_infos.pop(0)
            print('Removing CQ {}, query_time: {}, intersects: {}'.format(cur_cq_info[0], -cur_cq_info[1], cur_cq_info[2]))
            total_q_time += cur_cq_info[1]

        cursor = db.cursor()
        cursor.execute('DROP TABLE tq')
        cursor.close()

        # remaining CQs should be saved back into data
        new_cqs = {}
        for i, info in enumerate(cq_infos):
            new_cqid = format(i, '03')
            new_cqs[new_cqid] = task['cqs'][info[0]]
            if info[0] == tqid:
                task['ans'] = [new_cqid]
        task['cqs'] = OrderedDict(sorted(new_cqs.items(), key=lambda x: x[0]))

    data = OrderedDict(sorted(data.items(), key=lambda x: x[0]))

    json.dump(open('../data/{}.cleaned.json'.format(args.db), 'w'))

    if args.email is not None:
        mailer = Mailer()
        mailer.send(args.email, 'Done cleaning CQs for {}'.format(args.db), 'Done')

if __name__ == '__main__':
    main()
