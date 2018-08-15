from __future__ import division, print_function

import argparse
from collections import OrderedDict
import json
import time

from moz_sql_parser import parse
import mysql.connector
from progress.bar import ChargingBar

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    args = parser.parse_args()

    conn = mysql.connector.connect(user='root', password='', host='127.0.0.1', database=args.db)

    # set query timeout
    timeout = 15000
    cursor = conn.cursor()
    cursor.execute('SET SESSION MAX_EXECUTION_TIME={}'.format(timeout))
    cursor.close()

    with open(args.db + '.json') as f:
        data = json.load(f)

    data = {int(k):v for k,v in data.items()}
    sorted_data = OrderedDict(sorted(data.items(), key=lambda t: t[0]))

    for qid, cqs in sorted_data.items():
        print("QUERY {}".format(qid))
        start = time.time()

        tuples = {}
        valid_cqs = 0
        timed_out = 0

        bar = ChargingBar('Running CQs', max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

        for cq in cqs:
            # print(cq)
            parsed = parse(cq)

            projs = []
            for sel in parsed['select']:
                if 'distinct' in sel['value']:
                    projs.append(sel['value']['distinct'])
                else:
                    projs.append(sel['value'])

            cursor = conn.cursor()

            try:
                cursor.execute(cq)

                cq_tuples = set()
                for result in cursor:
                    cq_tuples.add(result)
                cursor.close()

                if len(cq_tuples) > 0:
                    valid_cqs += 1

                for t in cq_tuples:
                    if t in tuples:
                        tuples[t] += 1
                    else:
                        tuples[t] = 1
            except Exception, exc:
                cursor.close()
                if str(exc).startswith('3024'):
                    timed_out += 1

            bar.next()

        bar.finish()

        print("Done executing CQs [{}s]".format(time.time() - start))

        sorted_tuples = OrderedDict(sorted(tuples.items(), key=lambda t: t[1], reverse=True))

        print('Total CQs: {}'.format(len(cqs)))
        print('Timed Out CQs: {}'.format(timed_out))
        print('Valid CQs: {}'.format(valid_cqs))
        if valid_cqs > 0:
            print('Avg. tuples per CQ: {}'.format(len(tuples)/valid_cqs))
            k = 5
            print('Tuples with most CQ overlap: ', end='')
            for tuple, count in sorted_tuples.items()[0:k]:
                print(count, end=', ')
            print('\n')

if __name__ == '__main__':
    main()
