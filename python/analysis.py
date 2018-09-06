from __future__ import division

import argparse
from collections import OrderedDict
import ConfigParser
import os
import pickle

from beautifultable import BeautifulTable

from utils.find_spj import find_excludes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    parser.add_argument('--include_non_spj', action='store_true')
    parser.add_argument('--min', type=int)
    parser.add_argument('--max', type=int)
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    result_path = os.path.join(config.get('main', 'results_dir'), '{}_{}.pkl'.format(args.db, args.mode))
    results = pickle.load(open(result_path, 'rb'))

    excludes = []
    if not args.include_non_spj:
        excludes = find_excludes(args.db)

    result_count = 0
    accum_total_cq = 0
    accum_query_time = 0
    accum_comp_time = 0
    accum_total_time = 0

    iters_1 = 0
    iters_2 = 0
    iters_3 = 0
    iters_4 = 0
    iters_5 = 0
    iters_6_plus = 0
    failed = 0

    for qid, r in results.items():
        if args.min and qid < args.min:
            continue
        if args.max and qid > args.max:
            continue
        if not args.include_non_spj and qid in excludes:
            continue

        result_count += 1
        accum_total_cq += r['total_cqs']
        if r['iters'] is not None:
            if r['iters'] <= 1:
                iters_1 += 1
            if r['iters'] <= 2:
                iters_2 += 1
            if r['iters'] <= 3:
                iters_3 += 1
            if r['iters'] <= 4:
                iters_4 += 1
            if r['iters'] <= 5:
                iters_5 += 1
            iters_6_plus += 1
        else:
            failed += 1
        for meta in r['meta']:
            accum_query_time += meta['query_time']
            accum_comp_time += meta['comp_time']
            accum_total_time += meta['query_time'] + meta['comp_time']

    table = BeautifulTable()
    table.column_headers = ['METRIC', 'VALUE']
    table.column_alignments['METRIC'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Total Results', '{}'.format(len(results))])
    table.append_row(['Analyzed Results', '{}'.format(result_count)])
    table.append_row(['Avg. Total CQ #', '{:.3f}'.format(accum_total_cq / result_count)])
    table.append_row(['# Tasks <= 1 Iter (%)', '{} ({:.2f}%)'.format(iters_1, iters_1 / result_count * 100)])
    table.append_row(['# Tasks <= 2 Iter (%)', '{} ({:.2f}%)'.format(iters_2, iters_2 / result_count * 100)])
    table.append_row(['# Tasks <= 3 Iter (%)', '{} ({:.2f}%)'.format(iters_3, iters_3 / result_count * 100)])
    table.append_row(['# Tasks <= 4 Iter (%)', '{} ({:.2f}%)'.format(iters_4, iters_4 / result_count * 100)])
    table.append_row(['# Tasks <= 5 Iter (%)', '{} ({:.2f}%)'.format(iters_5, iters_5 / result_count * 100)])
    table.append_row(['# Tasks >= 6 Iter (%)', '{} ({:.2f}%)'.format(iters_6_plus, iters_6_plus / result_count * 100)])
    table.append_row(['# Failed Tasks (%)', '{} ({:.2f}%)'.format(failed, failed / result_count * 100)])
    table.append_row(['Avg. Query Time', '{:.3f}s'.format(accum_query_time / result_count)])
    table.append_row(['Avg. Computation Time', '{:.3f}s'.format(accum_comp_time / result_count)])
    table.append_row(['Avg. Total Time', '{:.3f}s'.format(accum_total_time / result_count)])
    print(table)

if __name__ == '__main__':
    main()
