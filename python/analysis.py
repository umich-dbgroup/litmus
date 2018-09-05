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
    accum_inv_iters = 0
    # accum_exec_cq = 0
    # accum_timeout_cq = 0
    # accum_norm_dist = 0
    # accum_parse_time = 0
    accum_query_time = 0
    accum_comp_time = 0
    for qid, r in results.items():
        if args.min and qid < args.min:
            continue
        if args.max and qid > args.max:
            continue
        if not args.include_non_spj and qid in excludes:
            continue

        result_count += 1
        accum_total_cq += r['total_cqs']
        if r['iters']:
            accum_inv_iters += 1 / r['iters']
        # accum_exec_cq += r['exec_cq']
        # accum_timeout_cq += r['timeout_cq']
        # accum_norm_dist += r['dist'] / r['total_cq']
        # accum_parse_time += r['parse_time']
        for meta in r['meta']:
            accum_query_time += meta['query_time']
            accum_comp_time += meta['comp_time']

    table = BeautifulTable()
    table.column_headers = ['METRIC', 'VALUE']
    table.column_alignments['METRIC'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Total Results', '{}'.format(len(results))])
    table.append_row(['Analyzed Results', '{}'.format(result_count)])
    table.append_row(['Avg. Total CQ #', '{:.3f}'.format(accum_total_cq / result_count)])
    table.append_row(['Avg. Inverse Iters', '{:.3f}'.format(accum_inv_iters / result_count)])
    # table.append_row(['Avg. Exec CQ #', '{:.3f}'.format(accum_exec_cq / result_count)])
    # table.append_row(['Avg. Timeout CQ #', '{:.3f}'.format(accum_timeout_cq / result_count)])
    # table.append_row(['Avg. Timeout CQ %', '{:.2f}%'.format(accum_timeout_cq / accum_exec_cq * 100)])
    # table.append_row(['Avg. Norm. Dist', '{:.3f}'.format(accum_norm_dist / result_count)])
    # table.append_row(['Avg. Parse Time', '{:.3f}s'.format(accum_parse_time / result_count)])
    table.append_row(['Avg. Query Time', '{:.3f}s'.format(accum_query_time / result_count)])
    table.append_row(['Avg. Computation Time', '{:.3f}s'.format(accum_comp_time / result_count)])
    print(table)

if __name__ == '__main__':
    main()
