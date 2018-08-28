from __future__ import division

import argparse
from collections import OrderedDict
import ConfigParser
import os
import pickle

from beautifultable import BeautifulTable

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    parser.add_argument('--min', type=int)
    parser.add_argument('--max', type=int)
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    result_path = os.path.join(config.get('main', 'results_dir'), '{}_{}.pkl'.format(args.db, args.mode))
    results = pickle.load(open(result_path, 'rb'))

    accum_total_cq = 0
    accum_exec_cq = 0
    accum_timeout_cq = 0
    accum_norm_dist = 0
    accum_parse_time = 0
    accum_query_time = 0
    accum_comp_time = 0
    for qid, r in results.items():
        accum_total_cq += r['total_cq']
        accum_exec_cq += r['exec_cq']
        accum_timeout_cq += r['timeout_cq']
        accum_norm_dist += r['dist'] / r['total_cq']
        accum_parse_time += r['parse_time']
        accum_query_time += r['query_time']
        accum_comp_time += r['comp_time']

    table = BeautifulTable()
    table.column_headers = ['METRIC', 'VALUE']
    table.column_alignments['METRIC'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Avg. Total CQ #', '{:.3f}'.format(accum_total_cq / len(results))])
    table.append_row(['Avg. Exec CQ #', '{:.3f}'.format(accum_exec_cq / len(results))])
    table.append_row(['Avg. Timeout CQ #', '{:.3f}'.format(accum_timeout_cq / len(results))])
    table.append_row(['Avg. Timeout CQ %', '{:.2f}%'.format(accum_timeout_cq / accum_exec_cq * 100)])
    table.append_row(['Avg. Norm. Dist', '{:.3f}'.format(accum_norm_dist / len(results))])
    table.append_row(['Avg. Parse Time', '{:.3f}s'.format(accum_parse_time / len(results))])
    table.append_row(['Avg. Query Time', '{:.3f}s'.format(accum_query_time / len(results))])
    table.append_row(['Avg. Computation Time', '{:.3f}s'.format(accum_comp_time / len(results))])
    print(table)

if __name__ == '__main__':
    main()
