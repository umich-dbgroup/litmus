from __future__ import division

import argparse
from collections import OrderedDict
import ConfigParser
import numpy as np
from operator import add
import os
import pickle

from beautifultable import BeautifulTable

def accumulate_results(results, min_qid, max_qid):
    analyzed_count = 0
    cq_counts = []
    each_iters = []
    query_times = []
    comp_times = []
    total_times = []
    times_per_iter = []

    for qid, r in results.items():
        if min_qid and qid < min_qid:
            continue
        if max_qid and qid > max_qid:
            continue

        if r['iters'] == 0:
            continue

        analyzed_count += 1
        cq_counts.append(r['total_cqs'])
        each_iters.append(r['iters'])

        query_time = 0
        comp_time = 0
        total_time = 0
        for meta in r['meta']:
            query_time += meta['query_time']
            comp_time += meta['comp_time']
            total_time += meta['query_time'] + meta['comp_time']

        query_times.append(query_time)
        comp_times.append(comp_time)
        total_times.append(total_time)
        times_per_iter.append(total_time / r['iters'])

    summary = {
        'total': len(results),
        'analyzed': analyzed_count,
        'cq_counts': cq_counts,
        'iters': each_iters,
        'query_times': query_times,
        'comp_times': comp_times,
        'total_times': total_times,
        'times_per_iter': times_per_iter
    }

    return summary

def avg_summaries(summaries):
    result = None
    for summary in summaries:
        if result is None:
            result = summary
        else:
            result['total'] += summary['total']
            result['analyzed'] += summary['analyzed']
            result['cq_counts'] = list(map(add,result['cq_counts'],summary['cq_counts']))
            result['iters'] = list(map(add,result['iters'],summary['iters']))
            result['query_times'] = list(map(add,result['query_times'],summary['query_times']))
            result['comp_times'] = list(map(add,result['comp_times'],summary['comp_times']))
            result['total_times'] = list(map(add,result['total_times'],summary['total_times']))
            result['times_per_iter'] = list(map(add,result['times_per_iter'],summary['times_per_iter']))

    result['total'] /= len(summaries)
    result['analyzed'] /= len(summaries)
    result['cq_counts'] = [i / len(summaries) for i in result['cq_counts']]
    result['iters'] = [i / len(summaries) for i in result['iters']]
    result['query_times'] = [i / len(summaries) for i in result['query_times']]
    result['comp_times'] = [i / len(summaries) for i in result['comp_times']]
    result['total_times'] = [i / len(summaries) for i in result['total_times']]
    result['times_per_iter'] = [i / len(summaries) for i in result['times_per_iter']]

    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    parser.add_argument('--min', type=int)
    parser.add_argument('--max', type=int)
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    dir = config.get('main', 'results_dir')
    summaries = []
    for filename in os.listdir(dir):
        if filename.startswith('{}_{}'.format(args.db, args.mode)) and filename.endswith('.pkl'):
            result_path = os.path.join(dir, filename)
            results = pickle.load(open(result_path, 'rb'))
            summaries.append(accumulate_results(results, args.min, args.max))
    stats = avg_summaries(summaries)

    iters_0 = sum(i == 0 for i in stats['iters'])
    iters_1 = sum(i <= 1 for i in stats['iters'])
    iters_2 = sum(i <= 2 for i in stats['iters'])
    iters_3 = sum(i <= 3 for i in stats['iters'])
    iters_4 = sum(i <= 4 for i in stats['iters'])
    iters_5 = sum(i <= 5 for i in stats['iters'])
    iters_else = sum(i is not None for i in stats['iters'])
    failed = sum(i is None for i in stats['iters'])

    table = BeautifulTable()
    table.column_headers = ['TASK INFO', 'VALUE']
    table.column_alignments['TASK INFO'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Total Results', '{}'.format(stats['total'])])
    table.append_row(['Analyzed Results', '{}'.format(stats['analyzed'])])
    table.append_row(['Min Total CQ #', '{:.3f}'.format(np.min(stats['cq_counts']))])
    table.append_row(['Mean Total CQ #', '{:.3f}'.format(np.mean(stats['cq_counts']))])
    table.append_row(['Max Total CQ #', '{:.3f}'.format(np.max(stats['cq_counts']))])
    print(table)

    table = BeautifulTable()
    table.column_headers = ['ITER INFO', 'VALUE']
    table.column_alignments['ITER INFO'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['# Tasks <= 0 Iter (%)', '{} ({:.2f}%)'.format(iters_0, iters_0 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks <= 1 Iter (%)', '{} ({:.2f}%)'.format(iters_1, iters_1 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks <= 2 Iter (%)', '{} ({:.2f}%)'.format(iters_2, iters_2 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks <= 3 Iter (%)', '{} ({:.2f}%)'.format(iters_3, iters_3 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks <= 4 Iter (%)', '{} ({:.2f}%)'.format(iters_4, iters_4 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks <= 5 Iter (%)', '{} ({:.2f}%)'.format(iters_5, iters_5 / stats['analyzed'] * 100)])
    table.append_row(['# Tasks >= 6 Iter (%)', '{} ({:.2f}%)'.format(iters_else, iters_else / stats['analyzed'] * 100)])
    table.append_row(['# Failed Tasks (%)', '{} ({:.2f}%)'.format(failed, failed / stats['analyzed'] * 100)])
    table.append_row(['Min # Iters', '{:.3f}'.format(np.min(stats['iters']))])
    table.append_row(['First Quartile # Iters', '{:.3f}'.format(np.percentile(stats['iters'], 25))])
    table.append_row(['Median # Iters', '{:.3f}'.format(np.percentile(stats['iters'], 50))])
    table.append_row(['Third Quartile # Iters', '{:.3f}'.format(np.percentile(stats['iters'], 75))])
    table.append_row(['Max # Iters', '{:.3f}'.format(np.max(stats['iters']))])
    table.append_row(['Std. Dev. # Iters', '{:.3f}'.format(np.std(stats['iters']))])
    table.append_row(['Mean # Iters', '{:.3f}'.format(np.mean(stats['iters']))])
    print(table)

    table = BeautifulTable()
    table.column_headers = ['TIME INFO', 'VALUE']
    table.column_alignments['TIME INFO'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Mean Query Time', '{:.3f}s'.format(np.mean(stats['query_times']))])
    table.append_row(['Mean Computation Time', '{:.3f}s'.format(np.mean(stats['comp_times']))])
    table.append_row(['Mean Total Time', '{:.3f}s'.format(np.mean(stats['total_times']))])
    table.append_row(['Max Total Time', '{:.3f}s'.format(np.max(stats['total_times']))])
    table.append_row(['Mean Total Time/Iter', '{:.3f}s'.format(np.mean(stats['times_per_iter']))])
    table.append_row(['Max Total Time/Iter', '{:.3f}s'.format(np.max(stats['times_per_iter']))])
    print(table)

if __name__ == '__main__':
    main()
