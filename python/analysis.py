from __future__ import division

import argparse
from collections import OrderedDict
import ConfigParser
import numpy as np
from operator import add
import os
import pickle

from tqc import load_tqc_cache

from beautifultable import BeautifulTable

def accumulate_results(results, min_qid, max_qid, tqcs, tqc_min, tqc_max):
    analyzed_count = 0
    qids = []
    cq_counts = []
    each_iters = []
    query_times = []
    comp_times = []
    total_times = []
    times_per_iter = []
    max_iter_times = []

    for qid, r in results.items():
        if min_qid and qid < min_qid:
            continue
        if max_qid and qid > max_qid:
            continue
        if tqc_min and tqcs[qid] <= tqc_min:
            continue
        if tqc_max and tqcs[qid] > tqc_max:
            continue

        if r['iters'] is None:
            print('FAILED QUERY: {}'.format(qid))
            continue
        if r['iters'] == 0:
            continue

        analyzed_count += 1
        qids.append(qid)
        cq_counts.append(r['total_cqs'])
        each_iters.append(r['iters'])

        query_time = 0
        comp_time = 0
        total_time = 0
        max_iter_time = 0
        for meta in r['meta']:
            query_time += meta['query_time']
            comp_time += meta['comp_time']
            iter_time = meta['query_time'] + meta['comp_time']
            total_time += iter_time

            if iter_time > max_iter_time:
                max_iter_time = iter_time

        query_times.append(query_time)
        comp_times.append(comp_time)
        total_times.append(total_time)
        times_per_iter.append(total_time / r['iters'])
        max_iter_times.append(max_iter_time)

    summary = {
        'total': len(results),
        'analyzed': analyzed_count,
        'qids': qids,
        'cq_counts': cq_counts,
        'iters': each_iters,
        'query_times': query_times,
        'comp_times': comp_times,
        'total_times': total_times,
        'times_per_iter': times_per_iter,
        'max_iter_times': max_iter_times
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
            assert(result['qids'] == summary['qids'])
            result['cq_counts'] = list(map(add,result['cq_counts'],summary['cq_counts']))
            result['iters'] = list(map(add,result['iters'],summary['iters']))
            result['query_times'] = list(map(add,result['query_times'],summary['query_times']))
            result['comp_times'] = list(map(add,result['comp_times'],summary['comp_times']))
            result['total_times'] = list(map(add,result['total_times'],summary['total_times']))
            result['times_per_iter'] = list(map(add,result['times_per_iter'],summary['times_per_iter']))
            result['max_iter_times'] = list(map(add,result['max_iter_times'],summary['max_iter_times']))

    result['total'] /= len(summaries)
    result['analyzed'] /= len(summaries)
    result['cq_counts'] = [i / len(summaries) for i in result['cq_counts']]
    result['iters'] = [i / len(summaries) for i in result['iters']]
    result['query_times'] = [i / len(summaries) for i in result['query_times']]
    result['comp_times'] = [i / len(summaries) for i in result['comp_times']]
    result['total_times'] = [i / len(summaries) for i in result['total_times']]
    result['times_per_iter'] = [i / len(summaries) for i in result['times_per_iter']]
    result['max_iter_times'] = [i / len(summaries) for i in result['max_iter_times']]

    return result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    parser.add_argument('--min', type=int)
    parser.add_argument('--max', type=int)
    parser.add_argument('--tqc_min', type=float)
    parser.add_argument('--tqc_max', type=float)
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    if args.tqc_min or args.tqc_max:
        tqcs = load_tqc_cache(config, args.db)

    dir = config.get('main', 'results_dir')
    summaries = []
    for filename in os.listdir(dir):
        if filename.startswith('{}_{}'.format(args.db, args.mode)) and filename.endswith('.pkl'):
            result_path = os.path.join(dir, filename)
            results = pickle.load(open(result_path, 'rb'))
            summaries.append(accumulate_results(results, args.min, args.max, tqcs, args.tqc_min, args.tqc_max))
    stats = avg_summaries(summaries)

    iters_0 = sum(i == 0 for i in stats['iters'])
    iters_1 = sum(i <= 1 for i in stats['iters'])
    iters_2 = sum(i <= 2 for i in stats['iters'])
    iters_3 = sum(i <= 3 for i in stats['iters'])
    iters_4 = sum(i <= 4 for i in stats['iters'])
    iters_5 = sum(i <= 5 for i in stats['iters'])
    iters_else = sum(i is not None for i in stats['iters'])
    failed = sum(i is None for i in stats['iters'])

    # Iter boxplot data
    iter_q1 = np.percentile(stats['iters'], 25)
    iter_q2 = np.percentile(stats['iters'], 50)
    iter_q3 = np.percentile(stats['iters'], 75)
    iter_iqr = iter_q3 - iter_q1
    iter_outliers = []
    iter_normal = []
    for it_r in stats['iters']:
        if it_r >= iter_q1 - (1.5 * iter_iqr) and it_r <= iter_q3 + (1.5 * iter_iqr):
            iter_normal.append(it_r)
        else:
            iter_outliers.append(it_r)
    iter_max = max(iter_normal)
    iter_min = min(iter_normal)

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
    table.append_row(['Min # Iters', '{:.3f}'.format(iter_min)])
    table.append_row(['First Quartile # Iters', '{:.3f}'.format(iter_q1)])
    table.append_row(['Median # Iters', '{:.3f}'.format(iter_q2)])
    table.append_row(['Third Quartile # Iters', '{:.3f}'.format(iter_q3)])
    table.append_row(['Max # Iters', '{:.3f}'.format(iter_max)])
    table.append_row(['Outlier Iters', '{}'.format(iter_outliers)])
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
    table.append_row(['Avg. Max Single Iter', '{:.3f}s'.format(np.mean(stats['max_iter_times']))])
    print(table)

    table = BeautifulTable()
    table.column_headers = ['SLOWEST TASKS', 'TIME']
    table.column_alignments['SLOWEST TASKS'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['TIME'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    for idx in np.argsort(stats['total_times'])[::-1][:11]:
        table.append_row([stats['qids'][idx], stats['total_times'][idx]])
    print(table)

if __name__ == '__main__':
    main()
