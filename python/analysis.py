from __future__ import division

import argparse
import re

from beautifultable import BeautifulTable

qid_re = re.compile('QUERY ([0-9]+).*')
cq_count_re = re.compile('Total CQs: ([0-9]+)')
timeout_count_re = re.compile('Timed Out CQs: ([0-9]+)')
dist_re = re.compile('.*Dist: ([0-9\.e]+).*')
time_re = re.compile('DONE \[Parse Time: ([0-9\.e-]+)s, Query Time: ([0-9\.e-]+)s, Computation Time: ([0-9\.e-]+)s\]')

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    args = parser.parse_args()

    results = {}

    with open('../results/{}_{}.out'.format(args.db, args.mode)) as f:
        cur_query = None
        for line in f:
            line = line.strip()

            qid_m = qid_re.match(line)
            if qid_m:
                if cur_query is not None:
                    if 'dist' not in cur_query:
                        cur_query['dist'] = 0
                    results[cur_query['id']] = cur_query
                cur_query = {'id': int(qid_m.group(1))}
                continue

            cq_count_m = cq_count_re.match(line)
            if cq_count_m:
                cur_query['cq_count'] = int(cq_count_m.group(1))
                continue

            timeout_count_m = timeout_count_re.match(line)
            if timeout_count_m:
                cur_query['timeout_count'] = int(timeout_count_m.group(1))
                continue

            dist_m = dist_re.match(line)
            # only set for tuple with highest dist
            if dist_m and 'dist' not in cur_query:
                cur_query['dist'] = float(dist_m.group(1))
                continue

            time_m = time_re.match(line)
            if time_m:
                cur_query['parse_time'] = float(time_m.group(1))
                cur_query['query_time'] = float(time_m.group(2))
                cur_query['comp_time'] = float(time_m.group(3))

    accum_cq_count = 0
    accum_timeout_count = 0
    accum_norm_dist = 0
    accum_parse_time = 0
    accum_query_time = 0
    accum_comp_time = 0
    for qid, r in results.items():
        accum_cq_count += r['cq_count']
        accum_timeout_count += r['timeout_count']
        accum_norm_dist += r['dist'] / r['cq_count']
        accum_parse_time += r['parse_time']
        accum_query_time += r['query_time']
        accum_comp_time += r['comp_time']

    accum_total_time = accum_parse_time + accum_query_time + accum_comp_time

    table = BeautifulTable()
    table.column_headers = ['METRIC', 'VALUE']
    table.column_alignments['METRIC'] = BeautifulTable.ALIGN_LEFT
    table.column_alignments['VALUE'] = BeautifulTable.ALIGN_RIGHT
    table.row_separator_char = ''
    table.append_row(['Avg. CQ #', '{:.3f}'.format(accum_cq_count / len(results))])
    table.append_row(['Avg. Timeout CQ #', '{:.3f}'.format(accum_timeout_count / len(results))])
    table.append_row(['Timeout CQ %', '{:.2f}%'.format(accum_timeout_count / accum_cq_count * 100)])
    table.append_row(['Avg. Norm. Dist', '{:.3f}'.format(accum_norm_dist / len(results))])
    table.append_row(['Avg. Parse Time', '{:.3f}s'.format(accum_parse_time / len(results))])
    table.append_row(['Avg. Query Time', '{:.3f}s'.format(accum_query_time / len(results))])
    table.append_row(['Avg. Computation Time', '{:.3f}s'.format(accum_comp_time / len(results))])
    table.append_row(['Avg. Total Time', '{:.3f}s'.format(accum_total_time / len(results))])
    print(table)


if __name__ == '__main__':
    main()
