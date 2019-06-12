import argparse
import os
import numpy as np
import pickle

from analysis_single import get_stats

def get_latex_format(name, results):
    output_str = name + ':\n'
    for r in results:
        output_str += str(r) + ' '
    return output_str

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode')
    parser.add_argument('--qid_min', type=int)
    parser.add_argument('--qid_max', type=int)
    parser.add_argument('--tqc_min', type=float)
    parser.add_argument('--tqc_max', type=float)
    parser.add_argument('--user_time', type=float, default=1)
    args = parser.parse_args()

    TQ_RANKS = ['1', 'q1', 'half', 'q3', 'n']
    # TQ_RANKS = ['equal']

    iters = []
    total_times = []
    times_per_iter = []
    for i, tq_rank in enumerate(TQ_RANKS):
        stats = get_stats(args.db, args.mode, tq_rank, qid_min=args.qid_min, qid_max=args.qid_max, tqc_min=args.tqc_min, tqc_max=args.tqc_max, user_time=args.user_time)

        if stats:
            iters.append((i, np.mean(stats['iters'])))
            total_times.append((i, np.median(stats['total_times'])))
            times_per_iter.append((i, np.mean(stats['times_per_iter'])))

    print('Format for LaTeX in paper:')
    print(get_latex_format('Iters', iters))
    print(get_latex_format('Median Total Times', total_times))
    print(get_latex_format('Times Per Iter', times_per_iter))


if __name__ == '__main__':
    main()
