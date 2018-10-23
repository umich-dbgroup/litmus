import argparse
from collections import OrderedDict
import json
import os

NON_SPJ_WORDS = [
    'order by',
    'group by',
    'count(',
    'max(',
    'min(',
    'avg(',
    'sum('
]

def find_excludes(db):
    path = os.path.join('../data', '{}.json'.format(db))

    data = json.load(open(path))
    data = OrderedDict(sorted(data.items(), key=lambda x: x[0]))
    excludes = []
    for qid, task in data.items():
        # if answer not assigned, skip
        if not task['ans']:
            excludes.append(int(qid))
            continue

        # if any CQs contain non-SPJ keywords
        for cqid, cq in task['cqs'].items():
            if any(w in cq.lower() for w in NON_SPJ_WORDS):
                excludes.append(int(qid))
                break
    return excludes

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    args = parser.parse_args()

    print(','.join(str(i) for i in find_excludes(args.db)))

if __name__ == '__main__':
    main()
