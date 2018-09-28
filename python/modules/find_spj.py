import argparse
from collections import OrderedDict
import json
import os

NON_SPJ_WORDS = [
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
        for cqid, cq in task['cqs'].items():
            if any(w in cq for w in NON_SPJ_WORDS):
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
