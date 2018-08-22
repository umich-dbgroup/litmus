import json
import re

def main():
    all_CQs = {}

    qid_re = re.compile('== QUERY ID: ([0-9]+) ==')

    with open('Templar.out') as f:
        reading = False
        cur_query_id = None
        cur_CQs = []
        for line in f:
            line = line.strip()

            m = qid_re.match(line)
            if m:
                cur_query_id = m.group(1)
                continue
            if line.startswith('--- SQL ---'):
                reading = True
                continue
            if line.startswith('-----------'):
                reading = False
                all_CQs[cur_query_id] = cur_CQs
                cur_query_id = None
                cur_CQs = []
                continue
            if reading:
                cur_CQs.append(line)

    data = {k.zfill(3):v for k,v in all_CQs.items()}
    with open('Templar.json', 'w') as outfile:
        json.dump(data, outfile, indent = 2, sort_keys=True)

if __name__ == '__main__':
    main()
