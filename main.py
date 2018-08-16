from __future__ import division, print_function

import argparse
from collections import OrderedDict
from email.mime.text import MIMEText
import json
import smtplib
import time

# from moz_sql_parser import parse
import mysql.connector
from progress.bar import ChargingBar

def send_email(to_email, subject, message):
    from_email = 'cannoliemailer@gmail.com'
    msg = MIMEText(message)
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = to_email

    s = smtplib.SMTP('smtp.gmail.com', 587)
    s.ehlo()
    s.starttls()
    s.ehlo()
    s.login(from_email, 'cannoli123')
    s.sendmail(from_email, [to_email], msg.as_string())
    s.quit()

def execute_query(conn, cq):
    cursor = conn.cursor()

    try:
        cursor.execute(cq)

        cq_tuples = set()
        for result in cursor:
            cq_tuples.add(result)
        cursor.close()
        return cq_tuples
    except Exception, exc:
        cursor.close()
        if str(exc).startswith('3024'):
            raise Exception('Timeout: Query timed out.')

def stats(conn, qid, cqs):
    print("QUERY {}".format(qid))
    start = time.time()

    tuples = {}
    valid_cqs = 0
    timed_out = 0

    bar = ChargingBar('Running CQs', max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

    for cq in cqs:
        # print(cq)
        # parsed = parse(cq)
        #
        # projs = []
        # for sel in parsed['select']:
        #     if 'distinct' in sel['value']:
        #         projs.append(sel['value']['distinct'])
        #     else:
        #         projs.append(sel['value'])

        try:
            cq_tuples = execute_query(conn, cq)

            if len(cq_tuples) > 0:
                valid_cqs += 1

            for t in cq_tuples:
                if t in tuples:
                    tuples[t] += 1
                else:
                    tuples[t] = 1
        except Exception, exc:
            if str(exc).startswith('Timeout'):
                timed_out += 1

        bar.next()

    bar.finish()

    print("Done executing CQs [{}s]".format(time.time() - start))

    sorted_tuples = OrderedDict(sorted(tuples.items(), key=lambda t: t[1], reverse=True))

    print('Total CQs: {}'.format(len(cqs)))
    print('Timed Out CQs: {}'.format(timed_out))
    print('Valid CQs: {}'.format(valid_cqs))
    if valid_cqs > 0:
        print('Avg. tuples per CQ: {}'.format(len(tuples)/valid_cqs))
        k = 5
        print('Tuples with most CQ overlap: ', end='')
        for tuple, count in sorted_tuples.items()[0:k]:
            print(count, end=', ')
        print('\n')

def dist(cqs, t, cqids):
    Q_len = len(cqs)
    C_t = len(cqids)
    # TODO: when probabilities are unequal, p_t should be changed
    p_t = C_t / Q_len

    return ((Q_len - C_t) * p_t) + (C_t * (1 - p_t));

def exhaustive(conn, qid, cqs):
    print("QUERY {}: Exhaustive".format(qid))
    start = time.time()

    valid_cqs = []
    timed_out = 0
    tuples = {}

    bar = ChargingBar('Running CQs', max=len(cqs), suffix='%(index)d/%(max)d (%(percent)d%%)')

    for cq in cqs:
        try:
            cq_tuples = execute_query(conn, cq)

            if len(cq_tuples) > 0:
                cqid = len(valid_cqs)
                valid_cqs.append(cq)

                for t in cq_tuples:
                    if t not in tuples:
                        tuples[t] = []
                    tuples[t].append(cqid)
        except Exception, exc:
            if str(exc).startswith('Timeout'):
                timed_out += 1
        bar.next()
    bar.finish()
    print("Done executing CQs [{}s]".format(time.time() - start))

    print('Total CQs: {}'.format(len(cqs)))
    print('Timed Out CQs: {}'.format(timed_out))
    print('Valid CQs: {}'.format(len(valid_cqs)))

    print("Calculating dist values...")
    start = time.time()
    tuple_dists = {}
    for t, cqids in tuples.items():
        tuple_dists[t] = dist(valid_cqs, t, cqids)

    sorted_tuple_dists = OrderedDict(sorted(tuple_dists.items(), key=lambda t: t[1], reverse=True))
    print("Done calculating dists [{}s]".format(time.time() - start))
    k = 10
    print("Top tuples:")
    for t, dist_val in sorted_tuple_dists.items()[0:k]:
        print("{}, Dist: {}, # CQs: {}".format(t, dist_val, len(tuples[t])))
    print()

def execute_mode(mode, conn, qid, cqs):
    if mode == 'stats':
        stats(conn, qid, cqs)
    elif mode == 'exhaustive':
        exhaustive(conn, qid, cqs)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    parser.add_argument('mode', choices=['stats', 'exhaustive'])
    parser.add_argument('--qid', type=int)
    parser.add_argument('--timeout', type=int, default=15000)
    parser.add_argument('--email')
    args = parser.parse_args()

    conn = mysql.connector.connect(user='root', password='', host='127.0.0.1', database=args.db)

    # set query timeout
    cursor = conn.cursor()
    cursor.execute('SET SESSION MAX_EXECUTION_TIME={}'.format(args.timeout))
    cursor.close()

    with open(args.db + '.json') as f:
        data = json.load(f)

    data = {int(k):v for k,v in data.items()}
    sorted_data = OrderedDict(sorted(data.items(), key=lambda t: t[0]))

    if args.qid is not None:
        # if executing single query
        execute_mode(args.mode, conn, args.qid, data[args.qid])
    else:
        # if executing all queries
        for qid, cqs in sorted_data.items():
            execute_mode(args.mode, conn, qid, cqs)

    if args.email is not None:
        send_email(args.email, 'Done {}'.format(args.db), 'Done')

if __name__ == '__main__':
    main()
