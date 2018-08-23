import argparse
import ConfigParser
import os
import pickle
import time

from utils.database import Database
from utils.text_intersect import TextIntersect, TextIntersectDatabase

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_path'), timeout=None)

    tidb = TextIntersectDatabase(db, os.path.join(config.get('tidb', 'dir'), args.db + '.tidb'))
    tidb.load()
    tidb.build_intersects()

if __name__ == '__main__':
    main()
