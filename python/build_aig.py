import argparse
import ConfigParser
import os
import pickle
import time

from modules.database import Database
from modules.aig import AIG

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=1000000)

    aig = AIG(db, os.path.join(config.get('aig', 'dir'), args.db + '.aig'))

if __name__ == '__main__':
    main()
