import argparse
import ConfigParser

from modules.database import Database

TEXT_INDEX_SIZE = 15

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('db')
    args = parser.parse_args()

    config = ConfigParser.RawConfigParser(allow_no_value=True)
    config.read('config.ini')

    db = Database(config.get('database', 'user'), config.get('database', 'pw'), config.get('database', 'host'), args.db, config.get('database', 'cache_dir'), timeout=config.get('database', 'timeout'))

    rels = db.get_relations()
    for relname, rel in rels.items():
        # find non-unique indexes
        cursor = db.cursor()
        cursor.execute('SHOW INDEXES FROM {}'.format(relname))
        index_rows = cursor.fetchall()
        cursor.close()

        # drop all indexes
        unique_keys = []
        for row in index_rows:
            non_unique = row[1]
            index_name = row[2]
            column_name = row[4]
            if non_unique == 1:
                cursor = db.cursor()
                qstr = 'ALTER TABLE {} DROP INDEX {}'.format(relname, index_name)
                print(qstr)
                try:
                    cursor.execute(qstr)
                    cursor.close()
                except Exception as e:
                    if str(e).startswith('1553'):
                        print('Did not drop {}.{} because of foreign key.'.format(relname, index_name))
            else:
                unique_keys.append(column_name)

        # add all needed indexes for each attribute
        for attr_name, attr in rel.attrs.items():
            if attr_name in unique_keys:
                continue

            if attr.type == 'num':
                cursor = db.cursor()
                qstr = 'ALTER TABLE {} ADD INDEX {}({})'.format(relname, attr_name, attr_name)
                print(qstr)
                cursor.execute(qstr)
                cursor.close()
            elif attr.type == 'text':
                cursor = db.cursor()
                qstr = 'ALTER TABLE {} ADD INDEX {}({}({}))'.format(relname, attr_name, attr_name, TEXT_INDEX_SIZE)
                print(qstr)
                cursor.execute(qstr)
                cursor.close()

                cursor = db.cursor()
                qstr = 'ALTER TABLE {} ADD FULLTEXT {}_ft({})'.format(relname, attr_name, attr_name)
                print(qstr)
                cursor.execute(qstr)
                cursor.close()



if __name__ == '__main__':
    main()
