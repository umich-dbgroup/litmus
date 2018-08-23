from collections import OrderedDict
import itertools
import os
import pickle
import time

from tqdm import tqdm

from database import AttributeSet

class TextIntersectDatabase(object):
    def __init__(self, db, path, max_size=7):
        self.db = db
        self.path = path
        self.max_size = max_size
        self.tis = None     # set size -> attr set -> vals

        self.load()
        self.build_intersects()

    def save(self):
        pickle.dump(self.tis, open(self.path, 'wb'))

    def load(self):
        if os.path.exists(self.path):
            self.tis = pickle.load(open(self.path, 'rb'))
            return True
        self.tis = {}
        return False

    # returns intersects with attrs by (# attrs, # intersecting vals) desc
    def get_ranked_intersects(self, attrs):
        intersects = []
        for size in range(2, len(attrs) + 1):
            for combo in itertools.combinations(attrs, size):
                attr_set = AttributeSet(combo)
                if size in self.tis and attr_set in self.tis[size]:
                    intersects.append(self.tis[size][attr_set])
        return sorted(intersects, key=lambda x: (len(x.attr_set), len(x.values)), reverse=True)

    # build intersects until max intersect size == len(attrs)
    # only includes non-empty intersects
    # RETURNS: dict with set size -> attr set -> vals
    def build_intersects(self):
        # calculate all pair of intersects from db first
        attrs = self.db.get_text_attrs()
        print('Total text attrs: {}'.format(len(attrs)))

        print('Building intersects for size 2...')
        self.calc_pair_intersects(attrs)
        print('Done building intersects for size 2.')

        # do sizes larger than 3 up to max_size
        for size in range(3, self.max_size + 1):
            print('Building intersects for size {}...'.format(size))
            if size not in self.tis:
                self.tis[size] = {}
            # if the previous size is empty, so is this
            if len(self.tis[size-1]) == 0:
                self.tis[size] = {}
                continue
            for combo in itertools.combinations(attrs, size):
                attr_set = AttributeSet(combo)

                # if already exists, continue
                if attr_set in self.tis[size]:
                    continue

                # figure out from lesser sets
                without_last = AttributeSet(combo[0:len(combo)-1])
                without_first = AttributeSet(combo[1:len(combo)])

                # if either of lesser sets are empty, skip
                if without_first not in self.tis[size-1] or without_last not in self.tis[size-1]:
                    continue

                without_last_vals = self.tis[size-1][without_last].values
                without_first_vals = self.tis[size-1][without_first].values

                vals = without_last_vals.intersection(without_first_vals)

                # only add if intersection nonempty
                if vals:
                    ti = TextIntersect(attr_set, vals)
                    self.tis[size][attr_set] = ti

            # save after each size computed
            self.save()
            print('Done building intersects for size {}.'.format(size))

    def calc_pair_intersects(self, attrs):
        if 2 not in self.tis:
            self.tis[2] = {}

        total = len(attrs) * (len(attrs)-1) / 2

        bar = tqdm(total=total, desc='Finding pair intersects')

        iter = 1
        cache_interval = 10
        for i, attr1 in enumerate(attrs):
            for j in range(i+1, len(attrs)):
                attr2 = attrs[j]
                attr_set = AttributeSet([attr1, attr2])
                if attr_set in self.tis[2]:
                    bar.update(1)
                    continue

                ti = TextIntersect.find_pair_intersects(self.db, attr_set)
                self.tis[2][attr_set] = ti

                # cache on every nth iteration
                if iter % cache_interval == 0:
                    self.save()

                bar.update(1)
                iter += 1

        bar.close()
        self.save()

def create_tmp_table_for_attr(db, attr):
    tmp_name = 'distinct_{}_{}'.format(attr.rel.name, attr.name)

    cursor = db.cursor()
    cursor.execute('CREATE TEMPORARY TABLE IF NOT EXISTS {} (INDEX ({}(15))) SELECT DISTINCT {} FROM {}'.format(tmp_name, attr.name, attr.name, attr.rel.name))
    cursor.close()

    return tmp_name

class TextIntersect(object):
    def __init__(self, attr_set, values):
        self.attr_set = attr_set
        self.values = values

    @staticmethod
    def find_pair_intersects(db, attr_set):
        # TODO: another option is to make this all in-database with stored tables.. is that easier?

        assert len(attr_set) == 2

        first = attr_set[0]
        second = attr_set[1]

        # use first letter of rel as alias
        first_rel_alias = first.rel.name[0]
        first_alias = '{}.{}'.format(first_rel_alias, first.name)

        second_rel_alias = second.rel.name[0]
        if second_rel_alias == first_rel_alias:
            second_rel_alias = second.rel.name[0:2]
        second_alias = '{}.{}'.format(second_rel_alias, second.name)

        proj_str = first_alias

        pred_str = '{} = {}'.format(first_alias, second_alias)

        # generate temp table to store distinct vals
        first_temp_name = create_tmp_table_for_attr(db, first)
        second_temp_name = create_tmp_table_for_attr(db, second)

        rel_strs = []
        rel_strs.append('{} AS {}'.format(first_temp_name, first_rel_alias))
        rel_strs.append('{} AS {}'.format(second_temp_name, second_rel_alias))

        from_str = ', '.join(rel_strs)

        query = 'SELECT DISTINCT {} FROM {} WHERE {}'.format(proj_str, from_str, pred_str)

        cursor = db.cursor()
        cursor.execute(query)
        values = set()
        for r in cursor:
            if r[0] is not None:
                values.add(r[0])
        cursor.close()

        return TextIntersect(attr_set, values)
