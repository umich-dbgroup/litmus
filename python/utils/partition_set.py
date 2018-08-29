from collections import OrderedDict

from overlap_types import ColumnNumIntervals, ColumnTextIntersects, NumInterval
from text_intersect import TextIntersect

class ProjPartition(object):
    def __init__(self, types, cqs):
        self.types = types
        self.cqs = cqs
        self.overlaps = None  # colnum -> ColumnNumIntervals or ColumnTextIntersects

    def __len__(self):
        return len(self.cqs)

    def add_cq(self, cqid, cq):
        self.cqs[cqid] = cq

    def top_n_col_overlaps(self, n, colnum):
        if colnum in self.overlaps:
            return self.overlaps[colnum].top_n(n)
        else:
            return None

    @staticmethod
    def attrs_from_overlaps(top_n):
        attrs = []
        for o in top_n:
            if isinstance(o, NumInterval):
                attrs.extend(o.attrs)
            elif isinstance(o, TextIntersect):
                attrs.extend(o.attr_set.attrs)
        return attrs

    def find_overlaps(self, attrs_to_cqs, tidb):
        self.overlaps = {}

        for colnum, type in enumerate(self.types):
            attrs = filter(lambda a: a.type == type, attrs_to_cqs[colnum].keys())

            if type == 'num':
                self.overlaps[colnum] = ColumnNumIntervals(colnum, attrs)
            elif type == 'text':
                self.overlaps[colnum] = ColumnTextIntersects(colnum, attrs, tidb)

    def max_overlap_count(self):
        return max(len(o) for o in self.overlaps.values())

class PartitionSet(object):
    def __init__(self, db, cqs):
        self.partitions = {}    # types -> ProjPartition
        self.attrs_to_cqs = {}  # stores colnum -> attr -> [(cqid, frag)]

        for cqid, cq in cqs.items():
            proj_types = ()
            for colnum, proj in enumerate(cq.projs):
                if colnum not in self.attrs_to_cqs:
                    self.attrs_to_cqs[colnum] = {}

                attr = db.get_attr(proj)

                if isinstance(proj, dict):
                    attr_type = 'aggr'
                else:
                    attr_type = attr.type
                    if colnum not in self.attrs_to_cqs:
                        self.attrs_to_cqs[colnum] = {}
                    if attr not in self.attrs_to_cqs[colnum]:
                        self.attrs_to_cqs[colnum][attr] = []
                    self.attrs_to_cqs[colnum][attr].append((cqid, proj))

                proj_types = proj_types + (attr_type,)

            if proj_types not in self.partitions:
                self.partitions[proj_types] = ProjPartition(proj_types, {})
            self.partitions[proj_types].add_cq(cqid, cq)

        self.sorted_partitions = OrderedDict(sorted(self.partitions.items(), key=lambda t: len(t[1]), reverse=True))

    # returns iterator in sorted order
    def __iter__(self):
        return iter(self.sorted_partitions.items())

    def __getitem__(self, index):
        return self.sorted_partitions.items()[index]

    def find_overlaps(self, tidb):
        for types, part in self.partitions.items():
            part.find_overlaps(self.attrs_to_cqs, tidb)
