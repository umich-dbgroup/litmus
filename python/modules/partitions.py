from collections import OrderedDict

class PartSet(object):
    def __init__(self, parts, cqs):
        self.cqs = cqs
        self.parts = parts
        self.executed = set()     # list of executed cqids

    def __len__(self):
        return len(self.parts)

    # returns iterator in sorted order
    def __iter__(self):
        return iter(self.parts.items())

    def __getitem__(self, index):
        return self.parts.items()[index]

    def cq_count(self):
        return len(self.cqs)

    def index(self, item):
        return self.parts.values().index(item)

    def update_executed(self, executed):
        self.executed.update(executed)

    def sort(self, sort_func=None):
        if sort_func is None:
            # default sort func is by number of CQs desc
            sort_func = lambda x: -len(x[1])
        self.parts = OrderedDict(sorted(self.parts.items(), key=sort_func))

class SinglePart(object):
    def __init__(self, meta=None):
        self.cqs = {}
        self.meta = meta

    def __len__(self):
        return len(self.cqs)

    def add(self, cqid, cq):
        self.cqs[cqid] = cq
