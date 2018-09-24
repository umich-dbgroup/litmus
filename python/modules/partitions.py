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
        return iter(self.parts.values())

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

    def constrain_query(self, cq):
        query_str = cq.query_str

        constraints = []
        for pos, proj in enumerate(cq.projs):
            intersect = self.meta['intersects'][pos]
            if not intersect.is_empty():
                if not intersect.is_empty():
                    if intersect.type == 'num':
                        constraints.append(u'({} >= {} AND {} <= {})'.format(proj, intersect.min, proj, intersect.max))
                    elif not intersect.is_all():
                        constraints.append(u"{} IN ('{}')".format(proj, u"','".join([v.replace("'", "''") for v in intersect.vals])))

        if constraints:
            if 'where' not in query_str.lower():
                query_str += u' WHERE '
            else:
                query_str += u' AND '

            query_str += u' AND '.join(constraints)
        return query_str
