from collections import OrderedDict

class PartSet(object):
    def __init__(self, cqs):
        self.cqs = cqs
        self.parts = None
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

    def partition(self, db, part_func):
        self.parts = part_func(db, self.cqs)

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

    def get_attrs_at_pos(self, db, pos):
        attrs = []
        attrs_to_cqs = {}

        for cqid, cq in self.cqs.items():
            attrs.append(db.get_attr(cq.projs[pos]))
            if attr not in attrs_to_cqs:
                attrs_to_cqs[attr] = []
            attrs_to_cqs[attr].append(cqid)
        return attrs, attrs_to_cqs

class PartByType(object):
    def __call__(self, db, cqs):
        parts = {}
        for cqid, cq in cqs.items():
            proj_types = ()
            for colnum, proj in enumerate(cq.projs):
                # if colnum not in self.attrs_to_cqs:
                #     self.attrs_to_cqs[colnum] = {}

                attr = db.get_attr(proj)

                if isinstance(proj, dict):
                    attr_type = 'aggr'
                else:
                    attr_type = attr.type
                    # if colnum not in self.attrs_to_cqs:
                    #     self.attrs_to_cqs[colnum] = {}
                    # if attr not in self.attrs_to_cqs[colnum]:
                    #     self.attrs_to_cqs[colnum][attr] = []
                    # self.attrs_to_cqs[colnum][attr].append((cqid, proj))

                proj_types = proj_types + (attr_type,)

            meta = { 'types': proj_types }
            key = self.part_key(meta)
            if key not in parts:
                parts[key] = SinglePart(meta)
            parts[key].add(cqid, cq)
        return parts

    def part_key(self, meta):
        return meta['types']

class PartByRange(object):
    def __init__(self, aig):
        self.aig = aig

    # combine position-wise partitionings, then reduce to only maximal ones
    def combine_pos_parts(self, pos_parts):
        for pos, pos_part in pos_parts.items():
            # TODO: finding combinations with partitions at another pos is a NIGHTMARE?!?!

        # TODO: return a dict with part_key -> SinglePart

    def __call__(self, db, cqs):
        part_by_type = PartByType()
        by_type = part_by_type(db, cqs)

        parts = {}
        for types, part in by_type:
            pos_parts = {}
            for pos, type in types:
                # collect all attrs in partition at position
                attrs, attrs_to_cqs = part.get_attrs_at_pos(db, pos)

                # find maximal cliques of these attrs
                cliques = self.aig.find_maximal_cliques(attrs)

                # pos_parts: the position-wise partitionings
                pos_parts[pos] = []
                for clique in cliques:
                    pos_part = []
                    for attr in clique:
                        # backtrack which CQs have which attrs
                        pos_part.extend(attrs_to_cqs[attr])
                    pos_parts[pos].append(pos_part)

            # add new partitions from splitting this one to existing `parts`
            parts.update(self.combine_pos_parts(pos_parts))

        return parts

    def part_key(self, meta):
        return meta['types']
