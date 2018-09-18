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
            attr = db.get_attr(cq.projs[pos])
            attrs.append(attr)
            if attr not in attrs_to_cqs:
                attrs_to_cqs[attr] = []
            attrs_to_cqs[attr].append(cqid)
        return attrs, attrs_to_cqs

class PositionPart(object):
    def __init__(self, pos, cqs, meta):
        self.pos = pos
        self.cqs = cqs
        self.meta = meta

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
            part = SinglePart(meta)
            key = self.part_key(part)
            if key not in parts:
                parts[key] = part
            parts[key].add(cqid, cq)
        return parts

    def part_key(self, part):
        return part.meta['types']

class PartByRange(object):
    def __init__(self, aig):
        self.aig = aig
        self.by_type_counter = {}

    def part_key(self, part):
        types = part.meta['types']
        if types not in self.by_type_counter:
            self.by_type_counter[types] = 0

        key = '{}/{}'.format(str(types), self.by_type_counter[types])
        self.by_type_counter[types] += 1
        return key

    def split_part(self, part, pos, pos_parts):
        new_parts = []
        for pos_part in pos_parts:
            intersect_cqs = set(part.cqs.keys()) & set(pos_part.cqs)

            new_part_meta = { 'types': part.meta['types'] }
            if 'by_pos' in part.meta:
                new_part_meta['by_pos'] = part.meta['by_pos']
            else:
                new_part_meta['by_pos'] = {}
            new_part_meta['by_pos'][pos] = pos_part.meta

            new_part = SinglePart(new_part_meta)
            for icq in intersect_cqs:
                new_part.add(icq, part.cqs[icq])
            new_parts.append(new_part)
        return new_parts

    def prune_non_maximal(self, parts):
        maximal_parts = []
        for i, part in enumerate(parts):
            maximal = True
            for j in range(i+1, len(parts)):
                if set(part.cqs.keys()) < set(parts[j].cqs.keys()):
                    maximal = False
                    break
            if maximal:
                maximal_parts.append(part)
        return maximal_parts

    # combine position-wise partitionings, then reduce to only maximal ones
    def combine_pos_parts(self, orig_part, pos_parts):
        intermediate_parts = [orig_part]
        for pos, pos_part_list in pos_parts.items():
            new_intermediate_parts = []
            for ip in intermediate_parts:
                split_parts = self.split_part(ip, pos, pos_part_list)
                new_intermediate_parts.extend(split_parts)
            intermediate_parts = self.prune_non_maximal(new_intermediate_parts)

        parts = {}
        for p in intermediate_parts:
            parts[self.part_key(p)] = p

        return parts

    def __call__(self, db, cqs):
        part_by_type = PartByType()
        by_type = part_by_type(db, cqs)

        parts = {}
        for types, part in by_type.items():
            pos_parts = {}
            for pos, type in enumerate(types):
                # collect all attrs in partition at position
                attrs, attrs_to_cqs = part.get_attrs_at_pos(db, pos)

                # find maximal cliques of these attrs
                cliques = self.aig.find_maximal_cliques(attrs)

                # pos_parts: the position-wise partitionings
                pos_parts[pos] = []
                for clique in cliques:
                    pos_part_cqs = []
                    for attr in clique:
                        # backtrack which CQs have which attrs
                        pos_part_cqs.extend(attrs_to_cqs[attr])

                    meta = self.aig.get_intersects(type, clique)
                    pos_part = PositionPart(pos, pos_part_cqs, meta)
                    pos_parts[pos].append(pos_part)

            # add new partitions from splitting this one to existing `parts`
            parts.update(self.combine_pos_parts(part, pos_parts))

        return parts
