from partitions import PartSet, SinglePart

# Query Intersection Graph
class QIG(object):
    def __init__(self, db, cqs):
        self.db = db
        self.cqs = cqs        # cqid -> cqs
        self.vertices = {}    # cqid -> QIGVertex
        self.posqigs = {}     # pos -> PosQIG

        self.init_vertices()
        self.construct_pos_qigs()
        self.combine_pos_qigs()

    def construct_pos_qigs(self):
        raise NotImplementedError

    def combine_pos_qigs(self):
        vlist = self.vertices.items()
        for i, v1item in enumerate(self.vertices.items()):
            for j in range(i+1, len(self.vertices)):
                v2item = self.vertices.items()[j]

                v1id = v1item[0]
                v2id = v2item[0]

                # only add an edge between two CQs if all PosQIGs have one
                if all(g.has_edge(v1id, v2id) for g in self.posqigs.values()):
                    self.add_edge(v1id, v2id)

    def init_vertices(self):
        raise NotImplementedError

    def add_vertex(self, cqid, meta):
        if cqid in self.vertices:
            raise Exception('Vertex {} already exists!'.format(cqid))
        self.vertices[cqid] = QIGVertex(cqid, meta)

    def get_vertex(self, cqid):
        if cqid not in self.vertices:
            raise Exception('Could not find vertex {}.'.format(cqid))
        return self.vertices[cqid]

    def get_cqids(self):
        return self.vertices.keys()

    def add_edge(self, cqid1, cqid2):
        v1 = self.get_vertex(cqid1)
        v2 = self.get_vertex(cqid2)

        # meta is a combination of posqig metas
        meta = {}
        for pos, posqig in self.posqigs.items():
            meta[pos] = posqig.get_meta(cqid1, cqid2)

        e = QIGEdge(meta)
        v1.add_neighbor(v2, e)
        v2.add_neighbor(v1, e)

    def get_meta(self, cqids):
        raise NotImplementedError

    def part_key(self, part):
        raise NotImplementedError

    def bron_kerbosch(self, cliques, R, P, X):
        if not P and not X:
            cliques.append(R)
            return cliques
        for cqid in P:
            singleton = set([cqid])
            N = set(self.get_vertex(cqid).get_adjacent())
            self.bron_kerbosch(cliques, R | singleton, P & N, X & N)
            P = P - singleton
            X = X | singleton
        return cliques

    def find_maximal_cliques(self):
        return self.bron_kerbosch([], set(), set(self.get_cqids()), set())

    def find_partition_set(self):
        cliques = self.find_maximal_cliques()

        parts = {}
        for clique in cliques:
            part = SinglePart(self.get_meta(clique))
            for cqid in clique:
                part.add(cqid, self.cqs[cqid])

            parts[self.part_key(part)] = part

        return PartSet(parts, self.cqs)

class QIGVertex(object):
    def __init__(self, cqid, meta):
        self.cqid = cqid
        self.meta = meta
        self.adjacent = {}     # cqid -> QIGEdge

    def add_neighbor(self, v, e):
        self.adjacent[v.cqid] = e

    def get_edge(self, cqid):
        if cqid not in self.adjacent:
            raise Exception('{} has no edge to {}.'.format(self.cqid, cqid))
        return self.adjacent[cqid]

    def get_adjacent(self):
        return self.adjacent.keys()

    def has_neighbor(self, cqid):
        return cqid in self.adjacent

class QIGEdge(object):
    def __init__(self, meta):
        self.meta = meta     # dict with info about query intersections

# Position-wise QIG
class PosQIG(object):
    def __init__(self, db, pos):
        self.db = db
        self.pos = pos
        self.vertices = {}     # cqid -> QIGVertex

    def add_vertex(self, cqid, meta):
        if cqid in self.vertices:
            raise Exception('Vertex {} already exists!'.format(cqid))
        self.vertices[cqid] = QIGVertex(cqid, meta)

    def get_vertex(self, cqid):
        if cqid not in self.vertices:
            raise Exception('Could not find vertex {}.'.format(cqid))
        return self.vertices[cqid]

    def add_edge(self, cqid1, cqid2, meta):
        v1 = self.get_vertex(cqid1)
        v2 = self.get_vertex(cqid2)

        e = QIGEdge(meta)
        v1.add_neighbor(v2, e)
        v2.add_neighbor(v1, e)

    def has_edge(self, cqid1, cqid2):
        return cqid1 in self.vertices \
            and cqid2 in self.vertices \
            and self.vertices[cqid1].has_neighbor(cqid2)

    def get_meta(self, cqid1, cqid2):
        v1 = self.get_vertex(cqid1)
        return v1.get_edge(cqid2).meta

class QIGByType(QIG):
    def init_vertices(self):
        for cqid, cq in self.cqs.items():
            attrs = []
            types = []
            for pos, proj in enumerate(cq.projs):
                attr = self.db.get_attr(proj)

                if isinstance(proj, dict):
                    attr_type = 'aggr'
                else:
                    attr_type = attr.type

                attrs.append(attr)
                types.append(attr_type)

            self.add_vertex(cqid, {
                'attrs': attrs,
                'types': types
            })

    def construct_pos_qigs(self):
        for cqid, cq in self.cqs.items():
            cq_types = self.get_vertex(cqid).meta['types']
            for pos, type in enumerate(cq_types):
                if pos not in self.posqigs:
                    self.posqigs[pos] = PosQIG(self.db, pos)
                posqig = self.posqigs[pos]

                posqig.add_vertex(cqid, { 'type': type })

                for vid, v in posqig.vertices.items():
                    # ignore if checking same vertex we just added
                    if vid == cqid:
                        continue

                    # add edge only if type is the same
                    if type == v.meta['type']:
                        posqig.add_edge(cqid, vid, {})

    def get_meta(self, cqids):
        if not cqids:
            raise Exception('Cannot get meta for empty cqids set.')
        v1 = self.get_vertex(list(cqids)[0])

        return v1.meta

    def part_key(self, part):
        return part.meta['types']

    def find_components(self):
        components = []
        found = set()
        for cqid in self.get_cqids():
            if cqid in found:
                continue

            component = set()
            component.add(cqid)
            component.update(self.get_vertex(cqid).get_adjacent())

            components.append(component)
            found.update(component)

        return components

    def find_maximal_cliques(self):
        return self.find_components()

class QIGByRange(QIGByType):
    def __init__(self, db, cqs, aig):
        self.aig = aig
        self.by_type_counter = {}
        super(QIGByRange, self).__init__(db, cqs)

    def construct_pos_qigs(self):
        for cqid, cq in self.cqs.items():
            cq_attrs = self.get_vertex(cqid).meta['attrs']
            cq_types = self.get_vertex(cqid).meta['types']
            for pos, attr in enumerate(cq_attrs):
                if pos not in self.posqigs:
                    self.posqigs[pos] = PosQIG(self.db, pos)
                posqig = self.posqigs[pos]

                posqig.add_vertex(cqid, {
                    'attr': attr,
                    'type': type
                })

                for vid, v in posqig.vertices.items():
                    # ignore if checking same vertex we just added
                    if vid == cqid:
                        continue

                    # add edge only if type is same and they intersect
                    e = self.aig.get_vertex(v.meta['attr']).get_edge(attr)
                    if type == v.meta['type'] and e:
                        posqig.add_edge(cqid, vid, {
                            'intersect': e.values
                        })

    def get_meta(self, cqids):
        if not cqids:
            raise Exception('Cannot get meta for empty cqids set.')

        cqids = list(cqids)
        v1 = self.get_vertex(cqids[0])
        size = len(v1.meta['attrs'])

        intersects = []
        for pos in range(0, size):
            attrs = [self.get_vertex(cqid).meta['attrs'][pos] for cqid in cqids]
            intersects.append(self.aig.get_intersects(attrs[0].type, attrs))

        return {
            'intersects': intersects,
            'types': v1.meta['types']
        }

    def part_key(self, part):
        types = part.meta['types']
        if types not in self.by_type_counter:
            self.by_type_counter[types] = 0

        key = '{}/{}'.format(str(types), self.by_type_counter[types])
        self.by_type_counter[types] += 1
        return key

    # TODO: do we need to implement something better than Bron-Kerbosch here?
    # def find_maximal_cliques(self):
