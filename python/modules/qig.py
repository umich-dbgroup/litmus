from .database import AttributeIntersect, AllAttributeIntersect
from .partitions import PartSet, SinglePart

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
        first_posqig = self.posqigs[0]

        explored = set()
        for i, v1item in enumerate(self.vertices.items()):
            v1id, v1 = v1item

            for v2id in first_posqig.vertices[v1id].get_adjacent():
                if v2id in explored:
                    continue

                v2 = self.vertices[v2id]

                # only add an edge between two CQs if all PosQIGs have one
                if all(g.has_edge(v1id, v2id) for g in self.posqigs.values()):
                    self.add_edge(v1id, v2id)

            explored.add(v1id)

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

    def remove_cq(self, cqid):
        if cqid not in self.vertices:
            raise Exception('Could not find vertex {}.'.format(cqid))
        for adj in self.vertices[cqid].get_adjacent():
            self.vertices[adj].remove_neighbor(cqid)

        self.vertices.pop(cqid, None)
        self.cqs.pop(cqid, None)

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
        return NotImplementedError

    def find_partition_set(self):
        cliques = self.find_maximal_cliques()

        parts = {}
        for clique in cliques:
            part = SinglePart(self.get_meta(clique))
            for cqid in clique:
                part.add(cqid, self.cqs[cqid])

            parts[self.part_key(part)] = part

        return PartSet(parts, self.cqs)

    def dot(self):
        stmts = []
        cqids = self.get_cqids()
        for i, cqid in enumerate(cqids):
            for j in range(i+1, len(cqids)):
                stmts.append('{} -- {}'.format(cqid, cqids[j]))
        return 'graph {{ {} }}'.format('; '.join(stmts))

    def update(self, cqs):
        diff = set(self.get_cqids()) - set(cqs.keys())
        for cqid in diff:
            self.remove_cq(cqid)

class QIGVertex(object):
    def __init__(self, cqid, meta):
        self.cqid = cqid
        self.meta = meta
        self.adjacent = {}     # cqid -> QIGEdge

    def add_neighbor(self, v, e):
        self.adjacent[v.cqid] = e

    def remove_neighbor(self, cqid):
        self.adjacent.pop(cqid, None)

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
        return str(tuple(part.meta['types']))

    def find_type_components(self):
        components = {}
        for cqid in self.get_cqids():
            types = tuple(self.get_vertex(cqid).meta['types'])
            if types not in components:
                components[types] = set()

            components[types].add(cqid)

        return components.values()

    def find_maximal_cliques(self):
        return self.find_type_components()

class QIGByRange(QIGByType):
    def __init__(self, db, cqs, aig):
        self.aig = aig
        self.by_type_counter = {}
        super(QIGByRange, self).__init__(db, cqs)

    def construct_pos_qigs(self):
        attr_maps = {}      # pos -> attrs -> cqids
        for cqid, cq in self.cqs.items():
            cq_attrs = self.get_vertex(cqid).meta['attrs']
            cq_types = self.get_vertex(cqid).meta['types']
            for pos, attr in enumerate(cq_attrs):
                if pos not in self.posqigs:
                    self.posqigs[pos] = PosQIG(self.db, pos)
                if pos not in attr_maps:
                    attr_maps[pos] = {}
                posqig = self.posqigs[pos]

                posqig.add_vertex(cqid, {
                    'attr': attr,
                    'type': type
                })

                if attr not in attr_maps[pos]:
                    attr_maps[pos][attr] = set()
                attr_maps[pos][attr].add(cqid)

        for pos, posqig in self.posqigs.items():
            for v1id, v1 in posqig.vertices.items():
                v1attr = v1.meta['attr']

                # same attribute case
                for v2id in attr_maps[pos][v1attr]:
                    if v1id == v2id:
                        continue
                    if v1attr.type == 'text':
                        posqig.add_edge(v1id, v2id, {
                            'intersect': AllAttributeIntersect('text')
                        })
                    elif v1attr.type == 'num':
                        posqig.add_edge(v1id, v2id, {
                            'intersect': AttributeIntersect('num', min=v1attr.min, max=v1attr.max)
                        })

                # intersecting attributes case
                v1aig = self.aig.get_vertex(v1attr)
                for v2attr in v1aig.get_adjacent():
                    if v2attr in attr_maps[pos]:
                        e = v1aig.get_edge(v2attr)
                        for v2id in attr_maps[pos][v2attr]:
                            posqig.add_edge(v1id, v2id, {
                                'intersect': e.intersect
                            })

    def get_meta(self, cqids):
        if not cqids:
            raise Exception('Cannot get meta for empty cqids set.')

        cqids = sorted(list(cqids))
        v1 = self.get_vertex(cqids[0])

        return {
            'types': v1.meta['types'],
            'cqids': cqids
        }

    def part_key(self, part):
        types = tuple(part.meta['types'])
        if types not in self.by_type_counter:
            self.by_type_counter[types] = 0

        key = '{}/{}'.format(str(types), self.by_type_counter[types])
        self.by_type_counter[types] += 1
        return key

    def find_pivot(self, P, X):
        max_size = 0
        max_cqid = None
        max_adj = None

        for cqid in P | X:
            adj = set(self.get_vertex(cqid).get_adjacent())
            size = len(P & adj)

            if size >= max_size:
                max_size = size
                max_cqid = cqid
                max_adj = adj

        return max_cqid, max_adj

    def tomita(self, cliques, R, P, X):
        if not P and not X:
            cliques.append(R)
            return cliques
        u, u_adj = self.find_pivot(P, X)
        for cqid in P - u_adj:
            singleton = set([cqid])
            N = set(self.get_vertex(cqid).get_adjacent())
            self.tomita(cliques, R | singleton, P & N, X & N)
            P = P - singleton
            X = X | singleton
        return cliques

    def find_maximal_cliques(self):
        results = []
        components = self.find_type_components()

        for c in components:
            cliques = self.tomita([], set(), c, set())
            results.extend(cliques)

        return results
