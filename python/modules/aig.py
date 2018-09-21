import os
import pickle
import time

# Attribute Intersection Graph
class AIG(object):
    def __init__(self, db, path):
        self.db = db
        self.path = path
        self.vertices = {}    # Attribute -> AIGVertex

        if not self.load():
            self.generate()
            self.save()

    def load(self):
        print('Loading AIG from cache...')
        start = time.time()
        if os.path.exists(self.path):
            self.vertices = pickle.load(open(self.path, 'rb'))
            print('Loaded. [{}s]'.format(time.time() - start))
            return True
        print('AIG not in cache.')
        return False

    def save(self):
        print('Saving AIG to cache...')
        start = time.time()
        pickle.dump(self.vertices, open(self.path, 'wb'))
        print('Saved. [{}s]'.format(time.time() - start))

    def generate(self):
        print('Generating AIG...')
        start = time.time()

        attrs = self.db.get_all_attrs()
        for i, attr1 in enumerate(attrs):
            for j in range(i+1, len(attrs)):
                attr2 = attrs[j]

                print('Finding intersections for {}, {}...'.format(attr1, attr2))
                intersects = self.db.find_intersects(attr1, attr2)
                if intersects:
                    self.add_edge(attr1, attr2, intersects)
        print('Done generating AIG. [{}s]'.format(time.time() - start))

    def add_vertex(self, attr):
        self.vertices[attr] = AIGVertex(attr)

    def get_vertex(self, attr):
        return self.vertices[attr]

    def add_edge(self, frm_attr, to_attr, values):
        if frm_attr not in self.vertices:
            self.add_vertex(frm_attr)
        if to_attr not in self.vertices:
            self.add_vertex(to_attr)

        frm = self.get_vertex(frm_attr)
        to = self.get_vertex(to_attr)

        e = AIGEdge(values)
        frm.add_neighbor(to, e)
        to.add_neighbor(frm, e)

    def get_attrs(self):
        return self.vertices.keys()

    def get_vertices(self):
        return self.vertices.values()

    def get_num_intersects(self, attrs):
        largest_min = max(a.min for a in attrs)
        smallest_max = min(a.max for a in attrs)

        return AttributeIntersect('num', min=largest_min, max=smallest_max)

    def get_text_intersects(self, attrs):
        attrs = list(attrs)

        # select a single attr, find edges to all other attrs
        attr = attrs[0]
        vals = None
        for i in range(1, len(attrs)):
            e = self.get_vertex(attr).get_edge(attrs[i])

            if not e:
                raise Exception('No edge found between {} and {}!'.format(attr, attrs[i]))

            if vals is None:
                vals = e.values.vals
            else:
                vals &= e.values.vals

        return AttributeIntersect('text', vals=vals)

    def get_intersects(self, type, attrs):
        if type == 'num':
            return self.get_num_intersects(attrs)
        elif type == 'text':
            return self.get_text_intersects(attrs)
        else:
            return None

    # def bron_kerbosch(self, cliques, R, P, X):
    #     if not P and not X:
    #         cliques.append(R)
    #         return cliques
    #     for attr in P:
    #         singleton = set([attr])
    #         N = set(self.get_vertex(attr).get_adjacent())
    #         self.bron_kerbosch(cliques, R | singleton, P & N, X & N)
    #         P = P - singleton
    #         X = X | singleton
    #     return cliques
    #
    # def find_maximal_cliques(self, attrs):
    #     return self.bron_kerbosch([], set(), set(attrs), set())


class AIGVertex(object):
    def __init__(self, attr):
        self.attr = attr
        self.adjacent = {}     # Attribute -> AIGEdge

    def add_neighbor(self, v, e):
        self.adjacent[v.attr] = e

    def get_edge(self, attr):
        return self.adjacent[attr]

    def get_adjacent(self):
        return self.adjacent.keys()

    def get_id(self):
        return self.id

class AIGEdge(object):
    def __init__(self, values):
        self.values = values
