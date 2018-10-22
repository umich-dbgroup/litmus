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
            if attr1 not in self.vertices:
                attr1_v = self.add_vertex(attr1)
            else:
                attr1_v = self.vertices[attr1]
            for j in range(i+1, len(attrs)):
                attr2 = attrs[j]

                if attr2 not in self.vertices:
                    attr2_v = self.add_vertex(attr2)
                else:
                    attr2_v = self.vertices[attr2]

                print('Finding intersections for {}, {}...'.format(attr1, attr2))
                intersects = self.db.find_intersects(attr1, attr2)
                if intersects:
                    self.add_edge(attr1_v, attr2_v, intersects)

        print('Done generating AIG. [{}s]'.format(time.time() - start))

    def add_vertex(self, attr):
        self.vertices[attr] = AIGVertex(attr)
        return self.vertices[attr]

    def get_vertex(self, attr):
        return self.vertices[attr]

    def add_edge(self, frm, to, intersect):
        e = AIGEdge(intersect)
        frm.add_neighbor(to, e)
        to.add_neighbor(frm, e)

    def get_attrs(self):
        return self.vertices.keys()

    def get_vertices(self):
        return self.vertices.values()

class AIGVertex(object):
    def __init__(self, attr):
        self.attr = attr
        self.adjacent = {}     # Attribute -> AIGEdge

    def add_neighbor(self, v, e):
        self.adjacent[v.attr] = e

    def get_edge(self, attr):
        if attr in self.adjacent:
            return self.adjacent[attr]
        else:
            return None

    def get_adjacent(self):
        return self.adjacent.keys()

    def get_id(self):
        return self.id

class AIGEdge(object):
    def __init__(self, intersect):
        self.intersect = intersect
