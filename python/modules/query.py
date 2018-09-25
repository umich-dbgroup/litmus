class Query(object):
    def __init__(self, cqid, query_str, projs, preds):
        self.cqid = cqid
        self.query_str = query_str
        self.projs = projs
        self.preds = preds

    def constrain(self, qig):
        query_str = self.query_str

        v = qig.get_vertex(self.cqid)

        constraints = []

        for pos, proj in enumerate(self.projs):
            pos_type = v.meta['types'][pos]
            pos_union = None

            for adj in v.get_adjacent():
                e = v.get_edge(adj)

                intersect = e.meta[pos]['intersect']

                if not intersect.is_empty():
                    if pos_union is None:
                        pos_union = intersect
                        continue

                    if pos_type == 'text' and intersect.is_all():
                        pos_union = None
                        break

                    pos_union.union(intersect)

            if pos_union:
                if pos_type == 'num':
                    constraints.append(u'({} >= {} AND {} <= {})'.format(proj, pos_union.min, proj, pos_union.max))
                elif pos_type == 'text':
                    constraints.append(u"({} IN ('{}'))".format(proj, u"','".join([v.replace("'", "''") for v in pos_union.vals])))

        if constraints:
            if 'where' not in query_str.lower():
                query_str += u' WHERE '
            else:
                query_str += u' AND '

            query_str += u' AND '.join(constraints)
        return query_str

    @staticmethod
    def tuple_in_query(db, t, query):
        if len(t) != len(query.projs):
            return False

        query_str = query.query_str

        if 'where' not in query_str.lower():
            query_str += u' WHERE '
        else:
            query_str += u' AND '

        # stores rel_alias_name -> attr_name for projs.
        # if multiple attrs for single rel, keeps the last one
        proj_alias_to_attr = {}
        preds = []
        for i, proj in enumerate(query.projs):
            if isinstance(t[i], unicode) or isinstance(t[i], str):
                preds.append(u"{} = '{}'".format(proj, t[i].replace("'", "''")))
            else:
                preds.append(u"{} = {}".format(proj, t[i]))

            attr = db.get_attr(proj)
            if not attr.pk:
                alias, attr_name = proj.split('.')
                proj_alias_to_attr[alias] = attr_name

        query_str += u' AND '.join(preds)
        query_str = re.sub(r'SELECT.*FROM', 'SELECT 1 FROM', query_str)

        for alias, attr_name in proj_alias_to_attr.items():
            index_regex = 'AS ({})'.format(alias)
            query_str = re.sub(index_regex, 'AS \g<1> USE INDEX ({})'.format(attr_name), query_str)

        query_str += ' LIMIT 1'

        try:
            cursor = db.cursor()
            cursor.execute(query_str)
            result = cursor.fetchone() is not None
            cursor.close()
        except Exception as e:
            cursor.close()
            if str(e).startswith('3024'):
                return False
        return result
