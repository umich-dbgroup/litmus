class ColumnTextIntersects(object):
    def __init__(self, colnum, attrs, tidb):
        self.colnum = colnum
        self.attrs = attrs
        self.intersects = tidb.get_ranked_intersects(attrs)

    def __len__(self):
        return len(self.intersects)

    def top_n(self, n):
        return self.intersects[0:n]

    def __unicode__(self):
        result = u'ColumnTextIntersects [Col: {}]'.format(self.colnum) + u'\n'
        for intersect in self.intersects:
            result += unicode(intersect) + u'\n'
        return result

    def __str__(self):
        return unicode(self).encode('utf-8')

class ColumnNumIntervals(object):
    def __init__(self, colnum, attrs):
        self.colnum = colnum
        self.attrs = filter(lambda a: a.min is not None and a.max is not None, attrs)
        self.find_intervals()

    def __len__(self):
        return len(self.intervals)

    def __unicode__(self):
        result = u'ColumnNumIntervals [Col: {}]'.format(self.colnum) + u'\n'
        for interval in self.intervals:
            result += unicode(interval) + u'\n'
        return result

    def __str__(self):
        return unicode(self).encode('utf-8')

    def top_n(self, n):
        return self.intervals[0:n]

    # Requires all necessary attrs added before execution
    # Inspired by: https://stackoverflow.com/questions/18373509
    def find_intervals(self):
        # case that there's no attrs (e.g. empty attrs)
        if not self.attrs:
            self.intervals = []
            return self.intervals

        transitions = []
        for attr in self.attrs:
            transitions.append((attr.min, 1, attr))
            transitions.append((attr.max, -1, attr))

        sorted_transitions = sorted(transitions, key=lambda x: (x[0], -x[1]))
        intervals = []

        cur_attrs = []
        cur_min = sorted_transitions[0][0]
        last_pos = sorted_transitions[0][0]
        last_entry = sorted_transitions[0][1]
        for t in sorted_transitions:
            # two types of transitions: new pos, or same pos, entry to exit
            if last_pos != t[0] or last_entry != t[1]:
                # save interval
                intervals.append(NumInterval(cur_min, t[0], list(cur_attrs)))
                cur_min = t[0]

            if t[1] == 1:   # entry case
                cur_attrs.append(t[2])
            else:           # exit case
                cur_attrs.remove(t[2])

            last_pos = t[0]
            last_entry = t[1]

        sorted_intervals = sorted(intervals, key=lambda i: -i.attr_count())

        self.intervals = sorted_intervals
        return self.intervals

class NumInterval(object):
    def __init__(self, min, max, attrs):
        self.min = min
        self.max = max
        self.attrs = attrs

    def attr_count(self):
        return len(self.attrs)

    def __unicode__(self):
        return u'Interval: ({}, {}), Attrs: {}'.format(self.min, self.max, self.attr_count())

    def __str__(self):
        return unicode(self).encode('utf-8')
