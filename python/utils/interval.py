class ColumnIntervals(object):
    def __init__(self, colnum):
        self.colnum = colnum
        self.attrs = []
        self.intervals = None     # call update() before retrieval!

    def top_n(self, n):
        return self.intervals[0:n+1]

    def interval_count(self):
        return len(self.intervals)

    def add_attr(self, attr):
        self.attrs.append(attr)

    # Requires all necessary attrs added before execution
    # Inspired by: https://stackoverflow.com/questions/18373509
    def update(self):
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
                intervals.append(Interval(cur_min, t[0], list(cur_attrs)))
                cur_min = t[0]

            if t[1] == 1:   # entry case
                cur_attrs.append(t[2])
            else:           # exit case
                cur_attrs.remove(t[2])

            last_pos = t[0]
            last_entry = t[1]

        sorted_intervals = sorted(intervals, key=lambda i: -i.attr_count())

        print('COLUMN {} INTERVALS'.format(self.colnum))
        for interval in sorted_intervals:
            print(interval.formatted())

        self.intervals = sorted_intervals
        return self.intervals

    @staticmethod
    def max_interval_count(column_intervals):
        max = 0
        for ci in column_intervals:
            if ci.interval_count() > max:
                max = ci.interval_count()
        return max

    @staticmethod
    def get_all_attrs(column_intervals):
        attrs = []
        for ci in column_intervals:
            attrs.extend(ci.attrs)
        return attrs

class Interval(object):
    def __init__(self, min, max, attrs):
        self.min = min
        self.max = max
        self.attrs = attrs

    def attr_count(self):
        return len(self.attrs)

    def formatted(self):
        return 'Interval: ({}, {}), Attrs: {}'.format(self.min, self.max, self.attr_count())
