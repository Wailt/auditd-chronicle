from collections import Counter
from copy import copy


def tree_stats(tree):
    stat = tree.counts().replace('\t', '')
    total_depth = max([int(line.split(': ')[1].split(';')[0]) for line in stat.split('\n')])
    total_width = max([int(line.split(': ')[2].split(";")[0][:-1]) for line in stat.split('\n')])
    return 'depth: ', total_depth, 'width: ', total_width


def get_features(event):
    features = []
    for key, val in event.iteritems():
        features.append(str(key) + "$" + str(val))
    return features


class ClusterTree:
    def __init__(self, event=("root",), deep=0, sim_level=0.01, fields=[]):
        self.prepocessed_events = dict()
        self.event = list(set(event))
        self.child = []
        self.deep = deep
        self.updated = event == ["root"]
        self.sim_level = sim_level

        ## fields - list of primal keys
        self.fields = fields
        self.address = [0, ]
        self.updated = False

    def __str__(self):
        s = "\t" * self.deep + str(self.event) + ":deep = " + str(self.deep) + ",\n"
        for i in self.child:
            s = s + str(i) + "\n"
        return s[:-1]

    def counts(self):
        s = "\t" * self.deep + 'children: ' + str(len(self.child)) + "; deep: " + str(self.deep) + ",\n"
        for i in self.child:
            s = s + str(i.counts()) + "\n"
        return s[:-1]

    def stats(self):
        return tree_stats(self)

    def is_empty(self):
        return len(self.child) == 0

    def get_sim_child(self, event):
        if not self.is_empty():
            sims = [self.sim(ch.event, event) for ch in self.child]
            valmax = max(sims)
            argn = sims.index(valmax)
            return {"argmax": self.child[argn], "address": argn} if valmax > self.sim_level - 10e-3 else None
        return None

    def filtered_update(self, event):
        event = set(event)
        ch = self.get_sim_child(event)
        if ch:
            child = ch['argmax']
            minus = event & set(child.event)
            self.address = [ch['address'], ]
            if len(event - minus) > 0:
                if not child.updated and len(minus) > 0:
                    child.filtered_update(set(child.event) - minus)
                    child.event = list(minus)
                    child.updated = True
                y = child.filtered_update(event - minus)
                return self.address + list(y)
            else:
                return self.address + [0]
        else:
            self.child.append(
                ClusterTree(event, deep=self.deep + 1, sim_level=self.sim_level if self.deep <= 1 else 0.5))
            return [len(self.child) - 1, 0]

    def update(self, event):
        event_copy = copy(event)
        if event:
            event = tuple(sorted(get_features(event_copy)))
            if event not in self.prepocessed_events:
                self.updated = True
                stamp = self.filtered_update(event)
                self.prepocessed_events[event] = stamp
            return self.prepocessed_events[event]
        else:
            return str([-1, ])

    def sim(self, t1, t2):
        first_counter = Counter(t1)
        second_counter = Counter(t2)
        united_counter = Counter(t1)
        united_counter.update(t2)

        condition = min([not (key.split('$')[0] in self.fields and key not in first_counter)
                         for key in united_counter] + [True])

        siml = sum([first_counter[i] == second_counter[i] for i in first_counter if i in second_counter] + [0])

        if min(len(first_counter), len(second_counter)) == 0:
            return 1
        else:
            return condition * float(siml) / float(len(first_counter))
