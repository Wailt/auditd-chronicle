from collections import Counter

from pattern import delete_some_field, get_features

def tree_stats(tree):
    stat = tree.counts().replace('\t', '')
    total_depth = max([line.split(': ')[-1][:-1] for line in stat.split('\n')])
    total_width = max([line.split(': ')[1].split(" ")[0] for line in stat.split('\n')])

    return 'depth: ', total_depth, 'width: ', total_width


class ClusterTree:
    def __init__(self, event=["root"], deep=0, sim_level=0.01, fields=[]):
        self.prepocessed_events=dict()
        self.event = list(set(event))
        self.child = []
        self.deep = deep
        self.updated = event == ["root"]
        self.count = 1
        self.sim_level = sim_level
        self.fields = fields
        self.address = [0, ]
        self.updated = False

    def __str__(self):
        s = "\t" * self.deep + str(self.event) + ":deep = " + str(self.deep) + ",count:" + str(self.count) + ",\n"
        for i in self.child:
            s = s + str(i) + "\n"
        return s[:-1]

    def counts(self):
        s = "\t" * self.deep + 'children: ' + str(len(self.child)) + "; deep: " + str(self.deep) + ",\n"
        for i in self.child:
            s = s  + str(i.counts()) + "\n"
        return s[:-1]

    def stats(self):
        return tree_stats(self)

    def in_child(self, event):
        for ch in self.child:
            if self.sim(ch.event, event) > self.sim_level - 10e-3:
                return True
        return False

    def get_sim_child(self, event):
        valmax = self.sim(self.child[0].event, event)
        argmax = self.child[0]
        argn = 0
        n = 0
        for ch in self.child:
            if self.sim(ch.event, event) > valmax:
                valmax = self.sim(ch.event, event)
                argmax = ch
                argn = n
            n += 1
        return {"argmax": argmax, "address": argn}

    def filtered_update(self, event):
        event = set(event)
        if self.in_child(event):
            ch = self.get_sim_child(event)
            child = ch['argmax']
            event = set(event)
            minus = event & set(child.event)
            child.event = list(minus)

            self.address = [ch['address'], ]
            if len(event-minus) > 0:
#                 if not child.updated:
#                     child.filtered_update(set(child.event) - minus)
#                     child.event = set(minus)
#                     child.updated = True
                y = child.filtered_update(event-minus)
                return self.address + list(y)
            else:
                return self.address+[0]
        else:
            self.child.append(ClusterTree(event, deep=self.deep + 1, sim_level=self.sim_level if self.deep <=1 else 0.5))
            return [len(self.child) - 1, 0]

    def update(self, event):
        event_copy = {k:event[k] for k in event}
        if event:
            delete_some_field(event_copy)
            event = tuple(sorted(get_features(event_copy)))
            if event not in self.prepocessed_events:
                self.updated = True
                stamp = self.filtered_update(event)
                self.prepocessed_events[event]=stamp
            return self.prepocessed_events[event]
        else:
            return str([-1, ])

    def sim(self, t1, t2):
        condition = True
        first_counter = Counter(t1)
        second_counter = Counter(t2)
        united_counter = Counter(t1)
        united_counter.update(t2)

        for key in united_counter:
            parsed_key = key.split('$')[0]
            if parsed_key in self.fields and key not in first_counter:
                condition = False
        siml = 0
        for i in first_counter:
            if i in second_counter and first_counter[i] == second_counter[i]:
                siml += 1
        if min(len(first_counter), len(second_counter)) == 0:
            return 1
        else:
            return condition * float(siml) / float(len(first_counter))
