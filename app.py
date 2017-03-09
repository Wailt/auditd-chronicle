from collections import Counter

from HiTree.hierarchy_tree import ClusterTree
from cook import prepare_log
from es.elastic_query import *
from ngrams import Ngrams
from tagging import sub_actions, post_sub_actions

aggs = scan(n=1000, verbose=1)

logs = dict()
act = []
tree = ClusterTree(sim_level=0.2)

# sub-action tagging
adres_slice = 3
h = 2
seq = sub_actions(aggs, tree, adres_slice=adres_slice, h=h)


# post sub-action tagging
sessions = dict()
nums = dict()

for key in seq:
    sessions[key] = [(i[0][:adres_slice], i[1]) for i in sorted(seq[key], key=lambda x: x[1])]
    nums[key] = [i[1] for i in sorted(Counter(sessions[key]).items(), key=lambda x: x[0][1])]
    sessions[key] = [i[0] for i in sorted(set(sessions[key]), key=lambda x: x[1])]

# ngrams
ng = Ngrams(sessions=sessions, n=3)

# price-list
vals = list(map(lambda x: (x[0], x[1] * 10 ** (len(x[0])) - 1), ng.ngrams.items()))
tag_list = sorted(list(set(vals)), key=lambda x: x[1], reverse=True)



#tagging
res = post_sub_actions(sessions, tag_list, ng)

#write tags to_elasticsearch
act = []
for key in seq:
    superactions = []
    for i in range(len(res[key])):
        superactions += [res[key][i] for j in range(nums[key][i])]
    if len(seq[key]) > h:
        for j in range(len(seq[key])):
            i = seq[key][j]
            action = {'_op_type': 'update', u'_id': i[2], u'_type': u'auditd-parent', u'_index': u'wailt'}
            if superactions[j] == (-1,):
                superactions[j] = (-1, -1)
            action['doc'] = {'action': superactions[j][0], 'action_separator': superactions[j][1]}
            act.append(action)
            if len(act) > 10000:
                print 'acted'
                bulk(es, actions=act)
                act = []
bulk(es, actions=act)
