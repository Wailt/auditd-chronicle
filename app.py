
from HiTree.hierarchy_tree import ClusterTree
from tagging import *

aggs = scan(n=20000, verbose=1)

logs = dict()
act = []
tree = ClusterTree(sim_level=0.2)

# sub-action tagging
adres_slice = 3
h = 1
n = 3
seq = sub_actions(aggs, tree, adres_slice=adres_slice, h=h)

#write tags to_elasticsearch
post_actions(seq, h=n, adres_slice=adres_slice)

f = open('tree.txt', 'w')
f.writelines(str(tree))
f.close()

f = open('counts.txt', 'w')
f.writelines(tree.counts())
f.close()

print tree.stats()
