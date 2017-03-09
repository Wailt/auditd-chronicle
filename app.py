
from HiTree.hierarchy_tree import ClusterTree
from tagging import *

aggs = scan(n=1000, verbose=1)

logs = dict()
act = []
tree = ClusterTree(sim_level=0.2)

# sub-action tagging
adres_slice = 3
h = 2
n = 2
seq = sub_actions(aggs, tree, adres_slice=adres_slice, h=h)

#write tags to_elasticsearch
post_actions(seq, h=n, adres_slice=adres_slice)

f = open('tree.txt', 'w')
f.writelines(str(tree))
f.close()