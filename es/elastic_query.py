from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.query import Type, HasChild, MatchAll

from config import *

es = Elasticsearch(host=host, timeout=120)
q_parent = Type(value='auditd-parent')
q_has_child = HasChild(type='auditd-child', query=MatchAll(),
                       inner_hits={"sort": {"@timestamp": "asc"}, 'size': 100})

search = Search(using=es, index=index).sort({"@timestamp": {"order": "asc"}})
search.query = Q((q_has_child | (q_parent & ~q_has_child) ))


def scan(n=10, verbose=False):
    i = 0
    aggs = dict()
    for hit in search.scan():
        aggs.setdefault(hit.ses, [])
        aggs[hit.ses].append((hit, hit.meta.inner_hits['auditd-child'].hits.hits))

        if i % 1000 == 0 and verbose:
            print str(round(100*i/float(n), 2)) + "%"

        if i > n:
            break
        i += 1
    return aggs
