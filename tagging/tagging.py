from cook import prepare_log
from es.elastic_query import *
from collections import Counter

from ngrams import Ngrams

previous_separate_policy = False

def sub_actions(aggs, tree, h=1, adres_slice=3):
    count = 0
    separator = 0
    seq = dict()

    act = []

    for key in aggs.keys():
        seq[key] = []
        addres_prev = []
        if len(aggs[key]) > h:
            # for j in range(h):
            #     count += 1
            #     i = aggs[key][j]
            #     addres = tree.update(prepare_log(i))
            #     action = {'_op_type': 'update', u'_id': i[0].meta.id, u'_type': u'auditd-parent', u'_index': u'wailt'}
            #     action['doc'] = {"htree": str(addres), 'order': count, 'separator': separator}
            #     act.append(action)

            for j in range(h, len(aggs[key])):
                i = aggs[key][j]
                count += 1
                log_current = prepare_log(i)

                for k in range(1, h + 1):
                    if previous_separate_policy:
                        log_prev = prepare_log(aggs[key][j - k])
                        log_current.update({'log_' + str(k): str(log_prev)})
                    else:
                        log_prev = prepare_log(aggs[key][j - k], annotation='prev' + str(k))
                        log_current.update(log_prev)

                addres = tree.update(log_current)

                if addres_prev[:adres_slice] != addres[:adres_slice]:
                    separator += 1
                seq[key].append((tuple(addres), separator, i[0].meta.id))

                action = {'_op_type': 'update', u'_id': i[0].meta.id, u'_type': u'auditd-parent', u'_index': u'wailt'}
                action['doc'] = {"htree": str(addres), 'order': count, 'separator': separator}
                act.append(action)

                addres_prev = addres

                if len(act) > 10000:
                    print 'acted'
                    bulk(es, actions=act)
                    act = []

    bulk(es, actions=act)
    print 'acted'

    return seq

def actions(sessions, tag_list, ng):
    first = tag_list
    res = dict()
    for ses in sessions:
        s = sessions[ses]
        result = [(-1,) for i in range(len(s))]
        count = 0
        tag_list = first
        while len(tag_list) > 0:
            tag = tag_list[0][0]
            n = len(tag)
            val = tag_list[0][1]
            tag_list = tag_list[1:]
            for i in range(len(s) - n + 1):
                flag = min([(1 if result[i + j] == (-1,) else 0) for j in range(n) if i + j < len(s)])
                if tuple(s[i:i + n]) == tag and flag:
                    for k in range(n):
                        result[i + k] = (ng.ngram_iterator.data[tag], count)
                    count += 1
        res[ses] = result
    return res

def post_actions(seq, h=1, adres_slice=3):
    sessions = dict()
    nums = dict()

    for key in seq:
        sessions[key] = [(i[0][:adres_slice], i[1]) for i in sorted(seq[key], key=lambda x: x[1])]
        nums[key] = [i[1] for i in sorted(Counter(sessions[key]).items(), key=lambda x: x[0][1])]
        sessions[key] = [i[0] for i in sorted(set(sessions[key]), key=lambda x: x[1])]

    # ngrams
    print str(h) + 'grams'
    ng = Ngrams(sessions=sessions, n=h)

    # price-list
    vals = list(map(lambda x: (x[0], x[1] * 10 ** (len(x[0])) - 1), ng.ngrams.items()))
    tag_list = sorted(list(set(vals)), key=lambda x: x[1], reverse=True)

    # tagging
    res = actions(sessions, tag_list, ng)

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