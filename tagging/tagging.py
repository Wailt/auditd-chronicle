from cook import prepare_log
from es.elastic_query import *

previous_separate_policy = False

def sub_actions(aggs, tree, h=1, adres_slice=3):
    count = 0
    separator = 0

    seq = dict()
    for key in aggs.keys():
        seq[key] = []
        addres_prev = []
        if len(aggs[key]) > h:
            for j in range(h):
                count += 1
                i = aggs[key][j]
                addres = tree.update(prepare_log(i))
                action = {'_op_type': 'update', u'_id': i[0].meta.id, u'_type': u'auditd-parent', u'_index': u'wailt'}
                action['doc'] = {"htree": str(addres), 'order': count, 'separator': separator}
                act.append(action)

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