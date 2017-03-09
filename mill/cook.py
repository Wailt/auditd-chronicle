allowed_parent = ['exe', 'auid', 'egid', 'gid', 'syscall', ]
allowed_child = ['mode', 'nametype', 'ogid', ]  # 'name']


def prepare_log(log, annotation=None):
    ann = annotation + "_" if annotation else ''
    par, chl = log
    parent = {ann + k: par[k] for k in allowed_parent}
    child = dict()
    for field in allowed_child:
        child.setdefault(ann + field, [])
        for ch in chl:
            if field in ch['_source']:
                child[ann + field].append(ch['_source'][field])
        child[ann + field] = sorted(child[ann + field])
    log = parent
    log.update(child)
    return log