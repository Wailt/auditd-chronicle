users = ['exe', 'ses', 'timestamp', 'event_id', 'pid', 'res', 'type', 'op', 'hostname', 'name', 'date']

syscall = ['fsuid', 'ses', 'uid', 'syscall', 'pid', 'sgid',
           'auid', 'suid', 'event_id', 'gid', 'exit', 'date',
           'ppid', 'type', 'timestamp', 'comm', 'euid', 'exe',
           'success', 'items', 'fsgid', 'egid', 'hostname', 'name']

path = ['timestamp', 'nametype', 'name', 'event_id', 'item', 'mode', 'type', 'hostname', 'ses', 'date']

fields = {
    'users': users,
    'syscall': syscall,
    'path': path
}

event_types = {
    'USER_END': users,
    'USER_LOGOUT': users,
    'USER_LOGIN': users,
    'USER_START': users,
    'USER_AUTH': users,
    'USER_ACCT': users,
    'CRED_ACQ': users,
    'CRED_DISP': users,
    'CRED_REFR': users,
    'SYSCALL': syscall,
    'PATH': path,

    # my types
    'DAEMON_END': users,
    'PROCTITLE': users,
    'USER_CMD': users,
    'CWD': users,
    'EXECVE': users,
    'DAEMON_START': users,
}

#access_list = [str(i) for i in range(20)]#['exe', 'op', 'type']
access_list = ['exe', 'auid', 'egid', 'gid', 'syscall', ] + ['mode', 'nametype', 'ogid']
def filter_fields(event):
    if event['type'] in event_types:
        return {key: value for key, value in event.iteritems() if key in event_types[event["type"]]}
    else:
        return event


def delete_some_field(event):
    for key in event.keys():
        if key not in access_list:
            pass
            #del event[key]


def get_features(event):
    features = []
    for key, val in event.iteritems():
        features.append(str(key) + "$" + str(val))
    return features


def filter_event(event):
    delete_some_field(event)
    filtered_event = get_features(event)
    return filtered_event
