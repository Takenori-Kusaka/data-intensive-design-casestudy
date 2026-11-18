"""Audit of the judge: feed it data with planted errors and clean data, and
check that it counts exactly what was planted. Results go to
results/a/audit.jsonl; run.py refuses to continue when a case fails."""
import copy

from ..common.record import Results
from . import judges as J


def clean(n_orders=5, per_order=4):
    """A consistent world: every order gets ``per_order`` updates; everything is
    acked, in the log once (one partition per order), delivered once, applied once."""
    expected, client, history, log = {}, [], [], []
    orders, index = {}, {}
    deliveries = {r: [] for r in J.ROLES}
    effects = {r: [] for r in J.ROLES}
    offs = {}
    seq = 0
    for v in range(1, per_order + 1):
        for o in range(1, n_orders + 1):
            seq += 1
            eid = f'w00-{seq}'
            st = f's{v}'
            expected[eid] = (o, st, 'w00', seq)
            client.append({'e': eid, 'ack': 1, 'db': 1})
            history.append((eid, o, v))
            part = o % 2
            off = offs.get(part, 0)
            offs[part] = off + 1
            log.append((part, off, str(o), eid))
            for r in J.ROLES:
                deliveries[r].append((seq, eid, part, off))
                effects[r].append((seq, eid, o))
            orders[o] = (st, eid, v)
            index[o] = (st, eid)
    return {'expected': expected, 'client': client, 'history': history, 'orders': orders, 'log': sorted(log),
            'deliveries': deliveries, 'effects': effects, 'index': index}


def _flat(m):
    out = {}
    for k, v in m.items():
        if isinstance(v, dict):
            for k2, v2 in v.items():
                out[f'{k}.{k2}'] = v2
        else:
            out[k] = v
    return out


def _drop_event(w, eid, where):
    if 'log' in where:
        w['log'] = [x for x in w['log'] if x[3] != eid]
    for r in J.ROLES:
        if r in where:
            w['deliveries'][r] = [x for x in w['deliveries'][r] if x[1] != eid]
            w['effects'][r] = [x for x in w['effects'][r] if x[1] != eid]


def cases():
    """[(name, world, {metric: expected value})]. Metrics not listed must be 0
    (except the size counters)."""
    out = []
    out.append(('clean', clean(), {}))

    w = clean()                                  # 3 acked events never published
    for eid in ('w00-3', 'w00-8', 'w00-12'):
        _drop_event(w, eid, ('log', 'index', 'notify'))
    out.append(('missing_3_everywhere', w, {
        'log.missing_committed': 3, 'log.missing_acked': 3, 'index.missing_committed': 3, 'index.missing_acked': 3,
        'notify.missing_committed': 3, 'notify.missing_acked': 3, 'log.messages': 17}))

    w = clean()                                  # the last update of 2 orders is lost: the index stays stale
    for eid in ('w00-16', 'w00-17'):
        _drop_event(w, eid, ('log', 'index', 'notify'))
    w['index'][1] = ('s3', 'w00-11')
    w['index'][2] = ('s3', 'w00-12')
    out.append(('missing_last_update_2_orders', w, {
        'log.missing_committed': 2, 'log.missing_acked': 2, 'index.missing_committed': 2, 'index.missing_acked': 2,
        'notify.missing_committed': 2, 'notify.missing_acked': 2, 'final.mismatch': 2, 'final.status_mismatch': 2,
        'log.messages': 18}))

    w = clean()                                  # committed but the caller got no answer, and never published
    w['client'][6]['ack'] = 0
    _drop_event(w, 'w00-7', ('log', 'index', 'notify'))
    out.append(('committed_unacked_and_lost', w, {
        'request_failures': 1, 'committed_unacked': 1, 'log.missing_committed': 1, 'index.missing_committed': 1,
        'notify.missing_committed': 1, 'log.messages': 19}))

    w = clean()                                  # delivered only to the index consumer
    _drop_event(w, 'w00-5', ('notify',))
    out.append(('missing_1_notify_only', w, {'notify.missing_committed': 1, 'notify.missing_acked': 1}))

    w = clean()                                  # redelivery, deduplicated by the consumer
    w['deliveries']['index'] += [(101, 'w00-4', 0, 1), (102, 'w00-4', 0, 1), (103, 'w00-9', 0, 4)]
    w['deliveries']['notify'] += [(101, 'w00-4', 0, 1)]
    out.append(('dup_delivered_no_effect', w, {
        'index.dup_delivered_events': 2, 'index.dup_delivered_extra': 3,
        'notify.dup_delivered_events': 1, 'notify.dup_delivered_extra': 1}))

    w = clean()                                  # redelivery that hit the state twice (old event applied last)
    w['deliveries']['index'] += [(101, 'w00-4', 0, 1)]
    w['effects']['index'] += [(101, 'w00-4', 4)]
    w['deliveries']['notify'] += [(101, 'w00-4', 0, 1), (102, 'w00-6', 1, 2)]
    w['effects']['notify'] += [(101, 'w00-4', 4), (102, 'w00-6', 1)]
    w['index'][4] = ('s1', 'w00-4')
    out.append(('dup_effect_replay_last', w, {
        'index.dup_delivered_events': 1, 'index.dup_delivered_extra': 1, 'index.dup_effect_events': 1,
        'index.dup_effect_extra': 1, 'index.regress_pairs': 1, 'index.regress_orders': 1,
        'notify.dup_delivered_events': 2, 'notify.dup_delivered_extra': 2, 'notify.dup_effect_events': 2,
        'notify.dup_effect_extra': 2, 'final.mismatch': 1, 'final.status_mismatch': 1}))

    w = clean()                                  # the same event twice in the broker log
    w['log'] = sorted(w['log'] + [(0, 50, '2', 'w00-2'), (0, 51, '2', 'w00-2'), (1, 50, '3', 'w00-13')])
    out.append(('dup_in_log', w, {'log.dup_events': 2, 'log.dup_extra': 3, 'log.messages': 23}))

    w = clean()                                  # two updates of order 3 swapped in the log and at the consumer
    a, b = 'w00-8', 'w00-13'                     # versions 2 and 3 of order 3
    w['log'] = sorted((p, o, k, (b if e == a else a if e == b else e)) for p, o, k, e in w['log'])
    w['effects']['index'] = [(i, (b if e == a else a if e == b else e), o) for i, e, o in w['effects']['index']]
    out.append(('reorder_1_pair_mid', w, {
        'log.reorder_pairs': 1, 'index.reorder_pairs': 1, 'index.reorder_orders': 1, 'index.regress_pairs': 1,
        'index.regress_orders': 1}))

    w = clean()                                  # the last two updates of order 5 swapped: the index ends stale
    a, b = 'w00-15', 'w00-20'
    w['log'] = sorted((p, o, k, (b if e == a else a if e == b else e)) for p, o, k, e in w['log'])
    w['effects']['index'] = [(i, (b if e == a else a if e == b else e), o) for i, e, o in w['effects']['index']]
    w['index'][5] = ('s3', 'w00-15')
    out.append(('reorder_last_pair_final_mismatch', w, {
        'log.reorder_pairs': 1, 'index.reorder_pairs': 1, 'index.reorder_orders': 1, 'index.regress_pairs': 1,
        'index.regress_orders': 1, 'final.mismatch': 1, 'final.status_mismatch': 1}))

    w = clean()                                  # same last event id is not required for a status match
    w['index'][2] = ('s4', 'w00-12')
    out.append(('final_mismatch_same_status', w, {'final.mismatch': 1}))

    w = clean()                                  # event in the log and applied, but not in the system of record
    w['expected']['w00-99'] = (1, 's9', 'w00', 99)
    w['client'].append({'e': 'w00-99', 'ack': 0, 'db': 0})
    w['log'] = sorted(w['log'] + [(1, 60, '1', 'w00-99')])
    for r in J.ROLES:
        w['deliveries'][r].append((200, 'w00-99', 1, 60))
        w['effects'][r].append((200, 'w00-99', 1))
    w['index'][1] = ('s9', 'w00-99')
    out.append(('ghost_event', w, {
        'expected': 21, 'attempted': 21, 'request_failures': 1, 'log.ghost': 1, 'log.messages': 21,
        'index.ghost': 1, 'notify.ghost': 1, 'final.mismatch': 1, 'final.status_mismatch': 1}))

    w = clean()                                  # an event id the generator never produced
    w['log'] = sorted(w['log'] + [(0, 70, '1', 'zz-1')])
    out.append(('unknown_event', w, {'unknown_events': 1, 'log.ghost': 1, 'log.messages': 21}))

    w = clean()                                  # an order absent from the index
    _drop_event(w, 'w00-1', ('index',))
    for eid in ('w00-6', 'w00-11', 'w00-16'):
        _drop_event(w, eid, ('index',))
    del w['index'][1]
    out.append(('order_absent_in_index', w, {
        'index.missing_committed': 4, 'index.missing_acked': 4, 'final.mismatch': 1, 'final.status_mismatch': 1,
        'final.absent': 1}))

    w = clean()                                  # events of one order spread over two partitions
    w['log'] = sorted((1 - p if e == 'w00-11' else p, 90 if e == 'w00-11' else o, k, e) for p, o, k, e in w['log'])
    out.append(('order_on_two_partitions', w, {'log.orders_multi_partition': 1}))

    w = clean()                                  # a request that was never attempted (caller stopped early)
    w['expected']['w00-21'] = (1, 's5', 'w00', 21)
    out.append(('not_attempted', w, {'expected': 21, 'not_attempted': 1}))
    return out


BASE = {'expected': 20, 'attempted': 20, 'acked': 20, 'committed': 20, 'log.messages': 20,
        'index.deliveries': None, 'index.effects': None, 'notify.deliveries': None, 'notify.effects': None,
        'final.orders_touched': 5}


def run(log=print):
    res = Results('a/audit.jsonl')
    failed = []
    for name, w, want in cases():
        got = _flat(J.judge(**copy.deepcopy(w)))
        diffs = {}
        for k, v in got.items():
            if k in want:
                exp = want[k]
            elif k in BASE:
                exp = BASE[k]
                if k == 'acked' and 'request_failures' in want:
                    exp = got['attempted'] - want['request_failures']
                if k == 'committed' and name == 'ghost_event':
                    exp = 20
            else:
                exp = 0
            if exp is not None and v != exp:
                diffs[k] = {'got': v, 'want': exp}
        ok = not diffs
        if not ok:
            failed.append(name)
        row = {'run_id': f'audit:judge:{name}', 'judge': 'judge', 'case': name, 'planted': want, 'ok': ok,
               'diffs': diffs}
        if not res.has(row['run_id']):
            res.append(row)
    # reconcile_diff
    for name, orders, index, want in [
        ('reconcile_clean', {1: ('a', 'e1', 1), 2: ('b', 'e2', 3), 3: ('new', None, 0)}, {1: ('a', 'e1'), 2: ('b', 'e2')}, []),
        ('reconcile_stale_and_absent', {1: ('a', 'e1', 1), 2: ('b', 'e2', 3), 3: ('c', 'e3', 1)},
         {1: ('z', 'e0'), 2: ('b', 'e2')}, [1, 3]),
        ('reconcile_same_status_other_event', {1: ('a', 'e5', 2)}, {1: ('a', 'e1')}, [1]),
    ]:
        got = J.reconcile_diff(orders, index)
        ok = got == want
        if not ok:
            failed.append(name)
        row = {'run_id': f'audit:reconcile:{name}', 'judge': 'reconcile_diff', 'case': name, 'planted': want,
               'ok': ok, 'diffs': {} if ok else {'got': got}}
        if not res.has(row['run_id']):
            res.append(row)
    log(f'audit: {len(cases()) + 3} cases, failed: {failed}')
    if failed:
        raise SystemExit(f'judge audit failed: {failed}')


if __name__ == '__main__':
    run()
