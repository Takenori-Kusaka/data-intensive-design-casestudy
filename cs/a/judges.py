"""Judge for topic A. A pure function over

- the expected events (from the generator),
- the client log (which requests were answered with success),
- the final state of the system of record (orders, order_history),
- the broker log read from the beginning,
- the final state of the derived database (deliveries, effects, search index).

It never looks at log messages or wall-clock time. Audited by audit.py with
planted errors.

Vocabulary
  committed  the change is in the system of record (order_history)
  acked      the caller got a success response
  delivered  the message was handed to the consumer (one row per hand-over)
  effect     the consumer changed its state because of the event
             (index: one row in index_applies; notify: one notification row)
"""
from collections import Counter, defaultdict

ROLES = ('index', 'notify')


def _decreases(seq):
    return sum(1 for a, b in zip(seq, seq[1:]) if b < a)


def _first_occurrences(seq):
    seen, out = set(), []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def judge(expected, client, history, orders, log, deliveries, effects, index):
    """
    expected    {event_id: (order_id, status, writer, seq)}                 generator
    client      [{'e': event_id, 'ack': 0|1, 'db': 0|1, ...}]                client log
    history     [(event_id, order_id, version)]                             system of record
    orders      {order_id: (status, last_event, version)}                   system of record
    log         [(partition, offset, key, event_id)] in partition/offset order   broker
    deliveries  {role: [(id, event_id, partition, offset)]} in id order      derived DB
    effects     {role: [(id, event_id, order_id)]} in id order               derived DB
    index       {order_id: (status, last_event)}                             derived DB
    """
    exp = set(expected)
    attempted = {r['e'] for r in client}
    acked = {r['e'] for r in client if r['ack']}
    committed = {h[0] for h in history}
    version = {h[0]: h[2] for h in history}
    order_of = {h[0]: h[1] for h in history}
    out = {
        'expected': len(exp), 'attempted': len(attempted), 'acked': len(acked), 'committed': len(committed),
        'request_failures': len(attempted) - len(acked),
        'not_attempted': len(exp - attempted),
        'committed_unacked': len(committed - acked),
        'acked_uncommitted': len(acked - committed),           # must be 0
    }
    unknown = (attempted | committed) - exp

    # ---- broker log
    log_ids = [x[3] for x in log]
    cnt = Counter(log_ids)
    in_log = set(cnt)
    unknown |= in_log - exp
    per_op = defaultdict(list)                     # (order, partition) -> versions in offset order
    parts_of_order = defaultdict(set)
    for part, _off, _key, eid in log:
        if eid in version:
            per_op[(order_of[eid], part)].append(version[eid])
            parts_of_order[order_of[eid]].add(part)
    out['log'] = {
        'messages': len(log_ids),
        'missing_committed': len(committed - in_log),
        'missing_acked': len(acked - in_log),
        'ghost': len(in_log - committed),          # in the log, not in the system of record
        'dup_events': sum(1 for c in cnt.values() if c > 1),
        'dup_extra': sum(c - 1 for c in cnt.values() if c > 1),
        'reorder_pairs': sum(_decreases(_first_occurrences(v)) for v in per_op.values()),
        'orders_multi_partition': sum(1 for ps in parts_of_order.values() if len(ps) > 1),
    }

    # ---- consumers
    for role in ROLES:
        dl = Counter(d[1] for d in deliveries.get(role, []))
        ef = Counter(e[1] for e in effects.get(role, []))
        delivered, effected = set(dl), set(ef)
        unknown |= (delivered | effected) - exp
        r = {
            'deliveries': sum(dl.values()),
            'effects': sum(ef.values()),
            'missing_acked': len(acked - effected),            # answered with success, no effect downstream
            'missing_committed': len(committed - effected),    # in the system of record, no effect downstream
            'ghost': len(effected - committed),
            'dup_delivered_events': sum(1 for c in dl.values() if c > 1),
            'dup_delivered_extra': sum(c - 1 for c in dl.values() if c > 1),
            'dup_effect_events': sum(1 for c in ef.values() if c > 1),
            'dup_effect_extra': sum(c - 1 for c in ef.values() if c > 1),
        }
        if role == 'index':
            per_order = defaultdict(list)
            for _id, eid, _o in effects.get(role, []):
                if eid in version:
                    per_order[order_of[eid]].append(version[eid])
            firsts = {o: _decreases(_first_occurrences(v)) for o, v in per_order.items()}
            raw = {o: _decreases(v) for o, v in per_order.items()}
            r['reorder_pairs'] = sum(firsts.values())          # true reordering (first occurrences only)
            r['reorder_orders'] = sum(1 for v in firsts.values() if v)
            r['regress_pairs'] = sum(raw.values())             # includes replays of old events
            r['regress_orders'] = sum(1 for v in raw.values() if v)
        out[role] = r

    # ---- final state: system of record vs search index
    touched = {o: v for o, v in orders.items() if v[2] > 0}
    mismatch = stale_status = absent = 0
    for o, (status, last_event, _ver) in touched.items():
        got = index.get(o)
        if got is None:
            absent += 1
            mismatch += 1
            stale_status += 1
        elif got[1] != last_event:
            mismatch += 1
            stale_status += 1 if got[0] != status else 0
    out['final'] = {
        'orders_touched': len(touched),
        'mismatch': mismatch,                       # last applied event differs (or the order is absent)
        'status_mismatch': stale_status,            # ... and the visible status differs
        'absent': absent,
        'extra_in_index': len(set(index) - set(touched)),
    }
    out['unknown_events'] = len(unknown)            # must be 0
    return out


def reconcile_diff(orders, index):
    """E-A3: compare the system of record with the search index. Returns the
    list of order ids whose status or last event differs (or that are absent)."""
    bad = []
    for o, (status, last_event, ver) in orders.items():
        if ver == 0:
            continue
        got = index.get(o)
        if got is None or got[0] != status or got[1] != last_event:
            bad.append(o)
    return sorted(bad)
