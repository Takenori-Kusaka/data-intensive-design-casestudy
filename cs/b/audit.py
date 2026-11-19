"""Audit of the judges of topic B: each judge gets data with planted errors and clean
data, and must count exactly what was planted. Results go to results/b/audit.jsonl;
run.py refuses to continue when a case fails.

A case is (judge, case name, function returning the judge's output, expected values).
Only the listed keys are compared; for dict outputs every other numeric key must be 0
unless it is listed in ``free``.
"""
from ..common.record import Results
from . import b3_ddl as B3, b4_swap as B4, b5_rebuild as B5, b6_sor as B6, b7_mv as B7, derive as V, schemas as S
from .load import Op, max_gap, window

F = S.F


def _reads():
    w = [F('a', 'int', 'a', 1), F('s', 'string', 's', 2), F('opt', 'string', 'opt', 3, default='', required=False)]
    truth = {'a': 7, 's': 'x', 'opt': 'o'}
    renamed = [F('a', 'int', 'a', 1), F('t', 'string', 's', 2)]
    r_extra_req = w + [F('need', 'string', 'need', 4)]
    r_extra_opt = w + [F('more', 'string', 'more', 4, default='', required=False)]
    reused = [F('a', 'int', 'a', 1), F('s', 'string', 's', 2), F('mail', 'string', 'mail', 3, default='', required=False)]
    j = S.judge_read
    return [
        ('clean', lambda: j('protobuf', w, w, truth, {'a': 7, 's': 'x', 'opt': 'o'}), 'ok'),
        ('error', lambda: j('avro', w, w, truth, ValueError('cannot read')), 'error'),
        ('wrong_value', lambda: j('protobuf', w, w, truth, {'a': 8, 's': 'x', 'opt': 'o'}), 'wrong'),
        ('wrong_type', lambda: j('json', w, w, truth, {'a': '7', 's': 'x', 'opt': 'o'}), 'wrong'),
        ('truncated', lambda: j('protobuf', w, w, {'a': 2**31 + 5, 's': 'x', 'opt': 'o'}, {'a': 5, 's': 'x', 'opt': 'o'}), 'wrong'),
        ('renamed_same_meaning', lambda: j('protobuf', renamed, w, truth, {'a': 7, 't': 'x'}), 'ok'),
        ('renamed_lost', lambda: j('protobuf', renamed, w, truth, {'a': 7, 't': ''}), 'wrong'),
        ('optional_default_filled', lambda: j('protobuf', r_extra_opt, w, truth, {'a': 7, 's': 'x', 'opt': 'o', 'more': ''}), 'ok'),
        ('required_default_filled', lambda: j('protobuf', r_extra_req, w, truth, {'a': 7, 's': 'x', 'opt': 'o', 'need': ''}), 'missing'),
        ('ghost_value', lambda: j('protobuf', reused, w, truth, {'a': 7, 's': 'x', 'mail': 'o'}), 'wrong'),
        ('json_optional_absent', lambda: j('json', r_extra_opt, w, truth, {'a': 7, 's': 'x', 'opt': 'o', 'more': None}), 'ok'),
        ('tally', lambda: S.tally(['ok', 'ok', 'error', 'wrong', 'missing', 'ok']),
         {'n': 6, 'ok': 3, 'error': 1, 'wrong': 1, 'missing': 1}),
    ]


def _codec():
    """The real encoders and decoders with the judge: a clean round trip, and a reader whose
    schema differs in a way that must be noticed."""
    out = []
    for fmt in S.FORMATS:
        v1, v2, closed = S.change_specs(fmt, 'add_optional')
        truth = S.gen_record(1, 3, v1, fmt)

        def rt(fmt=fmt, r=v1, w=v1, truth=truth, closed=closed):
            try:
                dec = S.decode(fmt, r, w, S.encode(fmt, w, truth), closed)
            except Exception as e:  # noqa: BLE001
                dec = e
            return S.judge_read(fmt, r, w, truth, dec)
        out.append((f'{fmt}_round_trip', rt, 'ok'))
        b, n, c2 = S.change_specs(fmt, 'retype')
        t2 = S.gen_record(1, 4, n, fmt)
        out.append((f'{fmt}_retype_old_reader',
                    lambda fmt=fmt, b=b, n=n, t2=t2, c2=c2: rt(fmt, b, n, t2, c2), 'wrong' if fmt == 'protobuf' else 'error'))
    return out


def _gaps():
    ops = [Op('w', 0, t - 0.01, t, True, None, 'p', False) for t in (1.0, 1.1, 1.2, 3.2, 3.3)]
    ops += [Op('w', 1, 1.3, 2.9, False, 'QueryCanceled:57014', 'p', False), Op('w', 1, 3.0, 3.25, True, None, 'p', False),
            Op('r', 2, 1.0, 1.05, True, None, 'p', True)]
    return [
        ('planted_gap', lambda: round(max_gap([1.0, 1.1, 1.2, 3.2, 3.3], 1.0, 3.3), 3), 2.0),
        ('no_gap', lambda: round(max_gap([1.0, 1.1, 1.2, 1.3], 1.0, 1.3), 3), 0.1),
        ('gap_at_end', lambda: round(max_gap([1.0, 1.1], 1.0, 4.0), 3), 2.9),
        ('gap_at_start', lambda: round(max_gap([3.0], 1.0, 3.2), 3), 2.0),
        ('no_completion', lambda: round(max_gap([], 1.0, 6.0), 3), 5.0),
        ('window_counts', lambda: {k: window(ops, 'w', 1.0, 3.3)[k] for k in ('ok', 'failed', 'max_gap_ms', 'slow', 'wrong')},
         {'ok': 6, 'failed': 1, 'max_gap_ms': 2000.0, 'slow': 1, 'wrong': 0}),
        ('window_wrong_read', lambda: {k: window(ops, 'r', 1.0, 3.3)[k] for k in ('ok', 'failed', 'wrong')},
         {'ok': 1, 'failed': 0, 'wrong': 1}),
    ]


def _migration():
    def world():
        rows = {i: (2, *B3.addr(i, 2)) for i in range(1, 11)}
        return rows, {i: 2 for i in rows}, {i: 2 for i in rows}
    j = B3.judge_migration
    out = [('clean', lambda: j(*world(), 0, 0), {'rows': 10})]

    def stale_value():
        rows, ack, att = world()
        rows[3] = (2, *B3.addr(3, 1))
        rows[4] = (2, None, None)
        return j(rows, ack, att, 1, 0)
    out.append(('value_mismatch_2', stale_value, {'rows': 10, 'value_mismatch': 2, 'null_new_columns': 1}))

    def lost():
        rows, ack, att = world()
        rows[5] = (1, *B3.addr(5, 1))
        return j(rows, ack, att, 0, 0)
    out.append(('lost_acked_write_1', lost, {'rows': 10, 'lost_acked_write': 1}))

    def unanswered():
        rows, ack, att = world()
        att[6] = 3
        rows[6] = (3, *B3.addr(6, 3))       # the unanswered write did reach the table: allowed
        rows[7] = (5, *B3.addr(7, 5))       # a version nobody attempted
        return j(rows, ack, att, 0, 4)
    out.append(('unanswered_ok_and_unexpected_1', unanswered, {'rows': 10, 'unexpected_version': 1, 'untouched_rows_mismatch': 4}))
    return out


def _store():
    ev = V.gen_events(5, 200, 10)
    orders, counts = V.truth_of(ev)

    def rows():
        return {o: (st, ver, ver) for o, (st, ver) in orders.items()}
    j = V.judge_store
    out = [('clean', lambda: j(orders, counts, rows(), dict(counts)), {})]

    def missing_extra():
        r = rows()
        del r[1], r[2]
        r[99] = ('accepted', 1, 1)
        return j(orders, counts, r, dict(counts))
    out.append(('missing_2_extra_1', missing_extra, {'missing': 2, 'extra': 1, 'orders_in_store': 9}))

    def stale():
        r = rows()
        for o in (3, 4, 5):
            st, ver, n = r[o]
            r[o] = (st, ver - 1, n - 1)
        return j(orders, counts, r, dict(counts))
    out.append(('stale_3', stale, {'state_mismatch': 3, 'count_mismatch': 3, 'count_deficit': 3}))

    def dup():
        r, c = rows(), dict(counts)
        r[6] = (r[6][0], r[6][1], r[6][2] + 2)
        r[7] = (r[7][0], r[7][1], r[7][2] + 1)
        c['accepted'] += 3
        return j(orders, counts, r, c)
    out.append(('duplicates_3_on_2_orders', dup, {'count_mismatch': 2, 'count_surplus': 3, 'agg_mismatch': 1, 'agg_surplus': 3}))

    def agg_short():
        c = dict(counts)
        c['cancelled'] -= 4
        del c['cooking']
        return j(orders, counts, rows(), c)
    out.append(('aggregate_short', agg_short, {'agg_mismatch': 2, 'agg_deficit': 4 + counts['cooking']}))
    out.append(('no_aggregate', lambda: j(orders, counts, rows(), None), {'agg_mismatch': None, 'agg_surplus': None, 'agg_deficit': None}))

    def deleted_left():
        o2, _ = V.truth_of(ev, deleted=[1, 2, 3])
        return j(o2, counts, rows(), None)
    out.append(('deleted_orders_left_3', deleted_left, {'orders_expected': 7, 'extra': 3, 'agg_mismatch': None, 'agg_surplus': None, 'agg_deficit': None}))
    return out


def _recon():
    def snap(old, new, agg=None):
        rows = {o: (old.get(o), new.get(o)) for o in set(old) | set(new)}
        return {'rows': rows, 'agg_total': sum(n[2] for n in new.values()) if agg is None else agg}
    base = {o: ('accepted', 3, 3) for o in range(1, 6)}
    j = B4.recon_judge

    def in_flight():
        new1 = dict(base)
        new1[2] = ('cooking', 2, 2)           # behind in the first snapshot, caught up in the second
        old2, new2 = dict(base), dict(base)
        old2[4] = ('completed', 4, 4)         # a new event that only old has applied so far
        return j(snap(base, new1), snap(old2, new2))
    out = [('clean', lambda: j(snap(base, base), snap(base, base)), {'passed': True, 'persistent': 0, 'in_flight': 0, 'exact_equal_snapshots': 2}),
           ('in_flight_only', in_flight, {'passed': True, 'persistent': 0, 'in_flight': 1, 'exact_equal_snapshots': 0})]

    def dropped():
        new = dict(base)
        new[1] = ('cooking', 2, 2)
        del new[5]
        return j(snap(base, new), snap(base, new))
    out.append(('persistent_stale_1_absent_1', dropped, {'passed': False, 'persistent': 2, 'exact_equal_snapshots': 0}))

    def double_applied():
        new = dict(base)
        new[3] = ('accepted', 3, 4)           # same version, one event applied twice
        return j(snap(base, new), snap(base, new))
    out.append(('same_version_other_count', double_applied, {'passed': False, 'persistent': 1, 'exact_equal_snapshots': 0}))
    out.append(('aggregate_inconsistent', lambda: j(snap(base, base), snap(base, base, agg=14)),
                {'passed': False, 'persistent': 0, 'agg_consistent': False, 'exact_equal_snapshots': 2}))
    return out


def _sor():
    def world():
        acked = {f'k{i}': 2 for i in range(6)}
        rows = {k: (B6.status_of(k, v), v) for k, v in acked.items()}
        return rows, acked, dict(acked)
    j = B6.judge_sor
    out = [('clean', lambda: j(*world()), {'acked_keys': 6})]

    def broken():
        rows, acked, att = world()
        del rows['k0']
        rows['k1'] = (B6.status_of('k1', 1), 1)
        rows['k2'] = (B6.status_of('k2', 1), 2)
        att['k3'] = 3
        rows['k3'] = (B6.status_of('k3', 3), 3)
        rows['k4'] = (B6.status_of('k4', 9), 9)
        return j(rows, acked, att)
    out.append(('missing_1_lost_1_value_1_unexpected_1', broken,
                {'acked_keys': 6, 'missing': 1, 'lost_acked_write': 1, 'value_mismatch': 1, 'unexpected_version': 1}))
    src = {'a': ('s', 3), 'b': ('s', 1), 'c': ('s', 2)}
    out.append(('stale_in', lambda: B6.stale_in({'a': ('s', 2), 'c': ('s', 2)}, src), {'a': 2, 'b': None}))
    out.append(('persistent_diff', lambda: B6.persistent_diff({'a': 2, 'b': None, 'c': 1}, {'a': 2, 'c': 2, 'd': None}), ['a']))
    return out


SUITES = [('judge_read', _reads), ('codec', _codec), ('max_gap', _gaps), ('judge_migration', _migration),
          ('judge_store', _store), ('recon_judge', _recon), ('judge_sor', _sor),
          ('b5', B5.audit_cases), ('b7', B7.audit_cases)]


def _check(got, want):
    if not isinstance(want, dict) or not isinstance(got, dict):
        return {} if got == want else {'got': got, 'want': want}
    diffs = {}
    for k, v in got.items():
        exp = want.get(k, v if isinstance(v, (bool, str, dict, list)) or k in ('rows', 'n', 'orders_expected', 'orders_in_store', 'acked_keys', 'in_flight', 'agg_consistent', 'seconds') else 0)
        if k in want:
            exp = want[k]
        if v != exp:
            diffs[k] = {'got': v, 'want': exp}
    for k in want:
        if k not in got:
            diffs[k] = {'got': None, 'want': want[k]}
    return diffs


def run(log=print):
    res = Results('b/audit.jsonl')
    failed, n = [], 0
    for judge, suite in SUITES:
        for name, fn, want in suite():
            n += 1
            try:
                got = fn()
                diffs = _check(got, want)
            except Exception as e:  # noqa: BLE001
                got, diffs = repr(e), {'exception': repr(e)}
            ok = not diffs
            if not ok:
                failed.append(f'{judge}:{name}')
            row = {'run_id': f'audit:{judge}:{name}', 'judge': judge, 'case': name, 'planted': want,
                   'got': got, 'ok': ok, 'diffs': diffs}
            if not res.has(row['run_id']):
                res.append(row)
    log(f'audit: {n} cases, failed: {failed}')
    if failed:
        raise SystemExit(f'judge audit failed: {failed}')


if __name__ == '__main__':
    run()
