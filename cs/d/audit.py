"""Audit of the judges: feed them data with planted errors and clean data, and
check that they count exactly what was planted. Results go to
results/d/audit.jsonl; run.py refuses to continue when a case fails."""
from ..common.record import Results
from . import judges as J
from .d1_hermitage import TEST_IDS, judge


def _o(reads=None, committed=None, final=None):
    return {'reads': reads or {}, 'committed': committed or {'T1': True, 'T2': True, 'T3': True},
            'final': final if final is not None else {1: 10, 2: 20}}


BOTH = {'T1': True, 'T2': True, 'T3': True}
T2_FAILED = {'T1': True, 'T2': False, 'T3': True}
T1_FAILED = {'T1': False, 'T2': True, 'T3': True}

# (test, planted?, observation)
D1_CASES = [
    ('G0', 'anomaly', _o(final={1: 11, 2: 22})), ('G0', 'anomaly', _o(final={1: 12, 2: 21})),
    ('G0', 'prevented', _o(final={1: 12, 2: 22})), ('G0', 'prevented', _o(final={1: 11, 2: 21}, committed=T2_FAILED)),
    ('G1a', 'anomaly', _o({'r1': {1: 101, 2: 20}, 'r2': {1: 10, 2: 20}})),
    ('G1a', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 10, 2: 20}})),
    ('G1a', 'inconclusive', _o({'r1': None, 'r2': None})),
    ('G1b', 'anomaly', _o({'r1': {1: 101, 2: 20}, 'r2': {1: 11, 2: 20}})),
    ('G1b', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 11, 2: 20}})),
    ('G1c', 'anomaly', _o({'r1': {2: 22}, 'r2': {1: 11}})),
    ('G1c', 'prevented', _o({'r1': {2: 20}, 'r2': {1: 10}})),
    ('G1c', 'prevented', _o({'r1': {2: 22}, 'r2': None}, committed=T2_FAILED)),
    ('OTV', 'anomaly', _o({'r1': {1: 12, 2: 19}, 'r2': {1: 12, 2: 18}, 'r3': {1: 12, 2: 18}})),
    ('OTV', 'anomaly', _o({'r1': {1: 12, 2: 18}, 'r2': {1: 11, 2: 19}, 'r3': {1: 12, 2: 18}})),
    ('OTV', 'prevented', _o({'r1': {1: 11, 2: 19}, 'r2': {1: 11, 2: 19}, 'r3': {1: 12, 2: 18}})),
    ('OTV', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 10, 2: 20}, 'r3': {1: 10, 2: 20}})),
    ('P2', 'anomaly', _o({'r1': {1: 10}, 'r2': {1: 12}})), ('P2', 'prevented', _o({'r1': {1: 10}, 'r2': {1: 10}})),
    ('P3', 'anomaly', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 10, 2: 20, 3: 30}})),
    ('P3', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 10, 2: 20}})),
    ('PMP', 'anomaly', _o({'r1': {}, 'r2': {3: 30}})), ('PMP', 'prevented', _o({'r1': {}, 'r2': {}})),
    ('PMPW', 'anomaly', _o(final={1: 20, 2: 30})), ('PMPW', 'prevented', _o(final={2: 30})),
    ('PMPW', 'prevented', _o(final={1: 20})), ('PMPW', 'prevented', _o(final={1: 20, 2: 30}, committed=T2_FAILED)),
    ('PMPW2', 'anomaly', _o({'r1': {2: 20}}, final={2: 30})), ('PMPW2', 'anomaly', _o({'r1': {1: 20, 2: 30}}, final={1: 20, 2: 30})),
    ('PMPW2', 'prevented', _o({'r1': {2: 30}}, final={2: 30})),
    ('PMPW2', 'prevented', _o({'r1': None}, final={1: 20, 2: 30}, committed=T2_FAILED)),
    ('P3W', 'anomaly', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 11, 2: 21, 3: 31}})),
    ('P3W', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 11, 2: 21}})),
    ('P4', 'anomaly', _o({'r1': {1: 10}, 'r2': {1: 10}}, final={1: 11, 2: 20})),
    ('P4', 'prevented', _o({'r1': {1: 10}, 'r2': {1: 10}}, committed=T2_FAILED, final={1: 11, 2: 20})),
    ('P4', 'prevented', _o({'r1': {1: 10}, 'r2': {1: 11}}, final={1: 11, 2: 20})),
    ('GS', 'anomaly', _o({'r1': {1: 10}, 'r2': {2: 18}})), ('GS', 'prevented', _o({'r1': {1: 10}, 'r2': {2: 20}})),
    ('GSD', 'anomaly', _o({'r1': {1: 10, 2: 20}, 'r2': {1: 12}})), ('GSD', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r2': {}})),
    ('GSW', 'anomaly', _o({'r1': {1: 10}}, final={1: 12, 2: 18})), ('GSW', 'anomaly', _o({'r1': {1: 10}}, final={1: 12, 2: 20})),
    ('GSW', 'prevented', _o({'r1': {1: 10}}, final={1: 12})),
    ('GSW', 'prevented', _o({'r1': {1: 10}}, final={1: 12, 2: 18}, committed=T1_FAILED)),
    ('MAVW', 'anomaly', _o({'r1': {1: 10}, 'r2': {1: 10, 2: 19}})),
    ('MAVW', 'prevented', _o({'r1': {1: 10}, 'r2': {1: 12, 2: 19}})),
    ('MAVW', 'prevented', _o({'r1': {1: 10}, 'r2': None}, committed=T1_FAILED)),
    ('G2I', 'anomaly', _o(final={1: 11, 2: 21})), ('G2I', 'prevented', _o(committed=T2_FAILED, final={1: 11, 2: 20})),
    ('G2', 'anomaly', _o(final={1: 10, 2: 20, 3: 30, 4: 42})),
    ('G2', 'prevented', _o(committed=T2_FAILED, final={1: 10, 2: 20, 3: 30})),
    ('G2E', 'anomaly', _o({'r1': {1: 10, 2: 20}, 'r3': {1: 10, 2: 25}})),
    ('G2E', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r3': {1: 10, 2: 25}}, committed=T1_FAILED)),
    ('G2E', 'prevented', _o({'r1': {1: 10, 2: 20}, 'r3': {1: 0, 2: 25}})),
]


def _cases():
    out = []
    for i, (t, expected, obs) in enumerate(D1_CASES):
        out.append((f'd1/{t}/{i}', 'd1_judge', {'test': t, 'obs': {'reads': obs['reads'], 'committed': obs['committed']}},
                    expected, judge(t, obs)))
    # inventory: 3 items of 10. item 1 clean, item 2 has 2 lost updates and is oversold by 1,
    # item 3 lost one acknowledged... (decrement without ack) and a false reject
    inv = J.inventory_judge({1: 10, 2: 10, 3: 10}, {1: 12, 2: 12, 3: 12}, {1: 10, 2: 11, 3: 8}, {1: 0, 2: 1, 3: 1})
    out.append(('inv/planted', 'inventory_judge', 'item2: served 11 of 10 with final 1; item3: served 8, final 1',
                {'lost_updates': 2, 'unacked_decrements': 1, 'oversold': 1, 'negative_items': 0,
                 'false_rejects': 2, 'served': 29, 'expected_served': 30}, inv))
    inv = J.inventory_judge({1: 10, 2: 10}, {1: 12, 2: 5}, {1: 10, 2: 5}, {1: 0, 2: 5})
    out.append(('inv/clean', 'inventory_judge', 'no error planted',
                {'lost_updates': 0, 'unacked_decrements': 0, 'oversold': 0, 'negative_items': 0,
                 'false_rejects': 0, 'served': 15, 'expected_served': 15}, inv))
    inv = J.inventory_judge({1: 5}, {1: 9}, {1: 7}, {1: -2})
    out.append(('inv/negative', 'inventory_judge', 'served 7 of 5, final -2',
                {'lost_updates': 0, 'unacked_decrements': 0, 'oversold': 2, 'negative_items': 1,
                 'false_rejects': 0, 'served': 7, 'expected_served': 5}, inv))
    # duty: area 1 clean, area 2 both on break (violation), area 3 nobody served, area 4 state/ack mismatch
    d = J.duty_judge({1: 1, 2: 0, 3: 2, 4: 2}, {1: 1, 2: 2, 4: 1})
    out.append(('duty/planted', 'duty_judge', 'area2 violation, area3 false reject, area4 mismatch',
                {'violations': 1, 'false_rejects': 1, 'served': 4, 'state_mismatch': 1}, d))
    d = J.duty_judge({1: 1, 2: 1}, {1: 1, 2: 1})
    out.append(('duty/clean', 'duty_judge', 'no error planted',
                {'violations': 0, 'false_rejects': 0, 'served': 2, 'state_mismatch': 0}, d))
    # replication loss: 10 acked, 3 removed from the survivor, 2 unacknowledged rows added
    acked = [(1, i) for i in range(1, 11)]
    present = [(1, i) for i in range(1, 11) if i not in (4, 9, 10)] + [(1, 11), (2, 1)]
    out.append(('repl/planted', 'repl_loss_judge', '3 acked rows removed, 2 unacked rows added',
                {'acked': 10, 'present': 9, 'lost_acked': 3, 'unacked_present': 2}, J.repl_loss_judge(acked, present)))
    out.append(('repl/clean', 'repl_loss_judge', 'no error planted',
                {'acked': 10, 'present': 10, 'lost_acked': 0, 'unacked_present': 0}, J.repl_loss_judge(acked, acked)))
    # duplicates in the input must not hide a loss
    out.append(('repl/dup', 'repl_loss_judge', 'acked list has a duplicate, one row lost',
                {'acked': 3, 'present': 2, 'lost_acked': 1, 'unacked_present': 0},
                J.repl_loss_judge([(1, 1), (1, 2), (1, 2), (1, 3)], [(1, 1), (1, 2)])))
    # split brain: ids 1-3 common, old has 4,5 from A, new has 4,5,6 from B -> 2 collisions
    old = [(1, 'A', 1), (2, 'A', 2), (3, 'A', 3), (4, 'A', 4), (5, 'A', 5)]
    new = [(1, 'A', 1), (2, 'A', 2), (3, 'A', 3), (4, 'B', 1), (5, 'B', 2), (6, 'B', 3)]
    out.append(('split/planted', 'split_judge', '2 rows only on old, 3 only on new, 2 ids reused',
                {'only_old': 2, 'only_new': 3, 'common': 3, 'id_collisions': 2}, J.split_judge(old, new)))
    out.append(('split/clean', 'split_judge', 'new = old + 2 rows',
                {'only_old': 0, 'only_new': 2, 'common': 3, 'id_collisions': 0},
                J.split_judge(old[:3], old[:3] + [(4, 'B', 1), (5, 'B', 2)])))
    out.append(('stale/planted', 'stale_judge', '3 of 8 not visible',
                {'n': 8, 'stale': 3, 'stale_ratio': 0.375}, J.stale_judge([True, False, True, True, False, False, True, True])))
    out.append(('stale/clean', 'stale_judge', 'all visible',
                {'n': 4, 'stale': 0, 'stale_ratio': 0.0}, J.stale_judge([True] * 4)))
    return out


def run(log=print):
    import os
    res = Results('d/audit.jsonl')
    if os.path.exists(res.path):
        os.remove(res.path)          # the audit is cheap and always re-run in full
        res.done.clear()
    cases = _cases()
    covered = {c[2]['test'] for c in cases if c[1] == 'd1_judge'}
    missing = [t for t in TEST_IDS if t not in covered]
    failed = 0
    for cid, judge_name, planted, expected, got in cases:
        ok = expected == got
        failed += 0 if ok else 1
        res.append({'run_id': f'audit/{cid}', 'exp': 'audit', 'judge': judge_name, 'planted': planted,
                    'expected': expected, 'got': got, 'pass': ok})
    log(f'  audit: {len(cases) - failed}/{len(cases)} cases passed; d1 tests without a case: {missing}')
    if failed or missing:
        raise SystemExit('judge audit failed - see results/d/audit.jsonl')
    return len(cases)


if __name__ == '__main__':
    run()
