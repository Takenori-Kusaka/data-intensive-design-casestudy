"""Audit of the judges of topic C: feed them data with planted errors and data without
errors, and check that they count exactly what was planted. Results: results/c/audit.jsonl.

  result_diff     a query result against the truth (missing / extra / duplicate / wrong value / order)
  classify_value  one value against what the upstream meant
  classify_drift  the rows a reader sees after a schema drift (real generator, planted outcomes)
  judge_sum       the reader's aggregate against the truth
  judge_table     a table after re-runs (duplicates / missing / stale / extra)
  retention       the check of the rows left after dropping old data
  judge_delivery  C-6: accepted / rejected / lost / duplicated log records
  end_to_end      the whole path generator -> CSV -> DuckDB/Parquet -> six queries -> result_diff,
                  on a copy of the CSV with one planted error (a row removed, a value changed,
                  a row duplicated, a NULL filled): the checker must report a mismatch
"""
import os

from ..common.record import Results
from . import c1_rowcol, c2_schema, c3_retention, c4_rerun, c6_collector, config as C, gen, judges as J, prod, queries as Q


def _result_diff():
    exp = [[1, 10, 100], [2, 20, 200], [3, 30, 300]]
    z = {'missing': 0, 'extra': 0, 'duplicates': 0, 'order': 0, 'mismatch': 0}
    return [
        ('clean', lambda: Q.result_diff(exp, [list(r) for r in exp]), z),
        ('missing_1', lambda: Q.result_diff(exp, exp[:2]), {**z, 'missing': 1, 'mismatch': 1}),
        ('extra_1', lambda: Q.result_diff(exp, exp + [[4, 40, 400]]), {**z, 'extra': 1, 'mismatch': 1}),
        ('duplicate_1', lambda: Q.result_diff(exp, exp + [exp[0]]), {**z, 'duplicates': 1, 'mismatch': 1}),
        ('wrong_value_1', lambda: Q.result_diff(exp, [exp[0], [2, 20, 201], exp[2]]),
         {**z, 'missing': 1, 'extra': 1, 'mismatch': 2}),
        ('order_swapped', lambda: Q.result_diff(exp, [exp[1], exp[0], exp[2]]), {**z, 'order': 1, 'mismatch': 1}),
        ('empty_expected_empty', lambda: Q.result_diff([], []), z),
        ('canon_types', lambda: Q.canon('agg_store', [('7', '2', 30.0)]), [[7, 2, 30]]),
        ('canon_row_null_and_ts', lambda: Q.canon_row([5, '2026-10-01 00:00:25.000', 6] + [1] * 3 + ['completed'] + [1] * 6
                                                      + ['card', 'web', '6.0.0', 1, None, 1, None, 1, 'ab', 'm'])[:3],
         [5, '2026-10-01 00:00:25', 6]),
    ]


def _classify_value():
    cv = J.classify_value
    return [
        ('faithful_number', lambda: cv(1200, '1200'), 'faithful'),
        ('faithful_quoted_number', lambda: cv(1200, '"1200"'), 'faithful'),
        ('faithful_float', lambda: cv(1234.5, '1234.5'), 'faithful'),
        ('rounded_is_altered', lambda: cv(1234.5, '1234'), 'altered'),
        ('null_when_value_meant', lambda: cv(1200, None), 'null'),
        ('zero_when_value_meant', lambda: cv(1200, '0'), 'defaulted'),
        ('raw_text_kept', lambda: cv(1200, '1200 JPY'), 'raw_text'),
        ('absent_stays_absent', lambda: cv(None, None), 'faithful'),
        ('absent_filled_with_zero', lambda: cv(None, '0'), 'defaulted'),
        ('absent_filled_with_other', lambda: cv(None, '7'), 'altered'),
        ('true_zero_is_faithful', lambda: cv(0, '0'), 'faithful'),
    ]


def _classify_drift():
    out = []
    zero = dict.fromkeys(J.CLASSES, 0)

    def planted(case, edit):
        _docs, truth, _t2 = c2_schema.make_drift(case, C.SEED0)
        obs = {i: (None if t['faithful'] is None else str(t['faithful'])) for i, t in truth.items()}
        drifted = [i for i, t in truth.items() if t['drifted']]
        clean = [i for i, t in truth.items() if not t['drifted']]
        q = edit(obs, drifted, clean) or []
        return J.classify_drift(truth, obs, q)
    n, nd = c2_schema.DRIFT_ROWS, c2_schema.DRIFT_ROWS - c2_schema.DRIFT_FROM
    out.append(('clean_none', lambda: planted('none', lambda o, d, c: None),
                {'drifted': zero, 'clean': {**zero, 'faithful': n}, 'unexpected_rows': 0}))
    out.append(('clean_text_in_number', lambda: planted('text_in_number', lambda o, d, c: None),
                {'drifted': {**zero, 'faithful': nd}, 'clean': {**zero, 'faithful': n - nd}}))

    def mix(o, d, c):
        for i in d[:5]:
            o[i] = '0'
        for i in d[5:8]:
            del o[i]
        for i in d[8:12]:
            o[i] = None
        for i in d[12:14]:
            o[i] = o[i] + ' JPY'
        o[d[14]] = str(float(o[d[14]]) + 1)
        del o[c[0]]
        o[c[1]] = None
        o[10**9] = '1'
        return [d[20], d[21], c[2]]
    out.append(('planted_mix', lambda: planted('num_as_string', mix),
                {'drifted': {**zero, 'defaulted': 5, 'row_missing': 3, 'null': 4, 'raw_text': 2, 'altered': 1,
                             'quarantined': 2, 'faithful': nd - 17},
                 'clean': {**zero, 'row_missing': 1, 'null': 1, 'quarantined': 1, 'faithful': n - nd - 3},
                 'unexpected_rows': 1, 'both_loaded_and_quarantined': 3}))

    def removed(o, d, c):
        for i in d[:6]:
            o[i] = '0'
    out.append(('removed_field_filled_with_zero_6', lambda: planted('field_removed', removed),
                {'drifted': {**zero, 'defaulted': 6, 'faithful': nd - 6}, 'clean': {**zero, 'faithful': n - nd}}))
    out.append(('nothing_readable', lambda: J.classify_drift(c2_schema.make_drift('none', C.SEED0)[1], None),
                {'drifted': zero, 'clean': {**zero, 'unreadable': n}}))
    out.append(('batch_rejected', lambda: J.classify_drift(c2_schema.make_drift('int_to_float', C.SEED0)[1], {}),
                {'drifted': {**zero, 'row_missing': nd}, 'clean': {**zero, 'row_missing': n - nd}}))
    return out


def _judge_sum():
    return [
        ('equal', lambda: J.judge_sum(100, 100.0), {'equal': True, 'diff': 0.0}),
        ('short_by_30', lambda: J.judge_sum(100, 70), {'equal': False, 'diff': -30.0}),
        ('null_result', lambda: J.judge_sum(100, None), {'equal': False, 'diff': None}),
        ('null_result_truth_zero', lambda: J.judge_sum(0, None), {'equal': True, 'diff': 0}),
        ('float_half', lambda: J.judge_sum(10.5, 10), {'equal': False, 'diff': -0.5}),
    ]


def _judge_table():
    exp = {i: i * 10 for i in range(1, 11)}
    rows = [(i, i * 10) for i in range(1, 11)]
    z = {'rows_expected': 10, 'duplicates': 0, 'missing': 0, 'extra': 0, 'stale': 0, 'wrong': 0}
    b = c4_rerun.make_batches(C.SEED0)
    v1 = {r[0]: r[4] for r in b['days'][1]}
    v2 = {r[0]: r[4] for r in b['day2_v2']}
    return [
        ('clean', lambda: J.judge_table(exp, rows), z),
        ('duplicates_3', lambda: J.judge_table(exp, rows + rows[:3]), {**z, 'duplicates': 3, 'wrong': 3}),
        ('missing_2', lambda: J.judge_table(exp, rows[2:]), {**z, 'missing': 2, 'wrong': 2}),
        ('stale_1', lambda: J.judge_table(exp, [(1, 11)] + rows[1:]), {**z, 'stale': 1, 'wrong': 1}),
        ('extra_4', lambda: J.judge_table(exp, rows + [(k, 1) for k in range(100, 104)]), {**z, 'extra': 4, 'wrong': 4}),
        ('all_planted', lambda: J.judge_table(exp, [(1, 11)] + rows[3:] + rows[3:5] + [(99, 1)]),
         {**z, 'stale': 1, 'missing': 2, 'duplicates': 2, 'extra': 1, 'wrong': 6}),
        # the generator of C-4: the old version of day 2 judged against the corrected one
        ('generator_corrected_vs_old', lambda: J.judge_table(v2, list(v1.items())),
         {'rows_expected': c4_rerun.DAY_ROWS - c4_rerun.REMOVED + c4_rerun.ADDED, 'duplicates': 0,
          'missing': c4_rerun.ADDED, 'extra': c4_rerun.REMOVED, 'stale': c4_rerun.CHANGED}),
    ]


def _retention():
    truth = {'rows_by_day': [10] * 30, 'amount_by_day': [1000] * 30}
    return [
        ('clean', lambda: c3_retention._check(200, 20000, truth), {'mismatch': 0}),
        ('one_row_left_over', lambda: c3_retention._check(201, 20000, truth), {'mismatch': 1}),
        ('nothing_dropped', lambda: c3_retention._check(300, 30000, truth), {'mismatch': 2}),
        ('sum_off', lambda: c3_retention._check(200, 19999, truth), {'mismatch': 1}),
    ]


def _end_to_end():
    """Six queries on DuckDB/Parquet over a CSV with one planted error; the truth is the generator's."""
    truth = gen.ensure('S', C.SCALES['S'], C.SEED0)
    prm = truth['params']
    pid = prm['point_ids'][1]
    ic = gen.NAMES.index('coupon_code')
    pid_null = next(i for i in prm['point_ids'][1:] if truth['points'][str(i)][ic] is None)

    def run(edit_sql):
        work = prod.workdir('audit_e2e')
        con = prod.duck(tmp=work)
        try:
            src = f"read_csv('{gen.paths('S')['csv']}', header=true, columns={Q.duck_columns()})"
            con.execute(f'CREATE TABLE e AS SELECT * FROM {src}')
            if edit_sql:
                con.execute(edit_sql)
            pq = os.path.join(work, 'e.parquet')
            con.execute(f"COPY (SELECT * FROM e ORDER BY event_id) TO '{pq}' (FORMAT parquet)")
            res = c1_rowcol.bench(lambda sql: con.execute(sql).fetchall(), 'duck', f"read_parquet('{pq}')", truth)
            return {q: int(v['mismatch'] > 0) for q, v in res.items()}
        finally:
            con.close()
            prod.clean_work()
    z = dict.fromkeys(Q.QUERIES, 0)
    return [
        ('clean', lambda: run(None), z),
        ('row_removed', lambda: run(f'DELETE FROM e WHERE event_id = {pid}'),
         {'point_pk': 1, 'agg_store': 1}),
        ('amount_changed', lambda: run(f'UPDATE e SET amount = amount + 1 WHERE event_id = {pid}'),
         {'point_pk': 1, 'agg_store': 1}),
        ('row_duplicated', lambda: run(f'INSERT INTO e SELECT * FROM e WHERE event_id = {pid}'),
         {'point_pk': 1, 'agg_store': 1}),
        ('null_filled', lambda: run(f"UPDATE e SET coupon_code = '' WHERE event_id = {pid_null}"),
         {'point_pk': 1, 'agg_store': 0, 'agg_day': 0, 'topn': 0}),
        ('status_changed_everywhere', lambda: run("UPDATE e SET status = 'done' WHERE status = 'completed'"),
         {'agg_day': 1, 'agg_store': 0, 'topn': 0}),
    ]


SUITES = [('result_diff', _result_diff), ('classify_value', _classify_value), ('classify_drift', _classify_drift),
          ('judge_sum', _judge_sum), ('judge_table', _judge_table), ('retention', _retention),
          ('end_to_end', _end_to_end), ('judge_delivery', c6_collector.audit_cases)]


def _check(got, want):
    """Every key named in ``want`` must match (nested dicts key by key)."""
    if not isinstance(want, dict) or not isinstance(got, dict):
        return {} if got == want else {'got': got, 'want': want}
    diffs = {}
    for k, w in want.items():
        g = got.get(k)
        if isinstance(w, dict) and isinstance(g, dict):
            d = _check(g, w)
            if d:
                diffs[k] = d
        elif g != w:
            diffs[k] = {'got': g, 'want': w}
    return diffs


def run(log=print):
    res = Results('c/audit.jsonl')
    failed, n = [], 0
    for judge, suite in SUITES:
        for name, fn, want in suite():
            n += 1
            try:
                got = fn()
                diffs = _check(got, want)
            except Exception as e:  # noqa: BLE001
                got, diffs = repr(e)[:300], {'exception': repr(e)[:300]}
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
