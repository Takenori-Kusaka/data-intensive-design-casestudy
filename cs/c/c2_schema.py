"""C-2: schema on write (typed columns) against schema on read (JSON kept as it came).

Part "perf"  (PERF_ROWS rows, all 23 fields): size, ingest time and three queries for
  pg_cols / pg_jsonb / ch_cols / ch_json / duck_parquet / duck_ndjson
  (no secondary index anywhere: C-5 measures indexes).

Part "drift" (DRIFT_ROWS rows of 7 fields; the last DRIFT_ROWS - DRIFT_FROM rows drift):
  where does the failure show when the upstream changes the shape?
  cases      none / num_as_string / text_in_number / int_to_float / field_removed /
             field_added / field_renamed / explicit_null
  pipelines  typed columns as each product ships them (N), the product's "skip errors"
             option, JSON kept as it came, and BE: validated typed columns + a column for
             unknown keys + a quarantine table (E-C2 of DESIGN.md)
  For every run the truth (what the upstream meant, row by row) comes from the generator;
  the rows a reader can see afterwards are classified by judges.classify_drift, and the
  reader's sum over the drifted field is compared with the truth.
"""
import json
import os
import re
import time

from . import config as C, gen, judges, prod, queries as Q

PERF_ROWS = 500_000
PERF_TRIALS = C.TRIALS
PERF_VARIANTS = ['pg_cols', 'pg_jsonb', 'ch_cols', 'ch_json', 'duck_parquet', 'duck_ndjson']
PERF_QUERIES = ['agg_store', 'agg_day', 'filter_count']

DRIFT_ROWS, DRIFT_FROM = 30_000, 24_000
FIELDS = ['event_id', 'ts', 'store_id', 'status', 'amount', 'delivery_fee', 'distance_m']
INT_FIELDS = ['event_id', 'store_id', 'amount', 'delivery_fee', 'distance_m']
CASES = {
    'none': {'probe': 'amount', 'ja': '揺れなし（正常な操作）'},
    'num_as_string': {'probe': 'amount', 'ja': '型が変わる：数値が数字の文字列に（1200 → "1200"）'},
    'text_in_number': {'probe': 'amount', 'ja': '型が変わる：数値が単位つきの文字列に（1200 → "1200 JPY"）'},
    'int_to_float': {'probe': 'distance_m', 'ja': '型が変わる：整数が小数に（1234 → 1234.5）'},
    'field_removed': {'probe': 'delivery_fee', 'ja': 'フィールドが消える（delivery_fee）'},
    'field_added': {'probe': 'tip', 'ja': 'フィールドが増える（tip）'},
    'field_renamed': {'probe': 'amount', 'probe2': 'total_amount', 'ja': '名前が変わる（amount → total_amount）'},
    'explicit_null': {'probe': 'amount', 'ja': '値が null になる（amount: null）'},
}
PIPELINES = {
    'pg': ['typed', 'typed_nullable', 'csv', 'csv_ignore', 'jsonb', 'be'],
    'ch': ['typed', 'typed_skip', 'json', 'be'],
    'duck': ['typed', 'typed_ignore', 'auto', 'auto_full', 'parquet_files', 'parquet_union', 'be'],
}
PIPELINE_JA = {
    'pg:typed': 'jsonb_populate_record で型つきの表へ（列は NOT NULL）',
    'pg:typed_nullable': '同上（列は NULL を許す）',
    'pg:csv': 'CSV を COPY（既定）',
    'pg:csv_ignore': 'CSV を COPY（ON_ERROR ignore）',
    'pg:jsonb': 'jsonb の列にそのまま入れる',
    'pg:be': 'BE：検査した型つきの列＋未知のキーの jsonb＋保留の表',
    'ch:typed': 'JSONEachRow で型つきの表へ（既定。列は Nullable でない）',
    'ch:typed_skip': '同上（input_format_allow_errors_num を指定）',
    'ch:json': 'JSON 型の列にそのまま入れる',
    'ch:be': 'BE：検査した型つきの列＋未知のキーの列＋保留の表',
    'duck:typed': 'read_json に列の型を指定して表へ',
    'duck:typed_ignore': '同上（ignore_errors=true）',
    'duck:auto': 'read_json の自動判定で NDJSON を直接読む（既定の sample_size）',
    'duck:auto_full': '同上（sample_size=-1：全行から判定）',
    'duck:parquet_files': 'バッチごとの Parquet を read_parquet で読む（既定）',
    'duck:parquet_union': '同上（union_by_name=true）',
    'duck:be': 'BE：検査した型つきの列＋未知のキーの JSON＋保留の表',
}
KIND = {'typed': 'N', 'typed_nullable': 'N', 'csv': 'N', 'typed_skip': 'skip', 'typed_ignore': 'skip',
        'csv_ignore': 'skip', 'jsonb': 'pass', 'json': 'pass', 'auto': 'pass', 'auto_full': 'pass',
        'parquet_files': 'pass', 'parquet_union': 'pass', 'be': 'BE'}


def plan():
    out = [{'exp': 'c2', 'part': 'perf', 'variant': v, 'trial': t, 'seed': C.SEED0}
           for v in PERF_VARIANTS for t in range(1, PERF_TRIALS + 1)]
    for p, pls in PIPELINES.items():
        for pl in pls:
            for case in CASES:
                if pl.startswith('csv') and case == 'field_renamed':
                    continue            # a CSV file names its columns once, in the header
                out.append({'exp': 'c2', 'part': 'drift', 'product': p, 'pipeline': pl, 'case': case, 'seed': C.SEED0})
    return out


def run_id_of(s):
    if s['part'] == 'perf':
        return f'c2:perf:{s["variant"]}:t{s["trial"]}'
    return f'c2:drift:{s["product"]}:{s["pipeline"]}:{s["case"]}'


def _err(e):
    # keep local paths out of the results (error texts quote file names, sometimes cut short)
    text = str(e).replace(C.DATA, '<data>')
    text = re.sub(re.escape(os.path.expanduser('~')) + r'[^ )]*', '<data>', text)
    return ' '.join(text.split())[:240]


def _try(fn):
    t0 = time.perf_counter()
    try:
        v = fn()
        return {'ok': True, 'error': None, 'ms': round((time.perf_counter() - t0) * 1000, 1)}, v
    except Exception as e:  # noqa: BLE001 - the error is the measurement
        return {'ok': False, 'error': _err(e), 'ms': round((time.perf_counter() - t0) * 1000, 1)}, None


# ================================================================ part: perf
def _perf_sql(variant, q, truth, i, src):
    c = truth['params']['customers'][i]
    if variant in ('pg_cols', 'ch_cols', 'duck_parquet', 'duck_ndjson'):
        p = variant.split('_')[0]
        if q == 'filter_count':
            return f'SELECT count(*), sum(amount) FROM {src} WHERE customer_id = {c}'
        return Q.sql(p, q, src, truth['params'], i)
    if variant == 'pg_jsonb':
        if q == 'agg_store':
            return (f"SELECT (doc->>'store_id')::int AS s, count(*), sum((doc->>'amount')::int) FROM {src} "
                    f"GROUP BY s ORDER BY s")
        if q == 'agg_day':
            return (f"SELECT (doc->>'ts')::timestamp::date AS d, sum((doc->>'quantity')::int) FROM {src} "
                    f"WHERE doc->>'status' = 'completed' GROUP BY d ORDER BY d")
        return f"SELECT count(*), sum((doc->>'amount')::int) FROM {src} WHERE (doc->>'customer_id')::int = {c}"
    if variant == 'ch_json':
        if q == 'agg_store':
            return (f'SELECT doc.store_id.:Int64 AS s, count(), sum(doc.amount.:Int64) FROM {src} '
                    f'GROUP BY s ORDER BY s')
        if q == 'agg_day':
            return (f"SELECT toDate(doc.ts.:DateTime) AS d, sum(doc.quantity.:Int64) FROM {src} "
                    f"WHERE doc.status.:String = 'completed' GROUP BY d ORDER BY d")
        return f'SELECT count(), sum(doc.amount.:Int64) FROM {src} WHERE doc.customer_id.:Int64 = {c}'
    raise KeyError(variant)


def _perf_expected(q, truth, i):
    if q == 'filter_count':
        rows = truth['customers'][str(truth['params']['customers'][i])]
        ia = gen.NAMES.index('amount')
        return [[len(rows), sum(r[ia] for r in rows) if rows else None]]
    return Q.expected(q, truth, i)


def _perf_canon(q, rows):
    if q == 'filter_count':
        return [[int(r[0]), None if r[1] is None or (int(r[0]) == 0) else int(r[1])] for r in rows]
    return Q.canon(q, rows)


def _perf_bench(run_sql, variant, src, truth):
    out = {}
    for q in PERF_QUERIES:
        ms, mism = [], 0
        for i in range(C.REPS + 1):
            sql = _perf_sql(variant, q, truth, i, src)
            t, res = prod.timed(lambda: run_sql(sql))
            mism += Q.result_diff(_perf_expected(q, truth, i), _perf_canon(q, res))['mismatch']
            if i > 0:
                ms.append(t)
        out[q] = {'ms': ms, 'mismatch': mism}
    return out


def run_perf(s):
    truth = gen.ensure('J', PERF_ROWS, s['seed'], ndjson=True)
    v = s['variant']
    ing, size, ver, st = {}, {}, {}, {}
    if v.startswith('pg'):
        ver = {'pg': prod.pg_up()}
        if v == 'pg_cols':
            prod.pgq(f'CREATE TABLE events ({Q.pg_columns()})')
            ing['load_ms'], _ = prod.timed(lambda: prod.pgq("COPY events FROM '/data/J/events.csv' WITH (FORMAT csv, HEADER true)"))
        else:
            prod.pgq('CREATE TABLE events (doc jsonb NOT NULL)')
            ing['load_ms'], _ = prod.timed(lambda: prod.pgq(
                "COPY events (doc) FROM '/data/J/events.ndjson' WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02')"))
        ing['vacuum_analyze_ms'], _ = prod.timed(lambda: prod.pgq('VACUUM (ANALYZE) events'))
        prod.pgq('CHECKPOINT')
        size['total_bytes'] = prod.pgq("select pg_total_relation_size('events')")[0][0]
        rows = prod.pgq('select count(*) from events')[0][0]
        with prod.pg() as c:
            qres = _perf_bench(lambda sql: c.execute(sql).fetchall(), v, 'events', truth)
        st = prod.pg_settings()
    elif v.startswith('ch'):
        ver = {'clickhouse': prod.ch_up()}
        if v == 'ch_cols':
            prod.ch(f'CREATE TABLE events ({Q.ch_columns()}) ENGINE = MergeTree ORDER BY (ts, event_id)')
            ing['load_ms'], _ = prod.timed(lambda: prod.ch(
                f"INSERT INTO events SELECT * FROM file('J/events.csv', 'CSVWithNames', '{Q.ch_structure()}')"))
        else:
            prod.ch('CREATE TABLE events (doc JSON) ENGINE = MergeTree ORDER BY tuple()')
            ing['load_ms'], _ = prod.timed(lambda: prod.ch(
                "INSERT INTO events SELECT json FROM file('J/events.ndjson', 'JSONAsObject', 'json JSON')"))
        ing['optimize_final_ms'], _ = prod.timed(lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
        ts = prod.ch_table_size('events')
        size = {'total_bytes': ts['bytes_on_disk'], 'uncompressed_bytes': ts['uncompressed_bytes'], 'parts': ts['parts']}
        rows = int(prod.ch('select count() from events')[0][0])
        qres = _perf_bench(prod.ch, v, 'events', truth)
        st = prod.ch_settings()
        if v == 'ch_json':
            # the JSON type decides a type per path while reading: record what "ts" became, and
            # how many rows a reader gets who asks for it as the string it was in the file
            size['ts_dynamic_types'] = {r[0]: int(r[1]) for r in prod.ch('SELECT dynamicType(doc.ts) t, count() FROM events GROUP BY t')}
            size['ts_as_string_nonnull'] = int(prod.ch('SELECT count() FROM events WHERE doc.ts.:String IS NOT NULL')[0][0])
    else:
        work = prod.workdir(run_id_of(s))
        con = prod.duck(tmp=work)
        p = gen.paths('J')
        if v == 'duck_parquet':
            pq = os.path.join(work, 'events.parquet')
            ing['load_ms'], _ = prod.timed(lambda: con.execute(
                f"COPY (SELECT * FROM read_csv('{p['csv']}', header=true, columns={Q.duck_columns()})) TO '{pq}' (FORMAT parquet)"))
            size['total_bytes'] = os.path.getsize(pq)
            src = f"read_parquet('{pq}')"
        else:
            ing['load_ms'] = 0.0             # nothing to load: the file is read as it is
            size['total_bytes'] = os.path.getsize(p['ndjson'])
            src = f"read_json('{p['ndjson']}')"
        rows = con.execute(f'select count(*) from {src}').fetchone()[0]
        qres = _perf_bench(lambda sql: con.execute(sql).fetchall(), v, src, truth)
        ver = {'duckdb': prod.versions_client()['duckdb']}
        st = prod.duck_settings(con)
        con.close()
    ing['total_ms'] = round(sum(ing.values()), 3)
    mism = sum(q['mismatch'] for q in qres.values()) + int(rows != truth['n'])
    return {'n_rows': truth['n'], 'csv_bytes': truth['csv_bytes'], 'ndjson_bytes': truth['ndjson_bytes'], 'reps': C.REPS,
            'versions': ver, 'settings': st, 'ingest': ing, 'size': size, 'rows_loaded': rows, 'queries': qres,
            'mismatch_total': mism,
            'brief': f'load={ing["total_ms"]:.0f}ms size={size["total_bytes"] / 1e6:.1f}MB mismatch={mism} '
                     f'agg_store={sorted(qres["agg_store"]["ms"])[C.REPS // 2]}ms'}


# =============================================================== part: drift
def make_drift(case, seed):
    """Documents (in file order) and the truth, row by row, for one case."""
    cols = gen.chunk(seed + 100, 0, DRIFT_ROWS)
    idx = {n: gen.NAMES.index(n) for n in FIELDS}
    docs, truth, truth2 = [], {}, {}
    probe = CASES[case]['probe']
    for i, r in enumerate(gen.rows_of(cols)):
        d = {n: r[idx[n]] for n in FIELDS}
        eid = d['event_id']
        drifted = i >= DRIFT_FROM and case != 'none'
        faithful = d.get(probe)
        if drifted:
            if case == 'num_as_string':
                d['amount'] = str(d['amount'])
            elif case == 'text_in_number':
                d['amount'] = f'{d["amount"]} JPY'
            elif case == 'int_to_float':
                d['distance_m'] = d['distance_m'] + 0.5
                faithful = d['distance_m']
            elif case == 'field_removed':
                del d['delivery_fee']
                faithful = None
            elif case == 'field_added':
                d['tip'] = 100 + (eid % 5) * 100
                faithful = d['tip']
            elif case == 'field_renamed':
                d['total_amount'] = d.pop('amount')
                truth2[eid] = d['total_amount']
            elif case == 'explicit_null':
                d['amount'] = None
                faithful = None
        truth[eid] = {'drifted': drifted, 'faithful': faithful}
        docs.append(d)
    return docs, truth, truth2


def write_files(work, docs, case):
    """NDJSON (whole file, and the clean / drifted batches) and the CSV a naive exporter would write."""
    p = {k: os.path.join(work, f) for k, f in (('ndjson', 'd.ndjson'), ('b1', 'b1.ndjson'), ('b2', 'b2.ndjson'), ('csv', 'd.csv'))}
    with open(p['ndjson'], 'w', encoding='utf-8', newline='\n') as f, open(p['b1'], 'w', encoding='utf-8', newline='\n') as f1, \
            open(p['b2'], 'w', encoding='utf-8', newline='\n') as f2, open(p['csv'], 'w', encoding='utf-8', newline='\n') as fc:
        fc.write(','.join(FIELDS) + '\n')
        for i, d in enumerate(docs):
            line = json.dumps(d, separators=(',', ':')) + '\n'
            f.write(line)
            (f1 if i < DRIFT_FROM else f2).write(line)
            fc.write(','.join('' if v is None else str(v) for v in d.values()) + '\n')
    for x in p.values():
        os.chmod(x, 0o644)
    return p


def _observe(p, probe):
    """What a reader of the probe field gets: ({id: text|None} or None when nothing can be
    read, field_absent, error head, ids_readable).
    field_absent: the store has no such column, so every row reads as "no value".
    ids_readable: a reader that does not touch the probe field can still read the rows."""
    def ids():
        return {int(r[0]): None for r in p['ids']()}
    try:
        cols = p['columns']() if p['columns'] else None
    except Exception as e:  # noqa: BLE001 - the target does not exist (ingest failed)
        return None, False, _err(e), False
    if cols is not None and probe not in cols:
        try:
            return ids(), True, None, True
        except Exception as e:  # noqa: BLE001
            return None, True, _err(e), False
    try:
        return {int(r[0]): (None if r[1] is None else str(r[1])) for r in p['observe']()}, False, None, True
    except Exception as e:  # noqa: BLE001
        err = _err(e)
        try:
            ids()
            return None, False, err, True
        except Exception:  # noqa: BLE001
            return None, False, err, False


def _query(scalar, sql, truth_value, agg='sum'):
    """The reader's aggregate over the probe field (sum, or avg of the same expression)."""
    assert sql.startswith('SELECT sum(')
    if agg == 'avg':
        sql = 'SELECT avg(' + sql[len('SELECT sum('):]
    st, v = _try(lambda: scalar(sql))
    if st['ok']:
        v = None if v is None else float(v)
        if v is not None and v != v:          # NaN (avg over no rows in ClickHouse)
            v = None
        if truth_value is None:
            st.update(value=v, truth=None, equal=v is None, diff=None)
        else:
            st.update(value=v, truth=truth_value, **judges.judge_sum(truth_value, v))
    else:
        st.update(value=None, truth=truth_value, equal=False, diff=None)
    return st


_PG_TYPED = ('event_id bigint PRIMARY KEY, ts timestamp {nn}, store_id integer {nn}, status text {nn}, '
             'amount integer {nn}, delivery_fee integer {nn}, distance_m integer {nn}')


def _pg_int_ok(f, required):
    ok = f"coalesce(jsonb_typeof(doc->'{f}') = 'number' AND (doc->>'{f}') ~ '^-?[0-9]+$', false)"
    if required:
        return ok
    return f"(doc->'{f}' IS NULL OR jsonb_typeof(doc->'{f}') = 'null' OR {ok})"


def drift_pg(pl, files, rel, probe, probe2):
    """Returns ingest status, observe(), ids-only(), strict query, tolerant query, quarantine ids."""
    q = prod.pgq
    out = {'quarantine': lambda: [], 'query_tolerant': None, 'probe2': None, 'columns': None,
           'scalar': lambda sql: q(sql)[0][0]}
    if pl in ('typed', 'typed_nullable'):
        q('CREATE TABLE ev (' + _PG_TYPED.format(nn='NOT NULL' if pl == 'typed' else '') + ')')
        q('CREATE TABLE stg (doc jsonb)')

        def ingest():
            with prod.pg(autocommit=False) as c:
                c.execute(f"COPY stg (doc) FROM '/data/{rel}/d.ndjson' WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02')")
                c.execute('INSERT INTO ev SELECT (jsonb_populate_record(null::ev, doc)).* FROM stg')
                c.commit()
        out.update(ingest=ingest, observe=lambda: q(f'SELECT event_id, {probe}::text FROM ev'), columns=lambda: FIELDS,
                   ids=lambda: q('SELECT event_id, NULL FROM ev'), query=(f'SELECT sum({probe}) FROM ev'))
    elif pl in ('csv', 'csv_ignore'):
        q('CREATE TABLE ev (' + _PG_TYPED.format(nn='NOT NULL') + ')')
        opt = ', ON_ERROR ignore' if pl == 'csv_ignore' else ''
        out.update(ingest=lambda: q(f"COPY ev FROM '/data/{rel}/d.csv' WITH (FORMAT csv, HEADER true{opt})"),
                   observe=lambda: q(f'SELECT event_id, {probe}::text FROM ev'), columns=lambda: FIELDS,
                   ids=lambda: q('SELECT event_id, NULL FROM ev'), query=(f'SELECT sum({probe}) FROM ev'))
    elif pl == 'jsonb':
        q('CREATE TABLE evj (doc jsonb NOT NULL)')
        out.update(ingest=lambda: q(f"COPY evj (doc) FROM '/data/{rel}/d.ndjson' WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02')"),
                   observe=lambda: q(f"SELECT (doc->>'event_id')::bigint, doc->>'{probe}' FROM evj"),
                   ids=lambda: q("SELECT (doc->>'event_id')::bigint, NULL FROM evj"),
                   query=(f"SELECT sum((doc->>'{probe}')::numeric) FROM evj"),
                   query_tolerant=(f"SELECT sum(CASE WHEN jsonb_typeof(doc->'{probe}') = 'number' "
                                         f"THEN (doc->>'{probe}')::numeric END) FROM evj"))
        if probe2:
            out['probe2'] = lambda: q(f"SELECT (doc->>'event_id')::bigint, doc->>'{probe2}' FROM evj WHERE doc ? '{probe2}'")
    elif pl == 'be':
        q('CREATE TABLE ev (event_id bigint PRIMARY KEY, ts timestamp NOT NULL, store_id integer NOT NULL, '
          'status text NOT NULL, amount integer NOT NULL, delivery_fee integer, distance_m integer NOT NULL, extra jsonb)')
        q('CREATE TABLE stg (doc jsonb)')
        q('CREATE TABLE quarantine (doc jsonb NOT NULL, reason text NOT NULL)')
        reason = ('CASE ' + ' '.join(f"WHEN NOT {_pg_int_ok(f, f != 'delivery_fee')} THEN '{f}'" for f in INT_FIELDS)
                  + " WHEN jsonb_typeof(doc->'ts') IS DISTINCT FROM 'string' THEN 'ts'"
                  + " WHEN jsonb_typeof(doc->'status') IS DISTINCT FROM 'string' THEN 'status' END")
        known = "ARRAY['" + "','".join(FIELDS) + "']"

        def ingest():
            with prod.pg(autocommit=False) as c:
                c.execute(f"COPY stg (doc) FROM '/data/{rel}/d.ndjson' WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02')")
                c.execute(f'CREATE TEMP TABLE v ON COMMIT DROP AS SELECT doc, {reason} AS reason FROM stg')
                c.execute('INSERT INTO quarantine SELECT doc, reason FROM v WHERE reason IS NOT NULL')
                c.execute("INSERT INTO ev SELECT (doc->>'event_id')::bigint, (doc->>'ts')::timestamp, (doc->>'store_id')::int, "
                          "doc->>'status', (doc->>'amount')::int, (doc->>'delivery_fee')::int, (doc->>'distance_m')::int, "
                          f"NULLIF(doc - {known}, '{{}}'::jsonb) FROM v WHERE reason IS NULL")
                c.commit()
        col = probe in FIELDS
        sel = f'{probe}::text' if col else f"extra->>'{probe}'"
        out.update(ingest=ingest, observe=lambda: q(f'SELECT event_id, {sel} FROM ev'),
                   ids=lambda: q('SELECT event_id, NULL FROM ev'),
                   query=(f'SELECT sum({probe}) FROM ev' if col else f"SELECT sum((extra->>'{probe}')::numeric) FROM ev"),
                   quarantine=lambda: [int(r[0]) for r in q("SELECT doc->>'event_id' FROM quarantine")])
        if probe2:
            out['probe2'] = lambda: q(f"SELECT event_id, extra->>'{probe2}' FROM ev WHERE extra ? '{probe2}'")
    return out


_CH_TYPED = ('event_id Int64, ts DateTime, store_id Int32, status String, amount Int32, {fee}, distance_m Int32')


def drift_ch(pl, files, rel, probe, probe2):
    ch = prod.ch
    out = {'quarantine': lambda: [], 'query_tolerant': None, 'probe2': None, 'columns': None,
           'scalar': lambda sql: ch(sql)[0][0]}
    body = lambda: open(files['ndjson'], 'rb').read()  # noqa: E731
    if pl in ('typed', 'typed_skip'):
        ch('CREATE TABLE ev (' + _CH_TYPED.format(fee='delivery_fee Int32') + ') ENGINE = MergeTree ORDER BY event_id')
        st = {'input_format_allow_errors_num': 1_000_000} if pl == 'typed_skip' else {}
        out.update(ingest=lambda: prod.ch_raw('INSERT INTO ev FORMAT JSONEachRow', st, body=body()),
                   observe=lambda: ch(f'SELECT event_id, toString({probe}) FROM ev'), columns=lambda: FIELDS,
                   ids=lambda: ch('SELECT event_id, NULL FROM ev'), query=(f'SELECT sum({probe}) FROM ev'))
    elif pl == 'json':
        ch('CREATE TABLE evj (doc JSON) ENGINE = MergeTree ORDER BY tuple()')
        out.update(ingest=lambda: prod.ch_raw('INSERT INTO evj FORMAT JSONAsObject', body=body()),
                   observe=lambda: ch(f'SELECT doc.event_id.:Int64, if(doc.{probe} IS NULL, NULL, toString(doc.{probe})) FROM evj'),
                   ids=lambda: ch('SELECT doc.event_id.:Int64, NULL FROM evj'),
                   query=(f'SELECT sum(doc.{probe}::Float64) FROM evj'),
                   query_tolerant=(f'SELECT sum(doc.{probe}.:Int64) FROM evj'))
        if probe2:
            out['probe2'] = lambda: ch(f'SELECT doc.event_id.:Int64, toString(doc.{probe2}) FROM evj WHERE doc.{probe2} IS NOT NULL')
    elif pl == 'be':
        ch('CREATE TABLE ev (' + _CH_TYPED.format(fee='delivery_fee Nullable(Int32)') + ', extra String) '
           'ENGINE = MergeTree ORDER BY event_id')
        ch('CREATE TABLE stg (line String) ENGINE = MergeTree ORDER BY tuple()')
        ch('CREATE TABLE quarantine (line String, reason String) ENGINE = MergeTree ORDER BY tuple()')
        conds = []
        for f in INT_FIELDS:
            ok = f"JSONType(line, '{f}') = 'Int64'"
            if f == 'delivery_fee':
                ok = f"(NOT JSONHas(line, '{f}') OR JSONType(line, '{f}') = 'Null' OR {ok})"
            conds.append(f"NOT {ok}, '{f}'")
        conds += ["JSONType(line, 'ts') != 'String', 'ts'", "JSONType(line, 'status') != 'String', 'status'"]
        reason = 'multiIf(' + ', '.join(conds) + ", '')"
        known = "('" + "','".join(FIELDS) + "')"
        extra = ("concat('{', arrayStringConcat(arrayMap(x -> concat(toJSONString(x.1), ':', x.2), "
                 f"arrayFilter(x -> x.1 NOT IN {known}, JSONExtractKeysAndValuesRaw(line))), ','), '}}')")

        def ingest():
            prod.ch_raw('INSERT INTO stg FORMAT JSONAsString', body=body())
            ch(f"INSERT INTO quarantine SELECT line, {reason} AS reason FROM stg WHERE reason != ''")
            ch("INSERT INTO ev SELECT JSONExtractInt(line, 'event_id'), parseDateTimeBestEffort(JSONExtractString(line, 'ts')), "
               "JSONExtractInt(line, 'store_id'), JSONExtractString(line, 'status'), JSONExtractInt(line, 'amount'), "
               "if(JSONType(line, 'delivery_fee') = 'Int64', JSONExtractInt(line, 'delivery_fee'), NULL), "
               f"JSONExtractInt(line, 'distance_m'), {extra} FROM stg WHERE {reason} = ''")
        col = probe in FIELDS
        sel = (f'toString({probe})' if col
               else f"if(JSONHas(extra, '{probe}'), JSONExtractRaw(extra, '{probe}'), NULL)")
        out.update(ingest=ingest, observe=lambda: ch(f'SELECT event_id, {sel} FROM ev'),
                   ids=lambda: ch('SELECT event_id, NULL FROM ev'),
                   query=(f'SELECT sum({probe}) FROM ev' if col
                                else f"SELECT sum(if(JSONHas(extra, '{probe}'), JSONExtractFloat(extra, '{probe}'), NULL)) FROM ev"),
                   quarantine=lambda: [int(r[0]) for r in ch("SELECT JSONExtractInt(line, 'event_id') FROM quarantine")])
        if probe2:
            out['probe2'] = lambda: ch(f"SELECT event_id, JSONExtractRaw(extra, '{probe2}') FROM ev WHERE JSONHas(extra, '{probe2}')")
    return out


_DUCK_COLS = ("{'event_id': 'BIGINT', 'ts': 'TIMESTAMP', 'store_id': 'INTEGER', 'status': 'VARCHAR', "
              "'amount': 'INTEGER', 'delivery_fee': 'INTEGER', 'distance_m': 'INTEGER'}")


def drift_duck(pl, files, con, probe, probe2):
    fa = lambda sql: (lambda: con.execute(sql).fetchall())  # noqa: E731
    out = {'quarantine': lambda: [], 'query_tolerant': None, 'probe2': None, 'columns': None,
           'scalar': lambda sql: con.execute(sql).fetchone()[0]}
    nd = files['ndjson']
    cols_of = lambda src: (lambda: [r[0] for r in con.execute(f'DESCRIBE SELECT * FROM {src}').fetchall()])  # noqa: E731
    if pl in ('typed', 'typed_ignore'):
        opt = ', ignore_errors=true' if pl == 'typed_ignore' else ''
        out.update(ingest=lambda: con.execute(f"CREATE TABLE ev AS SELECT * FROM read_json('{nd}', columns={_DUCK_COLS}{opt})"),
                   observe=fa(f'SELECT event_id, CAST({probe} AS VARCHAR) FROM ev'), ids=fa('SELECT event_id, NULL FROM ev'),
                   query=(f'SELECT sum({probe}) FROM ev'), columns=cols_of('ev'))
    elif pl in ('auto', 'auto_full'):
        src = f"read_json('{nd}'" + (', sample_size=-1' if pl == 'auto_full' else '') + ')'
        out.update(ingest=lambda: None, columns=cols_of(src),
                   observe=fa(f'SELECT event_id, CAST({probe} AS VARCHAR) FROM {src}'), ids=fa(f'SELECT event_id, NULL FROM {src}'),
                   query=(f'SELECT sum({probe}) FROM {src}'),
                   query_tolerant=(f'SELECT sum(TRY_CAST({probe} AS DOUBLE)) FROM {src}'))
        if probe2:
            out['probe2'] = fa(f'SELECT event_id, CAST({probe2} AS VARCHAR) FROM {src} WHERE {probe2} IS NOT NULL')
    elif pl in ('parquet_files', 'parquet_union'):
        d = os.path.dirname(nd)
        src = f"read_parquet(['{d}/b1.parquet', '{d}/b2.parquet']" + (', union_by_name=true' if pl == 'parquet_union' else '') + ')'

        def ingest():
            # each batch is written by the exporter of its day: types as found in that batch
            con.execute(f"COPY (SELECT * FROM read_json('{files['b1']}', columns={_DUCK_COLS})) TO '{d}/b1.parquet' (FORMAT parquet)")
            con.execute(f"COPY (SELECT * FROM read_json('{files['b2']}', sample_size=-1)) TO '{d}/b2.parquet' (FORMAT parquet)")
        out.update(ingest=ingest, columns=cols_of(src), observe=fa(f'SELECT event_id, CAST({probe} AS VARCHAR) FROM {src}'),
                   ids=fa(f'SELECT event_id, NULL FROM {src}'), query=(f'SELECT sum({probe}) FROM {src}'),
                   query_tolerant=(f'SELECT sum(TRY_CAST({probe} AS DOUBLE)) FROM {src}'))
        if probe2:
            out['probe2'] = fa(f'SELECT event_id, CAST({probe2} AS VARCHAR) FROM {src} WHERE {probe2} IS NOT NULL')
    elif pl == 'be':
        con.execute('CREATE TABLE ev (event_id BIGINT PRIMARY KEY, ts TIMESTAMP NOT NULL, store_id INTEGER NOT NULL, '
                    'status VARCHAR NOT NULL, amount INTEGER NOT NULL, delivery_fee INTEGER, distance_m INTEGER NOT NULL, extra JSON)')
        con.execute('CREATE TABLE quarantine (doc JSON, reason VARCHAR)')
        conds = []
        for f in INT_FIELDS:
            ok = f"json_type(json, '$.{f}') IN ('UBIGINT', 'BIGINT')"
            if f == 'delivery_fee':
                ok = f"(json_type(json, '$.{f}') IS NULL OR json_type(json, '$.{f}') = 'NULL' OR {ok})"
            else:
                ok = f"coalesce({ok}, false)"
            conds.append(f"WHEN NOT {ok} THEN '{f}'")
        conds += [f"WHEN coalesce(json_type(json, '$.{f}'), '') != 'VARCHAR' THEN '{f}'" for f in ('ts', 'status')]
        reason = 'CASE ' + ' '.join(conds) + ' END'
        patch = json.dumps(dict.fromkeys(FIELDS))

        def ingest():
            con.execute('BEGIN')
            con.execute(f"CREATE TEMP TABLE v AS SELECT json, {reason} AS reason FROM read_json_objects('{nd}')")
            con.execute('INSERT INTO quarantine SELECT json, reason FROM v WHERE reason IS NOT NULL')
            con.execute("INSERT INTO ev SELECT CAST(json->>'event_id' AS BIGINT), CAST(json->>'ts' AS TIMESTAMP), "
                        "CAST(json->>'store_id' AS INTEGER), json->>'status', CAST(json->>'amount' AS INTEGER), "
                        "CAST(json->>'delivery_fee' AS INTEGER), CAST(json->>'distance_m' AS INTEGER), "
                        f"NULLIF(CAST(json_merge_patch(json, '{patch}') AS VARCHAR), '{{}}')::JSON FROM v WHERE reason IS NULL")
            con.execute('COMMIT')
        col = probe in FIELDS
        sel = f'CAST({probe} AS VARCHAR)' if col else f"extra->>'{probe}'"
        out.update(ingest=ingest, observe=fa(f'SELECT event_id, {sel} FROM ev'), ids=fa('SELECT event_id, NULL FROM ev'),
                   query=(f'SELECT sum({probe}) FROM ev' if col
                                else f"SELECT sum(CAST(extra->>'{probe}' AS DOUBLE)) FROM ev"),
                   quarantine=lambda: [int(r[0]) for r in con.execute("SELECT json_extract(doc, '$.event_id') FROM quarantine").fetchall()])
        if probe2:
            out['probe2'] = fa(f"SELECT event_id, extra->>'{probe2}' FROM ev WHERE extra->>'{probe2}' IS NOT NULL")
    return out


def run_drift(s):
    case, pl, product = s['case'], s['pipeline'], s['product']
    probe, probe2 = CASES[case]['probe'], CASES[case].get('probe2')
    docs, truth, truth2 = make_drift(case, s['seed'])
    work = prod.workdir(run_id_of(s))
    files = write_files(work, docs, case)
    rel = os.path.relpath(work, C.DATA)
    con, ver = None, {}
    if product == 'pg':
        ver = {'pg': prod.pg_up()}
        p = drift_pg(pl, files, rel, probe, probe2)
    elif product == 'ch':
        ver = {'clickhouse': prod.ch_up()}
        p = drift_ch(pl, files, rel, probe, probe2)
    else:
        con = prod.duck(tmp=work)
        ver = {'duckdb': prod.versions_client()['duckdb']}
        p = drift_duck(pl, files, con, probe, probe2)
    ingest, _ = _try(p['ingest'])
    observed, field_absent, observe_error, ids_readable = _observe(p, probe)
    try:
        quarantined = p['quarantine']()
    except Exception:  # noqa: BLE001 - no quarantine table after a failed ingest
        quarantined = []
    counts = judges.classify_drift(truth, observed, quarantined)
    vals = [t['faithful'] for t in truth.values() if t['faithful'] is not None]
    truth_sum, truth_avg = sum(vals), (sum(vals) / len(vals) if vals else None)
    query = _query(p['scalar'], p['query'], truth_sum)
    query_avg = _query(p['scalar'], p['query'], truth_avg, 'avg')
    tol = _query(p['scalar'], p['query_tolerant'], truth_sum) if p['query_tolerant'] else None
    kept2 = None
    if probe2:
        try:
            got2 = {int(r[0]): judges._num(r[1]) for r in p['probe2']()} if p['probe2'] else {}
        except Exception:  # noqa: BLE001
            got2 = {}
        kept2 = sum(1 for i, v in truth2.items() if got2.get(i) == v)
    if con is not None:
        con.close()
    d, c = counts['drifted'], counts['clean']
    return {'n_rows': len(docs), 'n_drifted': sum(1 for t in truth.values() if t['drifted']), 'probe': probe,
            'kind': KIND[pl], 'versions': ver, 'ingest': ingest, 'rows_visible': None if observed is None else len(observed),
            'field_absent': field_absent, 'observe_error': observe_error, 'ids_readable': ids_readable, 'quarantined': len(quarantined),
            'counts': counts, 'query': query, 'query_avg': query_avg, 'query_tolerant': tol, 'probe2': probe2, 'probe2_kept': kept2,
            'brief': f'ingest={"ok" if ingest["ok"] else "ERR"} visible={None if observed is None else len(observed)} '
                     f'drifted={ {k: v for k, v in d.items() if v} } clean_bad={sum(v for k, v in c.items() if k != "faithful")} '
                     f'query={"ok" if query["ok"] else "ERR"}/{query["equal"]} avg={"ok" if query_avg["ok"] else "ERR"}/{query_avg["equal"]}'}


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    try:
        body = run_perf(s) if s['part'] == 'perf' else run_drift(s)
        cmem = prod.mem()
    finally:
        prod.down()
        prod.clean_work()
    return {'run_id': run_id_of(s), **s, 'container_memory': cmem, 'seconds': round(time.monotonic() - t0, 1), **body}
