"""C-5: what an index costs and what it buys. Indexes are created before the load.

  pg    idx0 (primary key only) / idx1 (+ customer_id) / idx3 (+ customer_id, order_id,
        (store_id, ts)) / idx_cover (+ customer_id INCLUDE (amount, ts))        data set M
        jsonb_noidx / jsonb_gin_ops (GIN jsonb_ops) / jsonb_gin_path (GIN jsonb_path_ops)
                                                                              data set G
  ch    skip0 (no skipping index) / skip_bloom (bloom_filter on customer_id)   data set M
  duck  tbl0 (native table) / tbl_art (index on customer_id)                    data set M

Measured per run: load time with the indexes in place, bytes of the data and of the
indexes, time to insert INSERT_ROWS more rows in batches of INSERT_BATCH, and the read
that the index is for (REPS repetitions after a warm-up, checked against the truth).
"""
import json
import os
import re
import time

from . import config as C, gen, prod, queries as Q

SCALE = 'M'
G_ROWS = 200_000
INSERT_ROWS, INSERT_BATCH = 20_000, 100
VARIANTS = {'pg': ['idx0', 'idx1', 'idx3', 'idx_cover', 'jsonb_noidx', 'jsonb_gin_ops', 'jsonb_gin_path'],
            'ch': ['skip0', 'skip_bloom'], 'duck': ['tbl0', 'tbl_art']}
VARIANT_JA = {
    'pg:idx0': '主キーだけ', 'pg:idx1': '＋ customer_id の B ツリー', 'pg:idx3': '＋ customer_id、order_id、(store_id, ts) の 3 本',
    'pg:idx_cover': '＋ customer_id INCLUDE (amount, ts)（一部の列を中に持つ索引）',
    'pg:jsonb_noidx': 'jsonb、索引なし', 'pg:jsonb_gin_ops': 'jsonb ＋ GIN（jsonb_ops：既定）',
    'pg:jsonb_gin_path': 'jsonb ＋ GIN（jsonb_path_ops）',
    'ch:skip0': 'スキップ索引なし', 'ch:skip_bloom': '＋ customer_id の bloom_filter のスキップ索引',
    'duck:tbl0': 'DuckDB の表、索引なし', 'duck:tbl_art': '＋ customer_id の索引（ART）',
}
PG_INDEXES = {
    'idx0': [],
    'idx1': ['CREATE INDEX ix_customer ON events (customer_id)'],
    'idx3': ['CREATE INDEX ix_customer ON events (customer_id)', 'CREATE INDEX ix_order ON events (order_id)',
             'CREATE INDEX ix_store_ts ON events (store_id, ts)'],
    'idx_cover': ['CREATE INDEX ix_customer_cover ON events (customer_id) INCLUDE (amount, ts)'],
}


def plan():
    return [{'exp': 'c5', 'product': p, 'variant': v, 'trial': t, 'seed': C.SEED0}
            for p, vs in VARIANTS.items() for v in vs for t in range(1, C.TRIALS + 1)]


def run_id_of(s):
    return f'c5:{s["product"]}:{s["variant"]}:t{s["trial"]}'


def extra_rows(seed):
    rows = gen.rows_of(gen.chunk(seed + 9, 0, gen.CHUNK), range(INSERT_ROWS))
    for r in rows:
        r[0] += 50_000_000
        r[4] = 300_000 + r[4]           # customers that the read queries never ask for
    return rows


def _batches(rows):
    return [rows[i:i + INSERT_BATCH] for i in range(0, len(rows), INSERT_BATCH)]


def _reads(run, truth, sum_sql, rows_sql=None):
    """filter_sum: count and sum(amount) of one customer; filter_rows: all columns of the rows."""
    out = {}
    ia = gen.NAMES.index('amount')
    ms, mism = [], 0
    for i in range(C.REPS + 1):
        c = truth['params']['customers'][i]
        exp = truth['customers'][str(c)]
        t, res = prod.timed(lambda: run(sum_sql.format(c=c)))
        got = (int(res[0][0]), int(res[0][1] or 0))
        mism += int(got != (len(exp), sum(r[ia] for r in exp)))
        if i > 0:
            ms.append(t)
    out['filter_sum'] = {'ms': ms, 'mismatch': mism}
    if rows_sql:
        ms, mism = [], 0
        for i in range(C.REPS + 1):
            c = truth['params']['customers'][i]
            t, res = prod.timed(lambda: run(rows_sql.format(c=c)))
            mism += Q.result_diff(truth['customers'][str(c)], Q.canon('filter_nokey', res))['mismatch']
            if i > 0:
                ms.append(t)
        out['filter_rows'] = {'ms': ms, 'mismatch': mism}
    return out


def run_pg(v, s, truth):
    ver = {'pg': prod.pg_up()}
    rows = extra_rows(s['seed'])
    if v.startswith('jsonb'):
        tg = gen.ensure('G', G_ROWS, s['seed'], ndjson=True)
        prod.pgq('CREATE TABLE events (doc jsonb NOT NULL)')
        if v != 'jsonb_noidx':
            ops = 'jsonb_path_ops' if v == 'jsonb_gin_path' else 'jsonb_ops'
            prod.pgq(f'CREATE INDEX ix_gin ON events USING GIN (doc {ops})')
        load, _ = prod.timed(lambda: prod.pgq(
            "COPY events (doc) FROM '/data/G/events.ndjson' WITH (FORMAT csv, QUOTE E'\\x01', DELIMITER E'\\x02')"))
        docs = [json.dumps({k: x for k, x in zip(gen.NAMES, r) if x is not None}) for r in rows]
        with prod.pg() as c:
            t0 = time.perf_counter()
            for b in _batches(docs):
                c.execute('INSERT INTO events (doc) SELECT unnest(%s::jsonb[])', (b,))
            ins = round((time.perf_counter() - t0) * 1000, 1)
            c.execute('VACUUM (ANALYZE) events')          # also flushes the GIN pending list
            heap, idx = c.execute("select pg_table_size('events'), pg_indexes_size('events')").fetchone()
            run = lambda sql: c.execute(sql).fetchall()  # noqa: E731
            reads = _reads(run, tg, "SELECT count(*), sum((doc->>'amount')::int) FROM events "
                                    "WHERE doc @> '{{\"customer_id\": {c}}}'")
            # "does the key exist": jsonb_ops can use the index, jsonb_path_ops cannot
            ms, mism = [], 0
            for i in range(C.REPS + 1):
                t, res = prod.timed(lambda: run("SELECT count(*) FROM events WHERE doc ? 'coupon_code'"))
                exp = tg['coupon_nonnull'] + sum(1 for r in rows if r[gen.NAMES.index('coupon_code')] is not None)
                mism += int(res[0][0] != exp)
                if i > 0:
                    ms.append(t)
            reads['key_exists'] = {'ms': ms, 'mismatch': mism}
            plans = {}
            for name, sql in (('filter_sum', "SELECT count(*) FROM events WHERE doc @> '{\"customer_id\": 1}'"),
                              ('key_exists', "SELECT count(*) FROM events WHERE doc ? 'coupon_code'")):
                plans[name] = _plan_nodes(c.execute('EXPLAIN (FORMAT JSON) ' + sql).fetchone()[0])
        return {'versions': ver, 'n_rows': tg['n'], 'load_ms': load, 'insert_ms': ins, 'data_bytes': heap, 'index_bytes': idx,
                'reads': reads, 'plans': plans}
    prod.pgq(f'CREATE TABLE events ({Q.pg_columns()}, PRIMARY KEY (event_id))')
    for ddl in PG_INDEXES[v]:
        prod.pgq(ddl)
    load, _ = prod.timed(lambda: prod.pgq(f"COPY events FROM '/data/{SCALE}/events.csv' WITH (FORMAT csv, HEADER true)"))
    ph = '(' + ', '.join(['%s'] * len(gen.NAMES)) + ')'
    with prod.pg() as c:
        t0 = time.perf_counter()
        for b in _batches(rows):
            c.execute('INSERT INTO events VALUES ' + ', '.join([ph] * len(b)), [x for r in b for x in r])
        ins = round((time.perf_counter() - t0) * 1000, 1)
        c.execute('VACUUM (ANALYZE) events')
        heap, idx = c.execute("select pg_table_size('events'), pg_indexes_size('events')").fetchone()
        per_index = {r[0]: r[1] for r in c.execute(
            "select indexrelname, pg_relation_size(indexrelid) from pg_stat_user_indexes where relname = 'events'").fetchall()}
        run = lambda sql: c.execute(sql).fetchall()  # noqa: E731
        reads = _reads(run, truth, 'SELECT count(*), sum(amount) FROM events WHERE customer_id = {c}',
                       'SELECT * FROM events WHERE customer_id = {c} ORDER BY event_id')
        plans = {'filter_sum': _plan_nodes(c.execute(
            'EXPLAIN (FORMAT JSON) SELECT count(*), sum(amount) FROM events WHERE customer_id = 1').fetchone()[0])}
    return {'versions': ver, 'n_rows': truth['n'], 'load_ms': load, 'insert_ms': ins, 'data_bytes': heap, 'index_bytes': idx,
            'per_index_bytes': per_index, 'reads': reads, 'plans': plans}


def _plan_nodes(ex):
    ex = ex if isinstance(ex, list) else json.loads(ex)
    out = []

    def walk(n):
        out.append(n['Node Type'] + (f" ({n['Index Name']})" if 'Index Name' in n else ''))
        for ch in n.get('Plans', []):
            walk(ch)
    walk(ex[0]['Plan'])
    return out


def run_ch(v, s, truth):
    ver = {'clickhouse': prod.ch_up()}
    ix = ', INDEX ix_customer customer_id TYPE bloom_filter GRANULARITY 1' if v == 'skip_bloom' else ''
    prod.ch(f'CREATE TABLE events ({Q.ch_columns()}{ix}) ENGINE = MergeTree ORDER BY (ts, event_id)')
    load, _ = prod.timed(lambda: prod.ch(
        f"INSERT INTO events SELECT * FROM file('{SCALE}/events.csv', 'CSVWithNames', '{Q.ch_structure()}')"))
    opt, _ = prod.timed(lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
    rows = extra_rows(s['seed'])
    bodies = [b'\n'.join(json.dumps({k: x for k, x in zip(gen.NAMES, r) if x is not None}).encode() for r in b)
              for b in _batches(rows)]
    t0 = time.perf_counter()
    for b in bodies:
        prod.ch_raw('INSERT INTO events FORMAT JSONEachRow', {'async_insert': 0}, body=b)
    ins = round((time.perf_counter() - t0) * 1000, 1)
    prod.ch('OPTIMIZE TABLE events FINAL')
    ts = prod.ch_table_size('events')
    sk = prod.ch("select sum(secondary_indices_compressed_bytes) from system.parts where active and table = 'events'")[0][0]
    reads = _reads(prod.ch, truth, 'SELECT count(), sum(amount) FROM events WHERE customer_id = {c}',
                   'SELECT * FROM events WHERE customer_id = {c} ORDER BY event_id')
    _, summ = prod.ch_raw(f"SELECT count() FROM events WHERE customer_id = {truth['params']['customers'][1]} FORMAT Null")
    return {'versions': ver, 'n_rows': truth['n'], 'load_ms': round(load + opt, 3), 'insert_ms': ins,
            'data_bytes': ts['bytes_on_disk'] - int(sk or 0), 'index_bytes': int(sk or 0), 'reads': reads,
            'read_rows': int(summ.get('read_rows', 0)),
            'insert_how': f'{len(bodies)} synchronous inserts of {INSERT_BATCH} rows (async_insert=0)'}


def run_duck(v, s, truth, work):
    db = os.path.join(work, 'events.duckdb')
    con = prod.duck(db, tmp=work)
    con.execute(f'CREATE TABLE events ({Q.duck_ddl()})')
    if v == 'tbl_art':
        con.execute('CREATE INDEX ix_customer ON events (customer_id)')
    src = f"read_csv('{gen.paths(SCALE)['csv']}', header=true, columns={Q.duck_columns()})"
    load, _ = prod.timed(lambda: con.execute(f'INSERT INTO events SELECT * FROM {src}'))
    rows = extra_rows(s['seed'])
    ph = '(' + ', '.join(['?'] * len(gen.NAMES)) + ')'
    t0 = time.perf_counter()
    for b in _batches(rows):
        con.execute('INSERT INTO events VALUES ' + ', '.join([ph] * len(b)), [x for r in b for x in r])
    ins = round((time.perf_counter() - t0) * 1000, 1)
    con.execute('CHECKPOINT')
    run = lambda sql: con.execute(sql).fetchall()  # noqa: E731
    reads = _reads(run, truth, 'SELECT count(*), sum(amount) FROM events WHERE customer_id = {c}',
                   'SELECT * FROM events WHERE customer_id = {c} ORDER BY event_id')
    plan = con.execute('EXPLAIN SELECT count(*), sum(amount) FROM events WHERE customer_id = 1').fetchall()
    text = ' '.join(str(r[1]) for r in plan)
    size = os.path.getsize(db)
    con.close()
    return {'versions': {'duckdb': prod.versions_client()['duckdb']}, 'n_rows': truth['n'], 'load_ms': load, 'insert_ms': ins,
            'data_bytes': size, 'index_bytes': 0, 'reads': reads,
            'plans': {'filter_sum': [x.strip() for x in re.findall(r'╭─ ([A-Za-z ]+?) ─', text)]},
            'note': 'data_bytes is the whole database file (the index is inside it)'}


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    truth = gen.ensure(SCALE, C.SCALES[SCALE], s['seed'])
    p, v = s['product'], s['variant']
    try:
        if p == 'pg':
            body = run_pg(v, s, truth)
        elif p == 'ch':
            body = run_ch(v, s, truth)
        else:
            body = run_duck(v, s, truth, prod.workdir(run_id_of(s)))
        cmem = prod.mem()
    finally:
        prod.down()
        prod.clean_work()
    mism = sum(r['mismatch'] for r in body['reads'].values())
    med = {k: sorted(r['ms'])[len(r['ms']) // 2] for k, r in body['reads'].items()}
    return {'run_id': run_id_of(s), **s, 'insert_rows': INSERT_ROWS, 'insert_batch': INSERT_BATCH, 'reps': C.REPS,
            'mismatch_total': mism, 'container_memory': cmem, 'seconds': round(time.monotonic() - t0, 1), **body,
            'brief': f'load={body["load_ms"]:.0f}ms insert={body["insert_ms"]:.0f}ms data={body["data_bytes"] / 1e6:.1f}MB '
                     f'index={body["index_bytes"] / 1e6:.1f}MB reads={med} mismatch={mism}'}
