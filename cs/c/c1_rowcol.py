"""C-1: row store / column store / files. Same rows, same queries.

  product  pg    PostgreSQL 18: heap table, primary key on event_id, B-tree on ts
           ch    ClickHouse: MergeTree ORDER BY (ts, event_id)
           duck  DuckDB reading one Parquet file written by DuckDB (rows in ts order)
  scale    S / M / L (rows: see config.SCALES)

One run = one product x one scale x one trial, on a fresh container and volume:
  1. ingest the same CSV (server-side read of the mounted file), time each step
  2. size on disk
  3. six queries, 1 untimed warm-up + REPS timed repetitions, each result compared with
     the truth of the generator (times are only meaningful when mismatch = 0)
  4. how much of the table one aggregation reads
  5. writes one row at a time against one batch (RW_ROWS rows)
  6. pg only: latency of key lookups and single inserts while 0 / 1 / 4 analytic
     queries loop on the same server
"""
import json
import os
import random
import threading
import time

from ..common.record import lat_summary
from . import config as C, gen, prod, queries as Q

RW_ROWS = 500
MIX_PHASE_S = 6
MIX_ANALYTIC = [0, 1, 4]
PRODUCTS = ['pg', 'ch', 'duck']


def plan():
    return [{'exp': 'c1', 'product': p, 'scale': s, 'trial': t, 'seed': C.SEED0}
            for s in C.SCALES for p in PRODUCTS for t in range(1, C.TRIALS + 1)]


def run_id_of(s):
    return f'c1:{s["scale"]}:{s["product"]}:t{s["trial"]}'


def bench(run_sql, product, table, truth):
    """Warm-up + REPS repetitions of every query; every result is checked."""
    out = {}
    for q in Q.QUERIES:
        ms, mism, rows = [], 0, 0
        for i in range(C.REPS + 1):
            sql = Q.sql(product, q, table, truth['params'], i)
            t, res = prod.timed(lambda: run_sql(sql))
            d = Q.result_diff(Q.expected(q, truth, i), Q.canon(q, res))
            mism += d['mismatch']
            if i > 0:                      # i == 0 is the warm-up
                ms.append(t)
                rows += d['rows_got']
        out[q] = {'ms': ms, 'mismatch': mism, 'rows_returned': rows}
    return out


def rw_rows(seed):
    rows = gen.rows_of(gen.chunk(seed + 7, 0, gen.CHUNK), range(RW_ROWS))
    for r in rows:
        r[0] += 10_000_000
    return rows


# ------------------------------------------------------------------ PostgreSQL
def _pg_blocks(plan):
    return plan.get('Shared Hit Blocks', 0) + plan.get('Shared Read Blocks', 0)


def _mixed_pg(n_rows, seed):
    """Key lookups and single-row inserts on one connection while k analytic loops run."""
    out = {}
    next_id = [20_000_000]
    for k in MIX_ANALYTIC:
        stop = threading.Event()
        analytic_done = [0] * k
        errors = []

        def analytic(j):
            try:
                with prod.pg() as c:
                    while not stop.is_set():
                        c.execute('SELECT store_id, count(*), sum(amount) FROM events GROUP BY store_id').fetchall()
                        analytic_done[j] += 1
            except Exception as e:  # noqa: BLE001
                errors.append(repr(e)[:200])
        ths = [threading.Thread(target=analytic, args=(j,), daemon=True) for j in range(k)]
        for t in ths:
            t.start()
        time.sleep(0.5 if k else 0)
        rng = random.Random(seed + k)
        look, ins, failed = [], [], 0
        with prod.pg() as c:
            t_end = time.monotonic() + MIX_PHASE_S
            while time.monotonic() < t_end:
                eid = rng.randint(1, n_rows)
                try:
                    t, r = prod.timed(lambda: c.execute('SELECT * FROM events WHERE event_id = %s', (eid,)).fetchall())
                    if len(r) != 1:
                        failed += 1
                    look.append(t)
                    next_id[0] += 1
                    t, _ = prod.timed(lambda: c.execute(
                        'INSERT INTO events_rw (event_id, ts, order_id, store_id, customer_id, item_id, status, quantity, '
                        'amount, delivery_fee, distance_m, prep_s, deliver_s, payment_method, platform, app_version, '
                        "region, courier_id, latency_ms, trace_id, message) VALUES (%s, now(), 1, 1, 1, 1, 'accepted', 1, "
                        "100, 0, 1, 1, 1, 'card', 'web', '6.0.0', 1, 1, 1, 'x', 'mixed')", (next_id[0],)))
                    ins.append(t)
                except Exception:  # noqa: BLE001
                    failed += 1
        stop.set()
        for t in ths:
            t.join(timeout=60)
        out[str(k)] = {'lookup': lat_summary(look), 'insert': lat_summary(ins), 'failed': failed,
                       'analytic_queries_done': sum(analytic_done), 'analytic_errors': errors[:3]}
    return out


def run_pg(s, truth, log):
    n = truth['n']
    ver = prod.pg_up()
    csv = f'/data/{s["scale"]}/events.csv'
    ing = {}
    prod.pgq(f'CREATE TABLE events ({Q.pg_columns()})')
    ing['load_ms'], _ = prod.timed(lambda: prod.pgq(f"COPY events FROM '{csv}' WITH (FORMAT csv, HEADER true)"))
    ing['pk_ms'], _ = prod.timed(lambda: prod.pgq('ALTER TABLE events ADD PRIMARY KEY (event_id)'))
    ing['index_ts_ms'], _ = prod.timed(lambda: prod.pgq('CREATE INDEX events_ts ON events (ts)'))
    ing['vacuum_analyze_ms'], _ = prod.timed(lambda: prod.pgq('VACUUM (ANALYZE) events'))
    prod.pgq('CHECKPOINT')
    ing['total_ms'] = round(sum(ing.values()), 3)
    heap, idx = prod.pgq("select pg_table_size('events'), pg_indexes_size('events')")[0]
    size = {'data_bytes': heap, 'index_bytes': idx, 'total_bytes': heap + idx}
    rows = prod.pgq('select count(*) from events')[0][0]
    with prod.pg() as c:
        qres = bench(lambda sql: c.execute(sql).fetchall(), 'pg', 'events', truth)
        ex = c.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ' + Q.sql('pg', 'agg_store', 'events', truth['params'], 1)).fetchone()[0]
        ex = ex if isinstance(ex, list) else json.loads(ex)
        scan = {'read_bytes': _pg_blocks(ex[0]['Plan']) * 8192, 'stored_bytes': heap,
                'how': 'EXPLAIN (ANALYZE, BUFFERS): shared hit + read blocks x 8192'}
        # one row at a time against one batch
        rw = {}
        rws = rw_rows(s['seed'])
        ph = ', '.join(['%s'] * len(gen.NAMES))
        c.execute(f'CREATE TABLE events_rw ({Q.pg_columns()}, PRIMARY KEY (event_id))')
        t0 = time.perf_counter()
        for r in rws:
            c.execute(f'INSERT INTO events_rw VALUES ({ph})', r)
        rw['single_ms'] = round((time.perf_counter() - t0) * 1000, 1)
        c.execute('TRUNCATE events_rw')
        flat = [v for r in rws for v in r]
        t0 = time.perf_counter()
        c.execute('INSERT INTO events_rw VALUES ' + ', '.join([f'({ph})'] * len(rws)), flat)
        rw['batch_ms'] = round((time.perf_counter() - t0) * 1000, 1)
        rw['rows'] = len(rws)
        rw['rows_after'] = c.execute('select count(*) from events_rw').fetchone()[0]
        rw['how'] = 'INSERT of 1 row per statement (autocommit) / one INSERT with all rows'
    mixed = _mixed_pg(n, s['seed'])
    return {'versions': {'pg': ver}, 'settings': prod.pg_settings(), 'ingest': ing, 'size': size, 'rows_loaded': rows,
            'queries': qres, 'scan': scan, 'rowwrite': rw, 'mixed': mixed}


# ------------------------------------------------------------------ ClickHouse
def run_ch(s, truth, log):
    ver = prod.ch_up()
    f = f"file('{s['scale']}/events.csv', 'CSVWithNames', '{Q.ch_structure()}')"
    ing = {}
    prod.ch(f'CREATE TABLE events ({Q.ch_columns()}) ENGINE = MergeTree ORDER BY (ts, event_id)')
    ing['load_ms'], _ = prod.timed(lambda: prod.ch(f'INSERT INTO events SELECT * FROM {f}'))
    before = prod.ch_table_size('events')
    ing['optimize_final_ms'], _ = prod.timed(lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
    ing['total_ms'] = round(ing['load_ms'] + ing['optimize_final_ms'], 3)
    ts = prod.ch_table_size('events')
    pk = prod.ch("select sum(primary_key_bytes_in_memory_allocated), sum(marks_bytes) from system.parts "
                 "where active and table='events'")[0]
    size = {'data_bytes': ts['bytes_on_disk'], 'index_bytes': int(pk[1] or 0), 'total_bytes': ts['bytes_on_disk'],
            'uncompressed_bytes': ts['uncompressed_bytes'], 'parts': ts['parts'], 'parts_before_optimize': before['parts'],
            'bytes_before_optimize': before['bytes_on_disk'],
            'note': 'index_bytes = mark files, already inside total_bytes'}
    qres = bench(prod.ch, 'ch', 'events', truth)
    _, summ = prod.ch_raw(Q.sql('ch', 'agg_store', 'events', truth['params'], 1) + ' FORMAT Null')
    scan = {'read_bytes': int(summ.get('read_bytes', 0)), 'read_rows': int(summ.get('read_rows', 0)),
            'stored_bytes': ts['uncompressed_bytes'],
            'how': 'X-ClickHouse-Summary read_bytes (uncompressed) against the uncompressed size of the table'}
    # one row at a time against one batch
    rws = rw_rows(s['seed'])
    lines = [json.dumps({k: v for k, v in zip(gen.NAMES, r) if v is not None}).encode() for r in rws]
    rw = {'rows': len(rws)}
    ddl = f'({Q.ch_columns()}) ENGINE = MergeTree ORDER BY (ts, event_id)'
    variants = (('single', {}), ('single_sync', {'async_insert': 0}),
                ('single_nowait', {'async_insert': 1, 'wait_for_async_insert': 0}))
    for name, settings in variants:
        prod.ch(f'CREATE TABLE rw_{name} {ddl}')
        errors = 0
        t0 = time.perf_counter()
        for ln in lines:
            try:
                prod.ch_raw(f'INSERT INTO rw_{name} FORMAT JSONEachRow', settings, body=ln)
            except prod.ChError:
                errors += 1
        rw[f'{name}_ms'] = round((time.perf_counter() - t0) * 1000, 1)
        rw[f'{name}_rows_at_return'] = int(prod.ch(f'select count() from rw_{name}')[0][0])
        sz = prod.ch_table_size(f'rw_{name}')
        rw[f'{name}_parts'], rw[f'{name}_bytes'], rw[f'{name}_errors'] = sz['parts'], sz['bytes_on_disk'], errors
        t_end = time.monotonic() + 15
        while int(prod.ch(f'select count() from rw_{name}')[0][0]) < len(lines) and time.monotonic() < t_end:
            time.sleep(0.2)
        rw[f'{name}_rows_after'] = int(prod.ch(f'select count() from rw_{name}')[0][0])
    prod.ch(f'CREATE TABLE rw_batch {ddl}')
    t0 = time.perf_counter()
    prod.ch_raw('INSERT INTO rw_batch FORMAT JSONEachRow', body=b'\n'.join(lines))
    rw['batch_ms'] = round((time.perf_counter() - t0) * 1000, 1)
    sz = prod.ch_table_size('rw_batch')
    rw['batch_parts'], rw['batch_bytes'] = sz['parts'], sz['bytes_on_disk']
    rw['rows_after'] = int(prod.ch('select count() from rw_batch')[0][0])
    rw['how'] = ('INSERT ... FORMAT JSONEachRow over HTTP, 1 row per request: settings as shipped (single) / '
                 'async_insert=0 (single_sync) / wait_for_async_insert=0 (single_nowait); one request with all rows (batch)')
    rows = int(prod.ch('select count() from events')[0][0])
    return {'versions': {'clickhouse': ver}, 'settings': prod.ch_settings(), 'ingest': ing, 'size': size,
            'rows_loaded': rows, 'queries': qres, 'scan': scan, 'rowwrite': rw}


# ---------------------------------------------------------------------- DuckDB
def run_duck(s, truth, log):
    work = prod.workdir(run_id_of(s))
    con = prod.duck(tmp=work)
    csv = gen.paths(s['scale'])['csv']
    pq = os.path.join(work, 'events.parquet')
    ing = {}
    ing['load_ms'], _ = prod.timed(lambda: con.execute(
        f"COPY (SELECT * FROM read_csv('{csv}', header=true, columns={Q.duck_columns()})) TO '{pq}' (FORMAT parquet)"))
    ing['total_ms'] = ing['load_ms']
    meta = con.execute(f"select count(distinct row_group_id), min(compression), max(compression) "
                       f"from parquet_metadata('{pq}')").fetchone()
    size = {'data_bytes': os.path.getsize(pq), 'index_bytes': 0, 'total_bytes': os.path.getsize(pq),
            'row_groups': meta[0], 'compression': sorted({meta[1], meta[2]})}
    table = f"read_parquet('{pq}')"
    qres = bench(lambda sql: con.execute(sql).fetchall(), 'duck', table, truth)
    r0 = prod.self_rchar()
    con.execute(Q.sql('duck', 'agg_store', table, truth['params'], 1)).fetchall()
    r1 = prod.self_rchar()
    scan = {'read_bytes': None if r0 is None else r1 - r0, 'stored_bytes': os.path.getsize(pq),
            'how': 'rchar of /proc/self/io around the query (bytes asked from the kernel) against the file size'}
    # one row per file against one file
    rws = rw_rows(s['seed'])
    small = os.path.join(work, 'rw.csv')
    gen.write_csv(small, rws)
    con.execute(f"CREATE TABLE rw AS SELECT * FROM read_csv('{small}', header=true, columns={Q.duck_columns()})")
    os.makedirs(os.path.join(work, 'single'))
    rw = {'rows': len(rws)}
    t0 = time.perf_counter()
    for r in rws:
        con.execute(f"COPY (SELECT * FROM rw WHERE event_id = {r[0]}) TO '{work}/single/{r[0]}.parquet' (FORMAT parquet)")
    rw['single_ms'] = round((time.perf_counter() - t0) * 1000, 1)
    t0 = time.perf_counter()
    con.execute(f"COPY rw TO '{work}/batch.parquet' (FORMAT parquet)")
    rw['batch_ms'] = round((time.perf_counter() - t0) * 1000, 1)
    rw['single_files'], rw['single_bytes'] = len(os.listdir(f'{work}/single')), prod.du(f'{work}/single')
    rw['batch_bytes'] = os.path.getsize(f'{work}/batch.parquet')
    cnt = "SELECT count(*), sum(amount) FROM read_parquet('{}')"
    con.execute(cnt.format(f'{work}/single/*.parquet')).fetchall()
    rw['single_scan_ms'] = [prod.timed(lambda: con.execute(cnt.format(f'{work}/single/*.parquet')).fetchall())[0] for _ in range(C.REPS)]
    rw['batch_scan_ms'] = [prod.timed(lambda: con.execute(cnt.format(f'{work}/batch.parquet')).fetchall())[0] for _ in range(C.REPS)]
    rw['rows_after'] = con.execute(cnt.format(f'{work}/single/*.parquet')).fetchone()[0]
    rw['how'] = 'COPY ... TO one Parquet file per row / one file with all rows'
    rows = con.execute(f'select count(*) from {table}').fetchone()[0]
    st = prod.duck_settings(con)
    con.close()
    return {'versions': {'duckdb': prod.versions_client()['duckdb']}, 'settings': st, 'ingest': ing, 'size': size,
            'rows_loaded': rows, 'queries': qres, 'scan': scan, 'rowwrite': rw}


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    truth = gen.ensure(s['scale'], C.SCALES[s['scale']], s['seed'])
    try:
        body = {'pg': run_pg, 'ch': run_ch, 'duck': run_duck}[s['product']](s, truth, log)
        cmem = prod.mem()
    finally:
        prod.down()
        prod.clean_work()
    mism = sum(q['mismatch'] for q in body['queries'].values()) + int(body['rows_loaded'] != truth['n'])
    med = {q: sorted(v['ms'])[len(v['ms']) // 2] for q, v in body['queries'].items()}
    return {'run_id': run_id_of(s), **s, 'n_rows': truth['n'], 'csv_bytes': truth['csv_bytes'], 'reps': C.REPS,
            'mismatch_total': mism, 'container_memory': cmem, 'seconds': round(time.monotonic() - t0, 1), **body,
            'brief': f'load={body["ingest"]["total_ms"]:.0f}ms size={body["size"]["total_bytes"] / 1e6:.1f}MB '
                     f'mismatch={mism} agg_store={med["agg_store"]}ms point={med["point_pk"]}ms'}
