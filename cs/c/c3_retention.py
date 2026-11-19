"""C-3: compression, sort order, and dropping old data (retention). Data set M.

Part "compress": the same rows stored with another sort order and another codec
  ch    MergeTree, ORDER BY ts / store / random  x  codec default (LZ4) / ZSTD(3)
  duck  Parquet written in ts / store / random order  x  snappy / zstd / uncompressed
  pg    the heap as it is (no per-table knob for short values; shown as the reference)
  measured: write time, bytes, the largest columns, three queries (checked against the truth)

Part "retain": remove the oldest 10 of the 30 days, by each way the product offers
  pg    delete (DELETE, then VACUUM, then VACUUM FULL) / partition_drop
  ch    alter_delete (mutation) / lightweight_delete (then OPTIMIZE FINAL) / drop_partition / ttl
  duck  files_per_day (remove the files) / single_file_rewrite / table_delete (native table)
  measured: time of every step, bytes before and after every step; the rows that remain are
  compared with the truth of the generator (count and sum(amount) of days 11..30)
"""
import os
import shutil
import time

from . import config as C, gen, prod, queries as Q

SCALE = 'M'
KEEP_FROM_DAY = 10                    # days 0..9 are dropped
CUTOFF = gen.ts_str(gen.BASE_TS + KEEP_FROM_DAY * 86400)
ORDERS = {'ts': {'ch': '(ts, event_id)', 'duck': None},
          'store': {'ch': '(store_id, ts, event_id)', 'duck': 'store_id, ts, event_id'},
          'random': {'ch': '(trace_id)', 'duck': 'trace_id'}}
ORDER_JA = {'ts': '時刻の順', 'store': '店舗、時刻の順', 'random': '乱数の列（trace_id）の順'}
CH_CODECS = {'lz4': None, 'zstd': 'ZSTD(3)'}
DUCK_CODECS = ['snappy', 'zstd', 'uncompressed']
BENCH_Q = ['range_ts', 'agg_store', 'agg_day']
RETAIN = {'pg': ['delete', 'partition_drop'],
          'ch': ['alter_delete', 'lightweight_delete', 'drop_partition', 'ttl'],
          'duck': ['files_per_day', 'single_file_rewrite', 'table_delete']}
RETAIN_JA = {'pg:delete': 'DELETE → VACUUM → VACUUM FULL', 'pg:partition_drop': '10 日ごとのパーティションを DROP',
             'ch:alter_delete': 'ALTER TABLE … DELETE（ミューテーション）',
             'ch:lightweight_delete': 'DELETE FROM（軽量な削除）→ OPTIMIZE FINAL',
             'ch:drop_partition': '日ごとのパーティションを DROP PARTITION', 'ch:ttl': 'TTL を付けて適用（MODIFY TTL）',
             'duck:files_per_day': '日ごとの Parquet ファイルを消す', 'duck:single_file_rewrite': '1 つの Parquet を書き直す',
             'duck:table_delete': 'DuckDB の表で DELETE → CHECKPOINT'}


def plan():
    out = [{'exp': 'c3', 'part': 'compress', 'product': 'pg', 'order': 'ts', 'codec': 'none', 'trial': t, 'seed': C.SEED0}
           for t in range(1, C.TRIALS + 1)]
    out += [{'exp': 'c3', 'part': 'compress', 'product': 'ch', 'order': o, 'codec': c, 'trial': t, 'seed': C.SEED0}
            for o in ORDERS for c in CH_CODECS for t in range(1, C.TRIALS + 1)]
    out += [{'exp': 'c3', 'part': 'compress', 'product': 'duck', 'order': o, 'codec': c, 'trial': t, 'seed': C.SEED0}
            for o in ORDERS for c in DUCK_CODECS for t in range(1, C.TRIALS + 1)]
    out += [{'exp': 'c3', 'part': 'retain', 'product': p, 'method': m, 'trial': t, 'seed': C.SEED0}
            for p, ms in RETAIN.items() for m in ms for t in range(1, C.TRIALS + 1)]
    return out


def run_id_of(s):
    if s['part'] == 'compress':
        return f'c3:compress:{s["product"]}:{s["order"]}:{s["codec"]}:t{s["trial"]}'
    return f'c3:retain:{s["product"]}:{s["method"]}:t{s["trial"]}'


def _bench(run_sql, product, table, truth):
    out = {}
    for q in BENCH_Q:
        ms, mism = [], 0
        for i in range(C.REPS + 1):
            t, res = prod.timed(lambda: run_sql(Q.sql(product, q, table, truth['params'], i)))
            mism += Q.result_diff(Q.expected(q, truth, i), Q.canon(q, res))['mismatch']
            if i > 0:
                ms.append(t)
        out[q] = {'ms': ms, 'mismatch': mism}
    return out


# ============================================================ part: compress
def run_compress(s, truth):
    p, order, codec = s['product'], s['order'], s['codec']
    if p == 'pg':
        ver = {'pg': prod.pg_up()}
        prod.pgq(f'CREATE TABLE events ({Q.pg_columns()})')
        load, _ = prod.timed(lambda: prod.pgq(f"COPY events FROM '/data/{SCALE}/events.csv' WITH (FORMAT csv, HEADER true)"))
        prod.pgq('CREATE INDEX events_ts ON events (ts)')
        prod.pgq('VACUUM (ANALYZE) events')
        heap, idx = prod.pgq("select pg_table_size('events'), pg_indexes_size('events')")[0]
        with prod.pg() as c:
            qres = _bench(lambda sql: c.execute(sql).fetchall(), 'pg', 'events', truth)
        return {'versions': ver, 'write_ms': load, 'bytes': heap, 'index_bytes': idx, 'top_columns': [],
                'queries': qres, 'note': 'heap only; the B-tree on ts is counted in index_bytes'}
    if p == 'ch':
        ver = {'clickhouse': prod.ch_up()}
        prod.ch(f'CREATE TABLE events ({Q.ch_columns(CH_CODECS[codec])}) ENGINE = MergeTree ORDER BY {ORDERS[order]["ch"]}')
        load, _ = prod.timed(lambda: prod.ch(
            f"INSERT INTO events SELECT * FROM file('{SCALE}/events.csv', 'CSVWithNames', '{Q.ch_structure()}')"))
        opt, _ = prod.timed(lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
        ts = prod.ch_table_size('events')
        top = prod.ch("select name, data_compressed_bytes, data_uncompressed_bytes from system.columns "
                      "where table = 'events' and database = 'default' order by data_compressed_bytes desc limit 4")
        qres = _bench(prod.ch, 'ch', 'events', truth)
        return {'versions': ver, 'write_ms': round(load + opt, 3), 'bytes': ts['bytes_on_disk'], 'index_bytes': 0,
                'uncompressed_bytes': ts['uncompressed_bytes'],
                'top_columns': [[r[0], int(r[1]), int(r[2])] for r in top], 'queries': qres}
    work = prod.workdir(run_id_of(s))
    con = prod.duck(tmp=work)
    pq = os.path.join(work, 'events.parquet')
    src = f"read_csv('{gen.paths(SCALE)['csv']}', header=true, columns={Q.duck_columns()})"
    ob = f' ORDER BY {ORDERS[order]["duck"]}' if ORDERS[order]['duck'] else ''
    load, _ = prod.timed(lambda: con.execute(
        f"COPY (SELECT * FROM {src}{ob}) TO '{pq}' (FORMAT parquet, COMPRESSION {codec})"))
    top = con.execute(f"select path_in_schema, sum(total_compressed_size), sum(total_uncompressed_size) "
                      f"from parquet_metadata('{pq}') group by 1 order by 2 desc limit 4").fetchall()
    qres = _bench(lambda sql: con.execute(sql).fetchall(), 'duck', f"read_parquet('{pq}')", truth)
    size = os.path.getsize(pq)
    con.close()
    return {'versions': {'duckdb': prod.versions_client()['duckdb']}, 'write_ms': load, 'bytes': size, 'index_bytes': 0,
            'top_columns': [[r[0], int(r[1]), int(r[2])] for r in top], 'queries': qres}


# ============================================================== part: retain
def _check(count, total, truth):
    exp_n = sum(truth['rows_by_day'][KEEP_FROM_DAY:])
    exp_sum = sum(truth['amount_by_day'][KEEP_FROM_DAY:])
    return {'rows_expected': exp_n, 'rows_got': int(count), 'sum_expected': exp_sum, 'sum_got': int(total or 0),
            'mismatch': int(int(count) != exp_n) + int(int(total or 0) != exp_sum)}


def _pg_size(tables):
    return sum(prod.pgq(f"select pg_total_relation_size('{t}')")[0][0] for t in tables)


def retain_pg(method, truth):
    ver = {'pg': prod.pg_up()}
    steps = []
    csv = f"/data/{SCALE}/events.csv"
    if method == 'delete':
        prod.pgq(f'CREATE TABLE events ({Q.pg_columns()}, PRIMARY KEY (event_id))')
        prod.pgq('CREATE INDEX events_ts ON events (ts)')
        prod.pgq(f"COPY events FROM '{csv}' WITH (FORMAT csv, HEADER true)")
        prod.pgq('VACUUM (ANALYZE) events')
        before = _pg_size(['events'])
        for name, sql in (('DELETE', f"DELETE FROM events WHERE ts < '{CUTOFF}'"), ('VACUUM', 'VACUUM events'),
                          ('VACUUM FULL', 'VACUUM FULL events')):
            ms, _ = prod.timed(lambda: prod.pgq(sql))
            steps.append({'step': name, 'ms': ms, 'bytes_after': _pg_size(['events'])})
        tables = ['events']
    else:
        prod.pgq(f'CREATE TABLE events ({Q.pg_columns()}, PRIMARY KEY (event_id, ts)) PARTITION BY RANGE (ts)')
        for i in range(3):
            a, b = gen.ts_str(gen.BASE_TS + i * 10 * 86400), gen.ts_str(gen.BASE_TS + (i + 1) * 10 * 86400)
            prod.pgq(f"CREATE TABLE events_p{i} PARTITION OF events FOR VALUES FROM ('{a}') TO ('{b}')")
        prod.pgq('CREATE INDEX events_ts ON events (ts)')
        prod.pgq(f"COPY events FROM '{csv}' WITH (FORMAT csv, HEADER true)")
        prod.pgq('VACUUM (ANALYZE) events')
        before = _pg_size(['events_p0', 'events_p1', 'events_p2'])
        ms, _ = prod.timed(lambda: prod.pgq('DROP TABLE events_p0'))
        steps.append({'step': 'DROP TABLE（パーティション）', 'ms': ms, 'bytes_after': _pg_size(['events_p1', 'events_p2'])})
        tables = ['events_p1', 'events_p2']
    n, total = prod.pgq('select count(*), sum(amount) from events')[0]
    return {'versions': ver, 'bytes_before': before, 'steps': steps, 'bytes_final': _pg_size(tables),
            'check': _check(n, total, truth)}


def retain_ch(method, truth):
    ver = {'clickhouse': prod.ch_up()}
    part = ' PARTITION BY toDate(ts)' if method == 'drop_partition' else ''
    prod.ch(f'CREATE TABLE events ({Q.ch_columns()}) ENGINE = MergeTree{part} ORDER BY (ts, event_id)')
    prod.ch(f"INSERT INTO events SELECT * FROM file('{SCALE}/events.csv', 'CSVWithNames', '{Q.ch_structure()}')",
            {'max_partitions_per_insert_block': 1000})
    prod.ch('OPTIMIZE TABLE events FINAL')
    size = lambda: prod.ch_table_size('events')['bytes_on_disk']  # noqa: E731
    before, steps = size(), []

    def step(name, fn):
        ms, _ = prod.timed(fn)
        steps.append({'step': name, 'ms': ms, 'bytes_after': size()})
    if method == 'alter_delete':
        step('ALTER TABLE DELETE（mutations_sync=2）',
             lambda: prod.ch(f"ALTER TABLE events DELETE WHERE ts < '{CUTOFF}'", {'mutations_sync': 2}))
    elif method == 'lightweight_delete':
        step('DELETE FROM', lambda: prod.ch(f"DELETE FROM events WHERE ts < '{CUTOFF}'"))
        n_mid, total_mid = prod.ch('select count(), sum(amount) from events')[0]
        steps[-1]['check_after'] = _check(n_mid, total_mid, truth)['mismatch']
        step('OPTIMIZE FINAL', lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
    elif method == 'drop_partition':
        days = [gen.ts_str(gen.BASE_TS + d * 86400)[:10] for d in range(KEEP_FROM_DAY)]

        def drop():
            for d in days:
                prod.ch(f"ALTER TABLE events DROP PARTITION '{d}'")
        step(f'DROP PARTITION × {len(days)}', drop)
    elif method == 'ttl':
        # a fixed boundary instead of now(): rows before the cutoff are already expired, the others never expire
        ttl = f"if(ts < toDateTime('{CUTOFF}'), toDateTime('2000-01-01 00:00:00'), toDateTime('2100-01-01 00:00:00'))"
        step('MODIFY TTL（materialize、mutations_sync=2）',
             lambda: prod.ch(f'ALTER TABLE events MODIFY TTL {ttl}', {'mutations_sync': 2, 'materialize_ttl_after_modify': 1}))
        n_mid, total_mid = prod.ch('select count(), sum(amount) from events')[0]
        steps[-1]['check_after'] = _check(n_mid, total_mid, truth)['mismatch']
        step('OPTIMIZE FINAL', lambda: prod.ch('OPTIMIZE TABLE events FINAL'))
    n, total = prod.ch('select count(), sum(amount) from events')[0]
    inactive = prod.ch("select count(), sum(bytes_on_disk) from system.parts where table = 'events' and not active")[0]
    return {'versions': ver, 'bytes_before': before, 'steps': steps, 'bytes_final': size(),
            'inactive_parts': int(inactive[0]), 'inactive_bytes': int(inactive[1] or 0), 'check': _check(n, total, truth)}


def retain_duck(method, truth, work):
    con = prod.duck(tmp=work)
    src = f"read_csv('{gen.paths(SCALE)['csv']}', header=true, columns={Q.duck_columns()})"
    steps = []
    if method == 'files_per_day':
        d = os.path.join(work, 'days')
        con.execute(f"COPY (SELECT *, CAST(ts AS DATE) AS day FROM {src}) TO '{d}' (FORMAT parquet, PARTITION_BY (day))")
        before = prod.du(d)
        days = [gen.ts_str(gen.BASE_TS + i * 86400)[:10] for i in range(KEEP_FROM_DAY)]
        ms, _ = prod.timed(lambda: [shutil.rmtree(os.path.join(d, f'day={x}')) for x in days])
        steps.append({'step': f'日のディレクトリを消す × {len(days)}', 'ms': ms, 'bytes_after': prod.du(d)})
        n, total = con.execute(f"select count(*), sum(amount) from read_parquet('{d}/*/*.parquet')").fetchone()
        final = prod.du(d)
    elif method == 'single_file_rewrite':
        pq = os.path.join(work, 'events.parquet')
        con.execute(f"COPY (SELECT * FROM {src}) TO '{pq}' (FORMAT parquet)")
        before = os.path.getsize(pq)

        def rewrite():
            con.execute(f"COPY (SELECT * FROM read_parquet('{pq}') WHERE ts >= '{CUTOFF}') TO '{pq}.new' (FORMAT parquet)")
            os.replace(pq + '.new', pq)
        ms, _ = prod.timed(rewrite)
        steps.append({'step': '残す行を新しいファイルに書いて置き換える', 'ms': ms, 'bytes_after': os.path.getsize(pq)})
        n, total = con.execute(f"select count(*), sum(amount) from read_parquet('{pq}')").fetchone()
        final = os.path.getsize(pq)
    else:
        con.close()
        db = os.path.join(work, 'events.duckdb')
        con = prod.duck(db, tmp=work)
        con.execute(f'CREATE TABLE events AS SELECT * FROM {src}')
        con.execute('CHECKPOINT')
        before = os.path.getsize(db)
        for name, sql in (('DELETE', f"DELETE FROM events WHERE ts < '{CUTOFF}'"), ('CHECKPOINT', 'CHECKPOINT')):
            ms, _ = prod.timed(lambda: con.execute(sql))
            steps.append({'step': name, 'ms': ms, 'bytes_after': os.path.getsize(db)})
        n, total = con.execute('select count(*), sum(amount) from events').fetchone()
        final = os.path.getsize(db)
    con.close()
    return {'versions': {'duckdb': prod.versions_client()['duckdb']}, 'bytes_before': before, 'steps': steps,
            'bytes_final': final, 'check': _check(n, total, truth)}


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    truth = gen.ensure(SCALE, C.SCALES[SCALE], s['seed'])
    try:
        if s['part'] == 'compress':
            body = run_compress(s, truth)
            mism = sum(q['mismatch'] for q in body['queries'].values())
            brief = f'bytes={body["bytes"] / 1e6:.1f}MB write={body["write_ms"]:.0f}ms mismatch={mism}'
        else:
            if s['product'] == 'pg':
                body = retain_pg(s['method'], truth)
            elif s['product'] == 'ch':
                body = retain_ch(s['method'], truth)
            else:
                body = retain_duck(s['method'], truth, prod.workdir(run_id_of(s)))
            mism = body['check']['mismatch']
            body['total_ms'] = round(sum(x['ms'] for x in body['steps']), 3)
            brief = (f'{body["bytes_before"] / 1e6:.1f}MB -> {body["bytes_final"] / 1e6:.1f}MB '
                     f'steps={[(x["step"][:14], round(x["ms"])) for x in body["steps"]]} mismatch={mism}')
        cmem = prod.mem()
    finally:
        prod.down()
        prod.clean_work()
    return {'run_id': run_id_of(s), **s, 'n_rows': truth['n'], 'csv_bytes': truth['csv_bytes'], 'reps': C.REPS,
            'mismatch_total': mism, 'container_memory': cmem, 'seconds': round(time.monotonic() - t0, 1), **body, 'brief': brief}
