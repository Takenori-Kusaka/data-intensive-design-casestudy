"""C-4: running the same batch again. What is in the table afterwards?

Table: events of three days (day 1 is already there). The job loads the events of day 2.
  scenario  same       the job runs twice with the same input
            corrected  the second run reads a corrected input (CHANGED amounts changed,
                       REMOVED rows gone, ADDED rows new)
            partial    the first run stopped after half of the rows was committed
  After the re-run, the batch of day 3 is loaded once by the same strategy (normal
  operation: a new batch must go in, and must not be taken for a duplicate).

The expected table comes from the generator ({event_id: amount} of day 1, the last
version of day 2, and day 3). judges.judge_table counts duplicates, missing, stale and
extra rows in what a reader gets with SELECT event_id, amount.

Strategies
  pg    insert (no key) / insert_pk / on_conflict_nothing / upsert / delete_insert /
        now_window (the range is "the last 24 hours before now", plain INSERT: the bad
        example of the guide; scenario same only, the second run is 6 hours later)
  ch    insert / insert_dedup (non_replicated_deduplication_window) / replacing
        (ReplacingMergeTree; read as is, with FINAL, after OPTIMIZE FINAL) /
        replace_partition (staging table, then REPLACE PARTITION) / delete_insert
  duck  append_file (a new Parquet file per run) / overwrite_file (one file per day,
        replaced by rename) / insert_or_replace (table with a key) / delete_insert
"""
import os
import random
import time

from . import config as C, gen, judges, prod

DAY_ROWS = 20_000
DAYS = ['2026-10-01', '2026-10-02', '2026-10-03']
CHANGED, REMOVED, ADDED = 200, 100, 150
SCENARIOS = ['same', 'corrected', 'partial']
SCENARIO_JA = {'same': '同じ入力で 2 回', 'corrected': '2 回目は訂正後の入力', 'partial': '1 回目が半分で止まり、流し直す'}
STRATEGIES = {
    'pg': ['insert', 'insert_pk', 'on_conflict_nothing', 'upsert', 'delete_insert', 'now_window'],
    'ch': ['insert', 'insert_dedup', 'replacing', 'replace_partition', 'delete_insert'],
    'duck': ['append_file', 'overwrite_file', 'insert_or_replace', 'delete_insert'],
}
STRATEGY_JA = {
    'pg:insert': 'INSERT（キーなし）', 'pg:insert_pk': 'INSERT（主キーあり）',
    'pg:on_conflict_nothing': 'INSERT … ON CONFLICT DO NOTHING', 'pg:upsert': 'INSERT … ON CONFLICT DO UPDATE（UPSERT）',
    'pg:delete_insert': '日の範囲を DELETE して INSERT（1 トランザクション）',
    'pg:now_window': '範囲を now() から決めて INSERT（ガイドの悪い例）',
    'ch:insert': 'INSERT（MergeTree、既定）', 'ch:insert_dedup': 'INSERT（non_replicated_deduplication_window=100）',
    'ch:replacing': 'ReplacingMergeTree に INSERT', 'ch:replace_partition': '仮の表に入れて REPLACE PARTITION',
    'ch:delete_insert': 'ALTER DELETE（同期）して INSERT',
    'duck:append_file': '実行ごとに新しい Parquet を足す', 'duck:overwrite_file': '日ごとの Parquet を置き換える（rename）',
    'duck:insert_or_replace': 'INSERT OR REPLACE（キーつきの表）', 'duck:delete_insert': '日の範囲を DELETE して INSERT（1 トランザクション）',
}
TRIALS = {'pg': 1, 'ch': 3, 'duck': 1}      # only ClickHouse depends on background merges


def plan():
    out = []
    for p, sts in STRATEGIES.items():
        for st in sts:
            for sc in SCENARIOS:
                if st == 'now_window' and sc != 'same':
                    continue
                for t in range(1, TRIALS[p] + 1):
                    out.append({'exp': 'c4', 'product': p, 'strategy': st, 'scenario': sc, 'trial': t, 'seed': C.SEED0 + t})
    return out


def run_id_of(s):
    return f'c4:{s["product"]}:{s["strategy"]}:{s["scenario"]}:t{s["trial"]}'


def make_batches(seed):
    """Rows are (event_id, day, ts, store_id, amount). Returns the three days, the corrected
    day 2 and the half of day 2 a stopped run committed."""
    cols = gen.chunk(seed + 200, 0, 3 * DAY_ROWS + ADDED)
    rng = random.Random(seed)
    days = []
    for d in range(3):
        rows = []
        for i in range(d * DAY_ROWS, (d + 1) * DAY_ROWS):
            sec = (i - d * DAY_ROWS) * 86400 // DAY_ROWS
            rows.append((int(cols['event_id'][i]), DAYS[d], f'{DAYS[d]} {sec // 3600:02d}:{sec % 3600 // 60:02d}:{sec % 60:02d}',
                         int(cols['store_id'][i]), int(cols['amount'][i])))
        days.append(rows)
    idx = list(range(DAY_ROWS))
    rng.shuffle(idx)
    changed, removed = set(idx[:CHANGED]), set(idx[CHANGED:CHANGED + REMOVED])
    v2 = []
    for j, r in enumerate(days[1]):
        if j in removed:
            continue
        v2.append((r[0], r[1], r[2], r[3], r[4] + 1000) if j in changed else r)
    for k in range(ADDED):
        i = 3 * DAY_ROWS + k
        v2.append((int(cols['event_id'][i]) + 1_000_000, DAYS[1], f'{DAYS[1]} 23:59:{k % 60:02d}', int(cols['store_id'][i]),
                   int(cols['amount'][i])))
    return {'days': days, 'day2_v2': v2, 'day2_half': days[1][:DAY_ROWS // 2]}


def runs_of(scenario, b):
    """The inputs of the runs of the day-2 job, and the day-2 rows the table should end with."""
    if scenario == 'same':
        return [b['days'][1], b['days'][1]], b['days'][1]
    if scenario == 'corrected':
        return [b['days'][1], b['day2_v2']], b['day2_v2']
    return [b['day2_half'], b['days'][1]], b['days'][1]


def _vals(rows):
    return ', '.join(f"({r[0]}, '{r[1]}', '{r[2]}', {r[3]}, {r[4]})" for r in rows)


# ------------------------------------------------------------------ PostgreSQL
def pg_strategy(st):
    q = prod.pgq
    pk = ', PRIMARY KEY (event_id)' if st not in ('insert', 'now_window', 'delete_insert') else ''
    q(f'CREATE TABLE t (event_id bigint NOT NULL, day date NOT NULL, ts timestamp NOT NULL, store_id int NOT NULL, '
      f'amount int NOT NULL{pk})')
    tail = {'on_conflict_nothing': ' ON CONFLICT (event_id) DO NOTHING',
            'upsert': ' ON CONFLICT (event_id) DO UPDATE SET day = EXCLUDED.day, ts = EXCLUDED.ts, '
                      'store_id = EXCLUDED.store_id, amount = EXCLUDED.amount'}.get(st, '')

    def load(rows, day):
        with prod.pg(autocommit=False) as c:
            if st == 'delete_insert':
                c.execute('DELETE FROM t WHERE day = %s', (day,))
            c.execute(f'INSERT INTO t VALUES {_vals(rows)}{tail}')
            c.commit()
    return load, lambda: q('SELECT event_id, amount FROM t'), {}


def pg_now_window(b):
    """Source table with all three days; the job copies "the last 24 hours before now"."""
    q = prod.pgq
    q('CREATE TABLE src (event_id bigint, day date, ts timestamp, store_id int, amount int)')
    q('CREATE TABLE t (event_id bigint NOT NULL, day date NOT NULL, ts timestamp NOT NULL, store_id int NOT NULL, amount int NOT NULL)')
    for rows in b['days']:
        q(f'INSERT INTO src VALUES {_vals(rows)}')
    job = "INSERT INTO t SELECT * FROM src WHERE ts >= %s::timestamp - interval '1 day' AND ts < %s::timestamp"
    return lambda now: q(job, (now, now))


# ------------------------------------------------------------------ ClickHouse
def ch_strategy(st):
    ch = prod.ch
    cols = 'event_id Int64, day Date, ts DateTime, store_id Int32, amount Int32'
    engine = {'replacing': 'ReplacingMergeTree ORDER BY event_id',
              'insert_dedup': 'MergeTree ORDER BY event_id SETTINGS non_replicated_deduplication_window = 100',
              'replace_partition': 'MergeTree PARTITION BY day ORDER BY event_id'}.get(st, 'MergeTree ORDER BY event_id')
    ch(f'CREATE TABLE t ({cols}) ENGINE = {engine}')
    if st == 'replace_partition':
        ch(f'CREATE TABLE stage ({cols}) ENGINE = MergeTree PARTITION BY day ORDER BY event_id')
    sync = {'async_insert': 0}

    def load(rows, day):
        if st == 'replace_partition':
            ch('TRUNCATE TABLE stage')
            ch(f'INSERT INTO stage VALUES {_vals(rows)}', sync)
            ch(f"ALTER TABLE t REPLACE PARTITION '{day}' FROM stage")
            return
        if st == 'delete_insert':
            ch(f"ALTER TABLE t DELETE WHERE day = '{day}'", {'mutations_sync': 2})
        ch(f'INSERT INTO t VALUES {_vals(rows)}', sync)
    extra = {}
    if st == 'replacing':
        extra = {'final': lambda: ch('SELECT event_id, amount FROM t FINAL'),
                 'optimized': lambda: (ch('OPTIMIZE TABLE t FINAL'), ch('SELECT event_id, amount FROM t'))[1]}
    return load, lambda: ch('SELECT event_id, amount FROM t'), extra


# ---------------------------------------------------------------------- DuckDB
def duck_strategy(st, con, work):
    counter = [0]
    if st in ('append_file', 'overwrite_file'):
        d = os.path.join(work, 'lake')
        os.makedirs(d)

        def load(rows, day):
            counter[0] += 1
            name = f'{day}_run{counter[0]}.parquet' if st == 'append_file' else f'{day}.parquet'
            tmp = os.path.join(work, 'tmp.parquet')
            con.execute(f"COPY (SELECT * FROM (VALUES {_vals(rows)}) v(event_id, day, ts, store_id, amount)) "
                        f"TO '{tmp}' (FORMAT parquet)")
            os.replace(tmp, os.path.join(d, name))
        return load, lambda: con.execute(f"SELECT event_id, amount FROM read_parquet('{d}/*.parquet')").fetchall(), {}
    pk = ' PRIMARY KEY' if st == 'insert_or_replace' else ''
    con.execute(f'CREATE TABLE t (event_id BIGINT{pk}, day DATE, ts TIMESTAMP, store_id INTEGER, amount INTEGER)')

    def load(rows, day):
        con.execute('BEGIN')
        try:
            if st == 'delete_insert':
                con.execute(f"DELETE FROM t WHERE day = '{day}'")
            con.execute(f"INSERT {'OR REPLACE ' if st == 'insert_or_replace' else ''}INTO t VALUES {_vals(rows)}")
            con.execute('COMMIT')
        except Exception:
            con.execute('ROLLBACK')
            raise
    return load, lambda: con.execute('SELECT event_id, amount FROM t').fetchall(), {}


def _judge(expected_rows, got):
    return judges.judge_table({r[0]: r[4] for r in expected_rows}, [(int(r[0]), int(r[1])) for r in got])


def run_one(s, log=print):
    t0 = time.monotonic()
    prod.down()
    b = make_batches(s['seed'])
    p, st, sc = s['product'], s['strategy'], s['scenario']
    con, ver, errors, extra_judges = None, {}, [], {}
    try:
        if p == 'pg':
            ver = {'pg': prod.pg_up()}
        elif p == 'ch':
            ver = {'clickhouse': prod.ch_up()}
        else:
            work = prod.workdir(run_id_of(s))
            con = prod.duck(tmp=work)
            ver = {'duckdb': prod.versions_client()['duckdb']}
        if st == 'now_window':
            job = pg_now_window(b)
            # day 1 is loaded by the job run at the end of day 1; day 2 at the end of day 2,
            # and again 6 hours later
            for now in ('2026-10-02 00:00:00', '2026-10-03 00:00:00', '2026-10-03 06:00:00'):
                job(now)
            expected = b['days'][0] + b['days'][1]
            got = prod.pgq('SELECT event_id, amount FROM t')
            after_rerun = _judge(expected, got)
            job('2026-10-04 00:00:00')              # the normal run of day 3
            got_final = prod.pgq('SELECT event_id, amount FROM t')
            run_errors = []
        else:
            load, read, extra = (pg_strategy(st) if p == 'pg' else ch_strategy(st) if p == 'ch'
                                 else duck_strategy(st, con, work))
            load(b['days'][0], DAYS[0])
            inputs, day2 = runs_of(sc, b)
            run_errors = []
            for i, rows in enumerate(inputs, 1):
                try:
                    load(rows, DAYS[1])
                    run_errors.append(None)
                except Exception as e:  # noqa: BLE001 - a failing re-run is a result
                    run_errors.append(' '.join(str(e).replace(C.DATA, '<data>').split())[:200])
            expected = b['days'][0] + day2
            after_rerun = _judge(expected, read())
            try:
                load(b['days'][2], DAYS[2])
                run_errors.append(None)
            except Exception as e:  # noqa: BLE001
                run_errors.append(' '.join(str(e).split())[:200])
            got_final = read()
            # other ways to read the same table, tried last (OPTIMIZE changes the table)
            for k, fn in extra.items():
                extra_judges[k] = _judge(expected + b['days'][2], fn())
        final = _judge(expected + b['days'][2], got_final)
        ids = {int(r[0]) for r in got_final}
        day3_missing = sum(1 for r in b['days'][2] if r[0] not in ids)
        cmem = prod.mem()
    finally:
        if con is not None:
            con.close()
        prod.down()
        prod.clean_work()
    return {'run_id': run_id_of(s), **s, 'day_rows': DAY_ROWS, 'changed': CHANGED, 'removed': REMOVED, 'added': ADDED,
            'versions': ver, 'run_errors': run_errors, 'after_rerun': after_rerun, 'read_variants': extra_judges,
            'final': final, 'day3_missing': day3_missing,
            'container_memory': cmem, 'seconds': round(time.monotonic() - t0, 1),
            'brief': f'rerun: dup={after_rerun["duplicates"]} missing={after_rerun["missing"]} stale={after_rerun["stale"]} '
                     f'extra={after_rerun["extra"]} errors={sum(1 for e in run_errors if e)} '
                     f'final_wrong={final["wrong"]} day3_missing={day3_missing} variants={ {k: v["wrong"] for k, v in extra_judges.items()} }'}
