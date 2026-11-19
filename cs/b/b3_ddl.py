"""B-3: changing a PostgreSQL schema under a running write load.

  ops    one DDL statement at a time: the lock it takes, whether the table is rewritten,
         and how long writes and reads stop while it runs
  queue  a short DDL behind a long transaction, with and without lock_timeout + retry
  mig    the address split of the guide (address -> postal_code + address_line) with old
         and new application versions mixed: N = one transaction (add, fill, drop) then a
         rolling deploy; BE = expand / migrate / contract; plus two ablations of BE

The load: 4 writer threads and 2 reader threads, one connection each, statement_timeout
3 s (a request that waits longer fails), a failed connection is dropped and reopened.
"""
import random
import threading
import time

from . import config as C, infra
from .load import Load, LockSampler, by_phase, window

ROWS_OPS = 300_000
ROWS_QUEUE = 50_000
ROWS_MIG = 200_000
HOT = 2000                 # ids that the load touches
HOLD_S = 6.0               # how long the long transaction stays open (queue)
APP_TIMEOUT_MS = 3000
TRIALS_OPS, TRIALS_QUEUE, TRIALS_MIG = 3, 5, 5

T = 'orders_t'
# key, label, setup statements, the statement, can run inside a transaction block
OPS = [
    ('add_col', '列の追加（既定値なし）', [], f'alter table {T} add column c1 int', True),
    ('add_col_default', '列の追加（定数の DEFAULT）', [], f'alter table {T} add column c1 int default 0', True),
    ('add_col_volatile', '列の追加（volatile な DEFAULT：random()）', [],
     f'alter table {T} add column c1 double precision default random()', True),
    ('alter_type', '列の型の変更（int → bigint）', [], f'alter table {T} alter column amount type bigint', True),
    ('alter_type_nowrite', '列の型の変更（varchar(20) → text）', [], f'alter table {T} alter column status type text', True),
    ('add_check', 'CHECK 制約の追加', [], f'alter table {T} add constraint ck check (amount >= 0)', True),
    ('add_check_notvalid', 'CHECK 制約の追加（NOT VALID）', [],
     f'alter table {T} add constraint ck check (amount >= 0) not valid', True),
    ('validate_check', 'VALIDATE CONSTRAINT（CHECK）', [f'alter table {T} add constraint ck check (amount >= 0) not valid'],
     f'alter table {T} validate constraint ck', True),
    ('add_fk', '外部キーの追加', [], f'alter table {T} add constraint fk foreign key (customer_id) references customers (id)', True),
    ('add_fk_notvalid', '外部キーの追加（NOT VALID）', [],
     f'alter table {T} add constraint fk foreign key (customer_id) references customers (id) not valid', True),
    ('validate_fk', 'VALIDATE CONSTRAINT（外部キー）',
     [f'alter table {T} add constraint fk foreign key (customer_id) references customers (id) not valid'],
     f'alter table {T} validate constraint fk', True),
    ('set_not_null', 'SET NOT NULL', [], f'alter table {T} alter column address set not null', True),
    ('set_not_null_after_check', 'SET NOT NULL（有効な CHECK (address IS NOT NULL) がある）',
     [f'alter table {T} add constraint ck_nn check (address is not null)'],
     f'alter table {T} alter column address set not null', True),
    ('add_notnull_notvalid', 'NOT NULL 制約の追加（NOT VALID）', [],
     f'alter table {T} add constraint nn not null address not valid', True),
    ('validate_notnull', 'VALIDATE CONSTRAINT（NOT NULL）', [f'alter table {T} add constraint nn not null address not valid'],
     f'alter table {T} validate constraint nn', True),
    ('create_index', '索引の作成', [], f'create index ix_cust on {T} (customer_id)', True),
    ('create_index_conc', '索引の作成（CONCURRENTLY）', [], f'create index concurrently ix_cust on {T} (customer_id)', False),
    ('drop_col', '列の削除', [], f'alter table {T} drop column pad', True),
]
OP_JA = {k: ja for k, ja, *_ in OPS}
QUEUE = [('plain', '長いトランザクションなし'), ('long_txn', '長いトランザクションの後ろ（lock_timeout なし）'),
         ('long_txn_timeout', '長いトランザクションの後ろ（lock_timeout 200 ms＋再試行）')]
QUEUE_JA = dict(QUEUE)
MIG = [('n', 'N：1 つのトランザクションで列の追加・埋め戻し・旧列の削除'), ('be', 'BE：expand／migrate／contract'),
       ('be_onebackfill', 'BE − 分割した埋め戻し（1 文で全行を埋め戻す）'),
       ('be_nodual', 'BE − 両方に書く期間（埋め戻しの後、旧版から新版へ直接切り替える）')]
MIG_JA = dict(MIG)


def plan():
    out = [{'exp': 'ops', 'case': k, 'trial': t, 'seed': C.SEED0 + t} for k, *_ in OPS for t in range(1, TRIALS_OPS + 1)]
    out += [{'exp': 'queue', 'case': k, 'trial': t, 'seed': C.SEED0 + t} for k, _ in QUEUE
            for t in range(1, TRIALS_QUEUE + 1)]
    out += [{'exp': 'mig', 'case': k, 'trial': t, 'seed': C.SEED0 + t} for k, _ in MIG
            for t in range(1, TRIALS_MIG + 1)]
    return out


def run_id_of(s):
    return f'b3:{s["exp"]}:{s["case"]}:t{s["trial"]}'


def _connect(_ctx=None):
    return infra.pg('pg')


def _setup_orders(rows):
    with infra.pg('pg') as c:
        c.execute("create table customers (id int primary key, name text)")
        c.execute("insert into customers select g, 'c' || g from generate_series(1, 1000) g")
        c.execute(f"create table {T} (id int primary key, customer_id int not null, status varchar(20) not null, "
                  "amount int not null, address text, pad text)")
        c.execute(f"insert into {T} select g, 1 + g %% 1000, 'accepted', g %% 1000, 'addr ' || g, repeat('x', 60) "
                  "from generate_series(1, %s) g", (rows,))
        c.execute('vacuum analyze')


def _simple_request(kind, conn, ctx):
    rng = ctx.setdefault('rng', random.Random(f'{ctx["seed"]}:{ctx["thread"]}'))
    i = rng.randrange(1, HOT + 1)
    if kind == 'w':
        conn.execute(f'update {T} set amount = amount + 1 where id = %s', (i,))
    else:
        conn.execute(f'select status, amount from {T} where id = %s', (i,)).fetchone()


def _load(seed, request):
    ld = Load(_connect, request, timeout_ms=APP_TIMEOUT_MS)
    for ctx in ld.ctxs:
        ctx['seed'] = seed
    return ld


def ddl_retry(sql, lock_timeout_ms=200, wait_s=0.3, max_s=60):
    """Run a DDL statement with lock_timeout; on a lock timeout wait and try again."""
    t0, attempts = time.monotonic(), 0
    with infra.pg('pg') as c:
        c.execute(f'set lock_timeout = {int(lock_timeout_ms)}')
        while True:
            attempts += 1
            try:
                c.execute(sql)
                return {'attempts': attempts, 'seconds': round(time.monotonic() - t0, 3)}
            except Exception as e:  # noqa: BLE001
                if getattr(e, 'sqlstate', None) != '55P03' or time.monotonic() - t0 > max_s:
                    raise
                time.sleep(wait_s)


# ----------------------------------------------------------------------- ops
def run_ops(spec):
    key = spec['case']
    _k, _ja, setup, sql, in_txn = next(o for o in OPS if o[0] == key)
    infra.pg_up('pg')
    _setup_orders(ROWS_OPS)
    for s in setup:
        infra.q('pg', s)
    locks_txn = None
    if in_txn:   # the locks held at the end of the statement, read inside the same transaction
        with infra.pg('pg', autocommit=False) as c:
            c.execute(sql)
            locks_txn = sorted({(r[0], r[1]) for r in c.execute(
                "select relation::regclass::text, mode from pg_locks where pid = pg_backend_pid() "
                "and locktype = 'relation' and relation in ('orders_t'::regclass, 'customers'::regclass)").fetchall()})
            c.rollback()
    ld = _load(spec['seed'], _simple_request).start()
    time.sleep(1.5)
    ddl = infra.pg('pg')
    sampler = LockSampler(_connect)
    sampler.pid = ddl.execute('select pg_backend_pid()').fetchone()[0]
    sampler.start()
    node0 = infra.q('pg', f"select pg_relation_filenode('{T}')")[0][0]
    t_a = time.monotonic()
    err = None
    try:
        ddl.execute(sql)
    except Exception as e:  # noqa: BLE001
        err = str(e)[:200]
    t_b = time.monotonic()
    node1 = infra.q('pg', f"select pg_relation_filenode('{T}')")[0][0]
    ddl.close()
    time.sleep(0.5)
    waits = sampler.stop()
    ops = ld.stop()
    sampled = sorted({(r, m) for r, m, _g in sampler.modes if r in (T, 'customers')})
    w, r = window(ops, 'w', t_a, t_b + 0.3), window(ops, 'r', t_a, t_b + 0.3)
    return {'sql': sql, 'setup': setup, 'rows': ROWS_OPS, 'ddl_ms': round((t_b - t_a) * 1000, 1), 'ddl_error': err,
            'locks_in_txn': locks_txn, 'locks_sampled': sampled, 'rewrite': node0 != node1,
            'write': w, 'read': r, 'baseline_write': window(ops, 'w', t_a - 1.2, t_a),
            'baseline_read': window(ops, 'r', t_a - 1.2, t_a), 'lock_waits': waits,
            'brief': f'ddl={round((t_b - t_a) * 1000)}ms rewrite={node0 != node1} wgap={w["max_gap_ms"]} '
                     f'rgap={r["max_gap_ms"]} wfail={w["failed"]} rfail={r["failed"]} locks={locks_txn or sampled}'}


# --------------------------------------------------------------------- queue
def run_queue(spec):
    cond = spec['case']
    infra.pg_up('pg')
    _setup_orders(ROWS_QUEUE)
    ld = _load(spec['seed'], _simple_request).start()
    time.sleep(1.0)
    sampler = LockSampler(_connect).start()
    long_end = [None]

    def long_txn():
        with infra.pg('pg', autocommit=False) as c:
            c.execute(f'select count(*) from {T} where id < 10').fetchone()     # ACCESS SHARE, kept until commit
            time.sleep(HOLD_S)
            c.commit()
        long_end[0] = time.monotonic()
    th = None
    if cond != 'plain':
        th = threading.Thread(target=long_txn, daemon=True)
        th.start()
        time.sleep(0.5)
    sql = f'alter table {T} add column c1 int'
    t_a = time.monotonic()
    if cond == 'long_txn_timeout':
        ddl = ddl_retry(sql, lock_timeout_ms=200, wait_s=0.5)
    else:
        infra.q('pg', sql)
        ddl = {'attempts': 1, 'seconds': round(time.monotonic() - t_a, 3)}
    t_b = time.monotonic()
    if th:
        th.join()
    time.sleep(0.5)
    waits = sampler.stop()
    ops = ld.stop()
    t_end = max(t_b, long_end[0] or t_b) + 0.3
    w, r = window(ops, 'w', t_a, t_end), window(ops, 'r', t_a, t_end)
    return {'sql': sql, 'rows': ROWS_QUEUE, 'hold_s': HOLD_S if cond != 'plain' else 0, 'app_timeout_ms': APP_TIMEOUT_MS,
            'ddl': ddl, 'write': w, 'read': r, 'lock_waits': waits,
            'brief': f'ddl={ddl} wgap={w["max_gap_ms"]} rgap={r["max_gap_ms"]} wfail={w["failed"]} '
                     f'rfail={r["failed"]} waiting={waits["max_waiting_sessions"]}'}


# ----------------------------------------------------------------------- mig
def addr(i, ver):
    """The address the generator writes as version ``ver`` of order ``i``: (postal code, line)."""
    return f'{(i * 7919 + ver * 104729) % 10**7:07d}', f'L{i}-{ver}'


_SQL_POSTAL0 = "lpad(mod(id::bigint * 7919, 10000000)::text, 7, '0')"
_SQL_LINE0 = "'L' || id || '-0'"


def _mig_request_factory(versions, n_writers=4):
    def request(kind, conn, ctx):
        rng = ctx.setdefault('rng', random.Random(f'{ctx["seed"]}:{ctx["thread"]}'))
        v = versions[ctx['group']]
        if kind == 'w':
            own = ctx.setdefault('own', [i for i in range(1, HOT + 1) if i % n_writers == ctx['thread']])
            att, ack = ctx.setdefault('attempted', {}), ctx.setdefault('acked', {})
            i = rng.choice(own)
            ver = att.get(i, 0) + 1
            att[i] = ver
            pc, ln = addr(i, ver)
            if v == 'v1':
                conn.execute('update orders_m set ver = %s, address = %s where id = %s', (ver, f'{pc} {ln}', i))
            elif v in ('v2a', 'v2b'):
                conn.execute('update orders_m set ver = %s, address = %s, postal_code = %s, address_line = %s '
                             'where id = %s', (ver, f'{pc} {ln}', pc, ln, i))
            else:
                conn.execute('update orders_m set ver = %s, postal_code = %s, address_line = %s where id = %s',
                             (ver, pc, ln, i))
            ack[i] = ver
            return False
        i = rng.randrange(1, HOT + 1)
        if v in ('v1', 'v2a'):
            ver, a = conn.execute('select ver, address from orders_m where id = %s', (i,)).fetchone()
            return a != ' '.join(addr(i, ver))
        ver, pc, ln = conn.execute('select ver, postal_code, address_line from orders_m where id = %s', (i,)).fetchone()
        return (pc, ln) != addr(i, ver)
    return request


def judge_migration(rows, acked, attempted, null_new, cold_mismatch):
    """Final state of the migrated table against the generator.
    rows       {id: (ver, postal_code, address_line)} for the ids the load touched
    acked      {id: last version whose write was answered with success}
    attempted  {id: last version that was attempted} (an unanswered write may or may not be there)"""
    out = {'rows': len(rows), 'value_mismatch': 0, 'lost_acked_write': 0, 'unexpected_version': 0,
           'null_new_columns': null_new, 'untouched_rows_mismatch': cold_mismatch}
    for i, (ver, pc, ln) in rows.items():
        if (pc, ln) != addr(i, ver):
            out['value_mismatch'] += 1
        if ver < acked.get(i, 0):
            out['lost_acked_write'] += 1
        elif ver not in (acked.get(i, 0), attempted.get(i, 0)):
            out['unexpected_version'] += 1
    return out


def _rolling(ld, versions, new, label, gap_s=0.7):
    ld.phase = label
    for g in range(len(versions)):
        versions[g] = new
        time.sleep(gap_s)


def run_mig(spec):
    cfg = spec['case']
    infra.pg_up('pg')
    with infra.pg('pg') as c:
        c.execute('create table orders_m (id int primary key, ver int not null, address text not null)')
        c.execute(f"insert into orders_m select id, 0, {_SQL_POSTAL0} || ' ' || {_SQL_LINE0} "
                  "from generate_series(1, %s) id", (ROWS_MIG,))
        c.execute('vacuum analyze')
    versions = ['v1', 'v1']
    ld = _load(spec['seed'], _mig_request_factory(versions))
    ld.phase = 'before'
    ld.start()
    time.sleep(1.0)
    sampler = LockSampler(_connect).start()
    steps = {}
    t_a = time.monotonic()
    fill = ("update orders_m set postal_code = split_part(address, ' ', 1), "
            "address_line = substr(address, position(' ' in address) + 1) where postal_code is null")
    if cfg == 'n':
        ld.phase = 'ddl'
        t0 = time.monotonic()
        with infra.pg('pg', autocommit=False) as c:
            c.execute('alter table orders_m add column postal_code text, add column address_line text')
            c.execute(fill)
            c.execute('alter table orders_m drop column address')
            c.commit()
        steps['one_transaction_s'] = round(time.monotonic() - t0, 3)
        _rolling(ld, versions, 'v3', 'deploy_new')
    else:
        ld.phase = 'expand'
        steps['expand'] = ddl_retry('alter table orders_m add column postal_code text, add column address_line text')
        if cfg != 'be_nodual':
            _rolling(ld, versions, 'v2a', 'deploy_dual_write')
        ld.phase = 'backfill'
        t0 = time.monotonic()
        if cfg == 'be_onebackfill':
            infra.q('pg', fill)
            steps['backfill'] = {'batches': 1, 'seconds': round(time.monotonic() - t0, 3)}
        else:
            n = 0
            with infra.pg('pg') as c:
                for lo in range(1, ROWS_MIG + 1, 2000):
                    c.execute(fill + ' and id between %s and %s', (lo, lo + 1999))
                    n += 1
            steps['backfill'] = {'batches': n, 'seconds': round(time.monotonic() - t0, 3)}
        steps['null_after_backfill'] = infra.q('pg', 'select count(*) from orders_m where postal_code is null')[0][0]
        if cfg != 'be_nodual':
            _rolling(ld, versions, 'v2b', 'deploy_read_new')
        ld.phase = 'relax_old'
        steps['drop_not_null'] = ddl_retry('alter table orders_m alter column address drop not null')
        _rolling(ld, versions, 'v3', 'deploy_new')
        ld.phase = 'contract'
        steps['contract'] = ddl_retry('alter table orders_m drop column address')
    ld.phase = 'after'
    t_b = time.monotonic()
    time.sleep(1.0)
    waits = sampler.stop()
    ops = ld.stop()
    acked, attempted = {}, {}
    for ctx in ld.ctxs:
        acked.update(ctx.get('acked', {}))
        attempted.update(ctx.get('attempted', {}))
    rows = {r[0]: (r[1], r[2], r[3]) for r in infra.q(
        'pg', 'select id, ver, postal_code, address_line from orders_m where id <= %s', (HOT,))}
    null_new = infra.q('pg', 'select count(*) from orders_m where postal_code is null or address_line is null')[0][0]
    cold = infra.q('pg', f'select count(*) from orders_m where ver = 0 and (postal_code is distinct from {_SQL_POSTAL0} '
                         f'or address_line is distinct from {_SQL_LINE0})')[0][0]
    final = judge_migration(rows, acked, attempted, null_new, cold)
    w, r = window(ops, 'w', t_a, t_b), window(ops, 'r', t_a, t_b)
    return {'rows': ROWS_MIG, 'hot_ids': HOT, 'steps': steps, 'write': w, 'read': r,
            'write_by_phase': by_phase(ops, 'w'), 'read_by_phase': by_phase(ops, 'r'), 'final': final,
            'lock_waits': waits, 'migration_s': round(t_b - t_a, 2),
            'brief': f'wfail={w["failed"]} rfail={r["failed"]} rwrong={r["wrong"]} wgap={w["max_gap_ms"]} '
                     f'final={final["value_mismatch"]}/{final["lost_acked_write"]}/{final["null_new_columns"]} '
                     f'mig={round(t_b - t_a, 1)}s'}


def run_one(spec, log=print):
    t0 = time.monotonic()
    infra.down()
    body = {'ops': run_ops, 'queue': run_queue, 'mig': run_mig}[spec['exp']](spec)
    row = {'run_id': run_id_of(spec), **spec, **body, 'versions': {'pg': infra.q('pg', 'show server_version')[0][0]},
           'container_memory': infra.mem()}
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row
