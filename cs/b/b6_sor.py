"""B-6: replacing the system of record (PostgreSQL -> another PostgreSQL instance)
under a running load, in the six stages of the guide: run in parallel, backfill,
reconcile, switch reads, switch writes (with a rollback window), remove the old one.

  n            dual write by the application (old, then new, two separate operations),
               backfill that overwrites, no reconciliation gate, route flip without fencing
  n_vbackfill  the same with a backfill that only writes a row when its version is newer
  be           old stays the system of record; new is built by logical replication
               (initial copy = backfill), reconcile before each switch, fenced write
               switch (revoke on old, wait for new to catch up, advance the sequence),
               reverse replication for the rollback window
  be_noseq     BE without advancing the sequence
  be_norev     BE without reverse replication
  be_nofence   BE without the fence (the two application groups switch 0.7 s apart)

Scenario 'complete' ends on the new instance; 'rollback' switches the writes back to the
old instance after 300 acknowledged writes on the new one. A fault is injected in the
parallel stage: the new instance is restarted once (after 300 acknowledged writes).

Two application groups (half of the threads each) change their routing 0.7 s apart
(a rolling deploy: old and new application versions are mixed).
"""
import random
import time

import psycopg

from ..common import dockerutil as D
from . import config as C, infra
from .load import Load, by_phase, window

PRELOAD = 20000
HOT = 800                 # preloaded rows that the writers update
TRIALS = 5
GAP_S = 0.7
APP_TIMEOUT_MS = 3000
CASES = [('n', 'complete'), ('n_vbackfill', 'complete'), ('be', 'complete'), ('be_noseq', 'complete'),
         ('be_nofence', 'complete'), ('n', 'rollback'), ('be', 'rollback'), ('be_norev', 'rollback')]
CONFIG_JA = {'n': 'N アプリの二重書き込み＋上書きの埋め戻し', 'n_vbackfill': 'N＋版を比べる埋め戻し',
             'be': 'BE 論理レプリケーション＋照合＋遮断つき切り替え＋逆向きの複製', 'be_noseq': 'BE − シーケンスを進める',
             'be_norev': 'BE − 逆向きの複製（切り戻しの窓）', 'be_nofence': 'BE − 遮断（旧への書き込みを止めずに切り替える）'}
SCENARIO_JA = {'complete': '新へ移し切る', 'rollback': '書きを切り替えた後で旧へ切り戻す'}

DDL = ('create table orders (id bigserial primary key, ext text not null unique, status text not null, '
       'ver int not null)')


def plan():
    return [{'exp': 'b6', 'config': c, 'scenario': s, 'trial': t, 'seed': C.SEED0 + t}
            for c, s in CASES for t in range(1, TRIALS + 1)]


def run_id_of(s):
    return f'b6:{s["config"]}:{s["scenario"]}:t{s["trial"]}'


def status_of(ext, ver):
    return C.STATUSES[(sum(ext.encode()) + ver) % len(C.STATUSES)]


class RowMissing(RuntimeError):
    pass


class Conns:
    """The two connections of one application thread (role ``app``)."""

    def __init__(self):
        self.c = {'old': None, 'new': None}
        self.timeout_ms = None

    def get(self, side):
        if self.c[side] is None:
            c = psycopg.connect(host='127.0.0.1', port=C.PORTS['pg' if side == 'old' else 'pg2'], user='app',
                                password=C.PASSWORD, dbname='postgres', autocommit=True, connect_timeout=2)
            if self.timeout_ms:
                c.execute(f'set statement_timeout = {self.timeout_ms}')
            self.c[side] = c
        return self.c[side]

    def execute(self, sql):            # called by Load for "set statement_timeout = N"
        self.timeout_ms = int(sql.rsplit('=', 1)[1])

    def drop(self, side):
        try:
            if self.c[side] is not None:
                self.c[side].close()
        except Exception:  # noqa: BLE001
            pass
        self.c[side] = None

    def close(self):
        self.drop('old')
        self.drop('new')


def _write(conn, op, ext, ver, explicit_id=None):
    st = status_of(ext, ver)
    if op == 'insert':
        if explicit_id is None:
            return conn.execute('insert into orders (ext, status, ver) values (%s, %s, %s) returning id',
                                (ext, st, ver)).fetchone()[0]
        conn.execute('insert into orders (id, ext, status, ver) values (%s, %s, %s, %s)', (explicit_id, ext, st, ver))
        return explicit_id
    return conn.execute('update orders set status = %s, ver = %s where ext = %s', (st, ver, ext)).rowcount


def make_request(routes, shared, n_writers=4):
    def request(kind, conns, ctx):
        rng = ctx.setdefault('rng', random.Random(f'{ctx["seed"]}:{ctx["thread"]}'))
        route = routes[ctx['group']]
        if kind == 'r':
            keys = shared['keys']
            ext = keys[rng.randrange(len(keys))]
            before = shared['acked'].get(ext, 0)
            row = conns.get(route['read']).execute('select status, ver from orders where ext = %s', (ext,)).fetchone()
            # wrong: the row is absent, older than a write that was acknowledged before the read
            # started, or carries a status the generator never wrote for that version
            return row is None or row[1] < before or (row[1] > 0 and row[0] != status_of(ext, row[1]))
        own = ctx.setdefault('own', [f'p-{i}' for i in range(1, HOT + 1) if i % n_writers == ctx['thread']])
        att = ctx.setdefault('attempted', {})
        if rng.random() < 0.3:
            ctx['n_ins'] = ctx.get('n_ins', 0) + 1
            op, ext, ver = 'insert', f'w{ctx["thread"]}-{ctx["n_ins"]}', 1
        else:
            ext = rng.choice(own)
            op, ver = 'update', max(att.get(ext, 0), shared['acked'].get(ext, 0)) + 1
        att[ext] = ver
        target = route['write']
        first = 'new' if target == 'new' else 'old'
        r = _write(conns.get(first), op, ext, ver)
        if op == 'update' and r != 1:
            raise RowMissing(ext)
        if target == 'dual':            # second, separate operation; a failure is logged and the request succeeds
            try:
                r2 = _write(conns.get('new'), op, ext, ver, explicit_id=r if op == 'insert' else None)
                if op == 'update' and r2 != 1:
                    shared['dual_new_missed'] += 1
            except Exception:  # noqa: BLE001
                shared['dual_new_failed'] += 1
                conns.drop('new')
        shared['acked'][ext] = ver
        shared['acked_on'][ext] = first if target != 'dual' else 'dual'
        if op == 'insert':
            own.append(ext)
            shared['keys'].append(ext)
        return False
    return request


def table_rows(which):
    return {r[0]: (r[1], r[2]) for r in infra.q(which, 'select ext, status, ver from orders')}


def stale_in(target_rows, source_rows):
    """Rows of ``source`` that are missing in ``target`` or older there: {ext: target version or None}."""
    out = {}
    for ext, (_st, ver) in source_rows.items():
        got = target_rows.get(ext)
        if got is None or got[1] < ver:
            out[ext] = None if got is None else got[1]
    return out


def persistent_diff(d1, d2):
    """Reconciliation under load: two diffs taken some time apart; a row counts when it is
    in both with the same target version (it is not just in flight)."""
    return sorted(k for k, v in d1.items() if k in d2 and d2[k] == v)


def diff_now(target, source, wait_s=0.5):
    w_t, w_s = ('pg2', 'pg') if target == 'new' else ('pg', 'pg2')
    d1 = stale_in(table_rows(w_t), table_rows(w_s))
    time.sleep(wait_s)
    d2 = stale_in(table_rows(w_t), table_rows(w_s))
    return len(persistent_diff(d1, d2))


def judge_sor(rows, acked, attempted):
    """Final state of an instance against the load's own record.
    rows {ext: (status, ver)}; acked {ext: last acknowledged version}; attempted {ext: last attempted version}"""
    out = {'acked_keys': len(acked), 'missing': 0, 'lost_acked_write': 0, 'unexpected_version': 0, 'value_mismatch': 0}
    for ext, ver in acked.items():
        got = rows.get(ext)
        if got is None:
            out['missing'] += 1
            continue
        if got[1] < ver:
            out['lost_acked_write'] += 1
        elif got[1] not in (ver, attempted.get(ext, ver)):
            out['unexpected_version'] += 1
        if got[1] > 0 and got[0] != status_of(ext, got[1]):      # version 0 is the preloaded row
            out['value_mismatch'] += 1
    return out


def _wait_equal(timeout=30):
    """Both instances hold the same rows (count, sum of versions, largest id)."""
    t0 = time.monotonic()
    sql = 'select count(*), coalesce(sum(ver), 0), coalesce(max(id), 0) from orders'
    while time.monotonic() - t0 < timeout:
        if infra.q('pg', sql) == infra.q('pg2', sql):
            return round(time.monotonic() - t0, 3)
        time.sleep(0.05)
    return None


def _flip(ld, routes, key, value, label):
    ld.phase = label
    for g in range(len(routes)):
        routes[g][key] = value
        if g < len(routes) - 1:
            time.sleep(GAP_S)


def _conninfo(host):
    return f'host={host} port=5432 user=postgres password={C.PASSWORD} dbname=postgres'


def run_one(spec, log=print):
    t_run = time.monotonic()
    cfg, scenario, seed = spec['config'], spec['scenario'], spec['seed']
    is_n = cfg.startswith('n')
    infra.down()
    pg_args = ['-c', 'wal_level=logical', '-c', 'max_wal_senders=4', '-c', 'max_replication_slots=4']
    infra.pg_up('pg', args=pg_args, wait=False)
    infra.pg_up('pg2', args=pg_args, wait=False)
    infra.pg_wait('pg')
    infra.pg_wait('pg2')
    for which in ('pg', 'pg2'):
        with infra.pg(which) as c:
            c.execute(f"create role app login password '{C.PASSWORD}'")
            c.execute(DDL)                                   # the schema is created by hand on both sides
            c.execute('grant select on orders to app')
    with infra.pg('pg') as c:
        c.execute("insert into orders (ext, status, ver) select 'p-' || g, 'accepted', 0 from generate_series(1, %s) g",
                  (PRELOAD,))
        c.execute('grant insert, update, delete on orders to app')
        c.execute('grant usage on sequence orders_id_seq to app')
    if is_n:
        with infra.pg('pg2') as c:
            c.execute('grant insert, update, delete on orders to app')
            c.execute('grant usage on sequence orders_id_seq to app')
    routes = [{'write': 'old', 'read': 'old'}, {'write': 'old', 'read': 'old'}]
    shared = {'acked': {}, 'acked_on': {}, 'keys': [f'p-{i}' for i in range(1, HOT + 1)],
              'dual_new_failed': 0, 'dual_new_missed': 0}
    ld = Load(lambda _ctx: Conns(), make_request(routes, shared), timeout_ms=APP_TIMEOUT_MS)
    for ctx in ld.ctxs:
        ctx['seed'] = seed
    ld.phase = '0_old_only'
    ld.start()
    time.sleep(1.0)
    marks, steps = {}, {}

    # ---- stage 1: run in parallel (+ the fault: restart the new instance once)
    marks['parallel'] = time.monotonic()
    if is_n:
        _flip(ld, routes, 'write', 'dual', '1_parallel')
    else:
        ld.phase = '1_parallel'
        infra.q('pg', 'create publication pub for table orders')
        infra.q('pg2', f"create subscription sub connection '{_conninfo(C.PG)}' publication pub")
    ld.wait_ops(300)
    t0 = time.monotonic()
    D.docker('restart', '-t', '1', C.PG2)
    infra.pg_wait('pg2')
    steps['new_restart_s'] = round(time.monotonic() - t0, 2)
    ld.wait_ops(300)

    # ---- stage 2: backfill
    ld.phase = '2_backfill'
    t0 = time.monotonic()
    if is_n:
        steps['diff_before_backfill'] = diff_now('new', 'old')
        guard = 'where orders.ver < excluded.ver' if cfg == 'n_vbackfill' else ''
        with infra.pg('pg') as src, infra.pg('pg2') as dst:
            last = 0
            while True:
                batch = src.execute('select id, ext, status, ver from orders where id > %s order by id limit 500',
                                    (last,)).fetchall()
                if not batch:
                    break
                last = batch[-1][0]
                dst.cursor().executemany(
                    'insert into orders (id, ext, status, ver) values (%s, %s, %s, %s) on conflict (id) do update '
                    f'set status = excluded.status, ver = excluded.ver {guard}', batch)
    else:
        D.wait_ready(C.PG2, lambda: infra.q('pg2', "select count(*) filter (where srsubstate <> 'r') = 0 and count(*) > 0 "
                                                   'from pg_subscription_rel')[0][0], timeout=120, interval=0.1)
    steps['backfill_s'] = round(time.monotonic() - t0, 2)
    ld.wait_ops(300)

    # ---- stage 3: reconcile
    ld.phase = '3_reconcile'
    steps['diff_after_backfill'] = diff_now('new', 'old')
    if not is_n:
        tries = 0
        while steps['diff_after_backfill'] and tries < 20:      # gate: do not go on while new is behind
            tries += 1
            steps['diff_after_backfill'] = diff_now('new', 'old')
        steps['reconcile_retries'] = tries
    steps['sequence_before_switch'] = {w: infra.q(w, 'select last_value from orders_id_seq')[0][0] for w in ('pg', 'pg2')}
    steps['max_id_before_switch'] = {w: infra.q(w, 'select max(id) from orders')[0][0] for w in ('pg', 'pg2')}

    # ---- stage 4: switch reads
    _flip(ld, routes, 'read', 'new', '4_read_new')
    ld.wait_ops(300)

    # ---- stage 5: switch writes
    marks['switch'] = time.monotonic()
    if is_n:
        _flip(ld, routes, 'write', 'new', '5_write_new')
    else:
        ld.phase = '5_write_new'
        if cfg != 'be_nofence':
            infra.q('pg', 'revoke insert, update, delete on orders from app')       # fence the old instance
            steps['catch_up_s'] = _wait_equal()
            infra.q('pg2', 'alter subscription sub disable')
        if cfg != 'be_noseq':
            infra.q('pg2', "select setval('orders_id_seq', (select max(id) from orders))")
        if cfg not in ('be_norev',):
            infra.q('pg2', 'create publication pub_rev for table orders')
            infra.q('pg', f"create subscription sub_rev connection '{_conninfo(C.PG2)}' publication pub_rev "
                          'with (copy_data = false, origin = none)')
        with infra.pg('pg2') as c:
            c.execute('grant insert, update, delete on orders to app')
            c.execute('grant usage on sequence orders_id_seq to app')
        for g in range(len(routes)):
            routes[g]['write'] = 'new'
            if g < len(routes) - 1:
                time.sleep(GAP_S)
        if cfg == 'be_nofence':
            time.sleep(0.5)
            infra.q('pg2', 'alter subscription sub disable')
    marks['switched'] = time.monotonic()
    ld.wait_ops(300, timeout=8)
    sub_state = None
    if not is_n:
        sub_state = infra.q('pg2', "select coalesce(sum(apply_error_count), 0) from pg_stat_subscription_stats")[0][0]

    # ---- stage 6: rollback window / remove the old instance
    final_side = 'new'
    if scenario == 'rollback':
        marks['rollback'] = time.monotonic()
        ld.phase = '6_rollback'
        if not is_n:
            infra.q('pg2', 'revoke insert, update, delete on orders from app')       # fence the new instance
            if cfg != 'be_norev':
                steps['rollback_catch_up_s'] = _wait_equal()
                infra.q('pg', 'alter subscription sub_rev disable')
            infra.q('pg', "select setval('orders_id_seq', (select max(id) from orders))")
            infra.q('pg', 'grant insert, update, delete on orders to app')
        for key in ('write', 'read'):
            for g in range(len(routes)):
                routes[g][key] = 'old'
        marks['rolled_back'] = time.monotonic()
        ld.wait_ops(300, timeout=8)
        final_side = 'old'
    else:
        ld.phase = '6_old_removed'
        ld.wait_ops(200, timeout=5)
    ops = ld.stop()
    attempted = {}
    for ctx in ld.ctxs:
        attempted.update(ctx.get('attempted', {}))
    acked = dict(shared['acked'])
    final = judge_sor(table_rows('pg' if final_side == 'old' else 'pg2'), acked, attempted)
    other = judge_sor(table_rows('pg2' if final_side == 'old' else 'pg'), acked, attempted)
    t_end = max(o.t1 for o in ops)
    sw_end = marks['switched'] + 1.0
    row = {'run_id': run_id_of(spec), **spec, 'preload': PRELOAD, 'steps': steps,
           'dual_new_failed': shared['dual_new_failed'], 'dual_new_missed': shared['dual_new_missed'],
           'write_by_phase': by_phase(ops, 'w'), 'read_by_phase': by_phase(ops, 'r'),
           'write_all': window(ops, 'w', marks['parallel'], t_end), 'read_all': window(ops, 'r', marks['parallel'], t_end),
           'write_switch': window(ops, 'w', marks['switch'], sw_end),
           'final_side': final_side, 'final': final, 'other_side': other,
           'acked_on_new_only': sum(1 for v in shared['acked_on'].values() if v == 'new'),
           'subscription_apply_errors': None if sub_state is None else int(sub_state),
           'versions': {'pg': infra.q('pg', 'show server_version')[0][0]}, 'container_memory': infra.mem()}
    if 'rollback' in marks:
        row['write_rollback'] = window(ops, 'w', marks['rollback'], marks['rolled_back'] + 1.0)
    wa, ra = row['write_all'], row['read_all']
    row['brief'] = (f'wfail={wa["failed"]} {wa["errors"]} rwrong={ra["wrong"]} rfail={ra["failed"]} '
                    f'switch_gap={row["write_switch"]["max_gap_ms"]} final[{final_side}]={final} steps={steps}')
    infra.down()
    row['seconds'] = round(time.monotonic() - t_run, 1)
    return row
