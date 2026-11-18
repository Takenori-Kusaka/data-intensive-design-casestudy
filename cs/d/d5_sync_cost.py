"""D-5: what the zero-loss setting costs.

latency   Commit latency of single-row INSERTs for every (synchronous_commit,
          synchronous_standby_names) pair, with and without an added delay on the
          replication path (Toxiproxy latency toxic, both directions).
stall     synchronous_commit=on with one named standby; the standby is stopped.
          A write is issued and watched for a fixed window. During the window:
          a read-only query, a rolled-back transaction, a second write, a write
          that needs the lock of the waiting transaction, and the wait event.
          Then one way out is tried per scenario:
            standby_back     start the standby again
            reload_empty     set synchronous_standby_names = '' and reload
            cancel           pg_cancel_backend() on the waiting backend
            primary_restart  restart the primary while the commit is waiting
            any1of2          two standbys with 'ANY 1 (dic_s1, dic_s2)': stop one, then both
"""
import threading
import time

from ..common import dockerutil as D
from ..common.record import lat_summary
from . import config as C
from .db import DBError, connect
from .repl import PRIMARY, Cluster

SYNC_COMMITS = ['off', 'local', 'remote_write', 'on', 'remote_apply']
N_LAT = 300
PATH_DELAY_MS = 5            # per direction
LAT_TRIALS = 3
STALL_TRIALS = 3
WINDOW_S = {'standby_back': 60, 'reload_empty': 15, 'cancel': 15, 'primary_restart': 15, 'any1of2': 15}
BASELINE_N = 50


def _timed_inserts(conn, start, n):
    ms = []
    for i in range(start, start + n):
        t0 = time.monotonic()
        conn.q('insert into t (id, v) values (%s, 0)', (i,))
        ms.append((time.monotonic() - t0) * 1000)
    return ms


def run_latency(trial, res, version_box):
    cl = Cluster(n_standby=1)
    try:
        cl.up()
        version_box.append(cl.version())
        cl.p_exec('create table t (id int primary key, v int not null)')
        next_id = 1
        for delay in (0, PATH_DELAY_MS):
            cl.set_latency(delay)
            for names in ('', 'dic_s1'):
                if delay and not names:
                    continue            # the path delay cannot matter when nothing waits for the standby
                for sc in SYNC_COMMITS:
                    rid = f'd5/latency/{sc}/{names or "empty"}/delay{delay}/t{trial}'
                    if res.has(rid):
                        continue
                    cl.set_sync(sc, names)
                    conn = cl.pconn()
                    try:
                        _timed_inserts(conn, next_id, 20)                 # warm-up, not recorded
                        ms = _timed_inserts(conn, next_id + 20, N_LAT)
                    finally:
                        conn.close()
                    next_id += 20 + N_LAT
                    res.append({'run_id': rid, 'exp': 'd5_latency', 'synchronous_commit': sc,
                                'synchronous_standby_names': names, 'path_delay_ms_each_way': delay,
                                'trial': trial, 'version': version_box[-1], 'latency': lat_summary(ms)})
            cl.set_latency(0)
        cl.set_sync('on', '')
    finally:
        cl.down()


class _Stmt(threading.Thread):
    """One statement on its own connection; never cancelled by the client."""

    def __init__(self, port, sql, params=None):
        super().__init__(daemon=True)
        self.port, self.sql, self.params = port, sql, params
        self.done = threading.Event()
        self.outcome = None
        self.notices = []
        self.pid = None
        self.t0 = None
        self.elapsed = None
        self.ready = threading.Event()

    def run(self):
        try:
            conn = connect('pg', port=self.port)
            conn.raw.add_notice_handler(lambda d: self.notices.append(f'{d.severity} {d.sqlstate}: {d.message_primary}'))
            self.pid = conn.q('select pg_backend_pid()')[0][0]
        except Exception as e:  # noqa: BLE001
            self.outcome = f'connect failed: {e!r}'[:200]
            self.ready.set()
            self.done.set()
            return
        self.t0 = time.monotonic()
        self.ready.set()
        try:
            conn.q(self.sql, self.params)
            self.outcome = 'ok'
        except DBError as e:
            self.outcome = f'error {e.code}: {e.msg}'[:200]
        except Exception as e:  # noqa: BLE001
            self.outcome = f'error {type(e).__name__}: {e}'[:200]
        self.elapsed = time.monotonic() - self.t0
        self.done.set()
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass

    def go(self):
        self.start()
        self.ready.wait(10)
        return self

    def waited(self):
        return round(self.elapsed if self.elapsed is not None else time.monotonic() - self.t0, 2)


def _quick(cl, fn, limit_s=5):
    """Run fn(conn) on a fresh primary connection in a thread; report whether it
    finished within limit_s and how long it took."""
    box = {}

    def target():
        t0 = time.monotonic()
        try:
            conn = cl.pconn()
            try:
                box['value'] = fn(conn)
                box['outcome'] = 'ok'
            finally:
                conn.close()
        except Exception as e:  # noqa: BLE001
            box['outcome'] = f'error: {e}'[:160]
        box['ms'] = round((time.monotonic() - t0) * 1000, 1)

    th = threading.Thread(target=target, daemon=True)
    th.start()
    th.join(limit_s)
    if th.is_alive():
        return {'returned': False, 'observed_s': limit_s}
    return {'returned': True, **box}


def _rolled_back(conn):
    conn.q('begin')
    conn.q('insert into t (id, v) values (2001, 0)')
    conn.q('rollback')
    return True


def run_stall(kind):
    two = kind == 'any1of2'
    names = 'ANY 1 (dic_s1, dic_s2)' if two else 'dic_s1'
    cl = Cluster(n_standby=2 if two else 1)
    port = C.PORTS['pgp']
    out = {'kind': kind, 'synchronous_commit': 'on', 'synchronous_standby_names': names,
           'window_s': WINDOW_S[kind]}
    try:
        cl.up()
        out['version'] = cl.version()
        cl.p_exec('create table t (id int primary key, v int not null)')
        cl.set_sync('on', names)
        conn = cl.pconn()
        base = _timed_inserts(conn, 1, BASELINE_N)
        out['baseline'] = lat_summary(base)
        cl.stop_standby(1, at=f'after {BASELINE_N} acknowledged commits')
        if two:
            one_down = _timed_inserts(conn, 101, BASELINE_N)      # must still return: s2 confirms
            out['one_standby_down'] = {'acked': len(one_down), **lat_summary(one_down)}
            cl.stop_standby(2, at=f'after {2 * BASELINE_N} acknowledged commits')
        conn.close()

        a = _Stmt(port, 'insert into t (id, v) values (1001, 0)').go()
        time.sleep(2.0)
        obs = {}
        obs['wait_event'] = cl.p_exec('select wait_event_type, wait_event from pg_stat_activity where pid = %s', (a.pid,))
        obs['read_only_query'] = _quick(cl, lambda c: c.q('select count(*) from t')[0][0])
        obs['rolled_back_txn'] = _quick(cl, _rolled_back)
        obs['row_visible_to_others'] = _quick(cl, lambda c: c.q('select count(*) from t where id = 1001')[0][0])
        b = _Stmt(port, 'insert into t (id, v) values (1002, 0)').go()
        lockp = _Stmt(port, 'insert into t (id, v) values (1001, 1)').go()   # needs the waiting txn to finish
        time.sleep(3.0)
        obs['second_write_returned_in_3s'] = b.done.is_set()
        obs['same_key_write_returned_in_3s'] = lockp.done.is_set()
        # watch the first write for the whole window
        a.done.wait(max(0.0, WINDOW_S[kind] - a.waited()))
        out['first_write_returned_in_window'] = a.done.is_set()
        out['first_write_waited_s'] = a.waited()
        out['during_wait'] = obs

        t_fix = time.monotonic()
        if kind == 'standby_back' or two:
            cl.start_standby(2 if two else 1, at=f'after the {WINDOW_S[kind]} s window')
        elif kind == 'reload_empty':
            cl.p_exec("alter system set synchronous_standby_names = ''")
            cl.p_exec('select pg_reload_conf()')
            cl.events.append(['reload', "synchronous_standby_names = ''"])
        elif kind == 'cancel':
            cl.p_exec('select pg_cancel_backend(%s)', (a.pid,))
            cl.events.append(['pg_cancel_backend', 'on the waiting backend'])
        elif kind == 'primary_restart':
            D.docker('restart', '-t', '20', PRIMARY)
            cl.events.append(['restart_primary', 'docker restart (fast shutdown) while the commit was waiting'])
            D.wait_ready(PRIMARY, cl._probe('pgp'), timeout=120)
        a.done.wait(60)
        out['after_fix'] = {
            'first_write_returned': a.done.is_set(), 'first_write_outcome': a.outcome,
            'first_write_notices': a.notices[:3],
            'returned_after_fix_s': round(time.monotonic() - t_fix, 2) if a.done.is_set() else None,
        }
        # final state on the primary: is the row of the waiting commit there?
        fin = _quick(cl, lambda c: [r[0] for r in c.q('select id from t where id in (1001, 1002) order by id')], limit_s=20)
        out['after_fix']['rows_1001_1002_on_primary'] = fin.get('value')
        if kind in ('cancel', 'primary_restart'):
            # the standby is still down: a new write is expected to wait again
            n = _Stmt(port, 'insert into t (id, v) values (1003, 0)').go()
            n.done.wait(5)
            out['after_fix']['new_write_returned_in_5s'] = n.done.is_set()
        b.done.wait(5)
        lockp.done.wait(5)
        out['after_fix']['second_write_outcome'] = b.outcome
        out['after_fix']['same_key_write_outcome'] = lockp.outcome
        out['events'] = cl.events
        return out
    finally:
        cl.down()


def run(res, mem_check, log=print):
    vb = []
    for trial in range(1, LAT_TRIALS + 1):
        if all(res.has(f'd5/latency/{sc}/{n or "empty"}/delay{d}/t{trial}')
               for d in (0, PATH_DELAY_MS) for n in ('', 'dic_s1') for sc in SYNC_COMMITS if not (d and not n)):
            continue
        mem_check(f'd5 latency t{trial}')
        run_latency(trial, res, vb)
        log(f'  d5 latency t{trial}: done')
    for kind in WINDOW_S:
        for trial in range(1, STALL_TRIALS + 1):
            rid = f'd5/stall/{kind}/t{trial}'
            if res.has(rid):
                continue
            mem_check(rid)
            t0 = time.monotonic()
            r = run_stall(kind)
            res.append({'run_id': rid, 'exp': 'd5_stall', 'trial': trial,
                        'duration_s': round(time.monotonic() - t0, 1), **r})
            log(f'  {rid}: returned_in_window={r["first_write_returned_in_window"]} '
                f'waited={r["first_write_waited_s"]}s after_fix={r["after_fix"]["first_write_outcome"]}')
