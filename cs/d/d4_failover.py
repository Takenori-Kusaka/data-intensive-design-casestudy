"""D-4: acknowledged commits that are missing after failover.

For each (synchronous_commit, synchronous_standby_names) pair, three modes:

cut            One writer. After N1 acknowledged commits (and after the standby has
               replayed them, checked by LSN) the replication path is cut. The writer
               goes on until N2 more commits are acknowledged or it gets no reply for
               STALL_S seconds. Then the primary is SIGKILLed and the standby promoted.
kill           Four writers, no cut. The primary is SIGKILLed when KILL_AT commits have
               been acknowledged, then the standby is promoted.
crash_restart  Like kill, but no failover: the killed primary is started again and
               read (shows what synchronous_commit=off loses on its own).

Each commit is one single-row INSERT of (writer, seq). "Acknowledged" means the
INSERT returned without error to the client. Lost = acknowledged but absent on
the node that serves afterwards.
"""
import threading
import time

from . import config as C
from .db import DBError, connect
from .judges import repl_loss_judge
from .repl import Cluster

SYNC_COMMITS = ['off', 'local', 'remote_write', 'on', 'remote_apply']
STANDBY_NAMES = ['', 'dic_s1']
N1, N2 = 200, 200
KILL_AT = 1000
STALL_S = 5
TRIALS = {'cut': 3, 'kill': 5, 'crash_restart': 5}


class Writer(threading.Thread):
    def __init__(self, port, wid, start_seq=1, limit=None):
        super().__init__(daemon=True)
        self.port, self.wid, self.seq, self.limit = port, wid, start_seq, limit
        self.acked = []
        self.inflight = None
        self.error = None
        self.stop_flag = threading.Event()
        self.last_ack = time.monotonic()

    def run(self):
        try:
            conn = connect('pg', port=self.port)
        except Exception as e:  # noqa: BLE001
            self.error = repr(e)[:200]
            return
        try:
            while not self.stop_flag.is_set() and (self.limit is None or len(self.acked) < self.limit):
                self.inflight = self.seq
                conn.q('insert into acks (writer, seq) values (%s, %s)', (self.wid, self.seq))
                self.acked.append((self.wid, self.seq))     # acknowledged
                self.last_ack = time.monotonic()
                self.inflight = None
                self.seq += 1
        except DBError as e:
            self.error = f'{e.code}: {e.msg}'[:200]
        except Exception as e:  # noqa: BLE001 - connection killed with the server
            self.error = repr(e)[:200]
        finally:
            conn.close()


def _prepare(sc, names):
    cl = Cluster(n_standby=1)
    try:
        cl.up()
        cl.p_exec('create table acks (writer int not null, seq int not null, primary key (writer, seq))')
        repl_state = cl.set_sync(sc, names)
        cl.wait_caught_up()
        cl.pg_version = cl.version()
    except BaseException:
        cl.down()
        raise
    return cl, repl_state


def run_cut(sc, names):
    cl, repl_state = _prepare(sc, names)
    try:
        w = Writer(C.PORTS['pgp'], 1, limit=N1)
        w.start()
        w.join(120)
        before = list(w.acked)
        cl.wait_caught_up()
        cl.cut(at=f'after {len(before)} acknowledged commits')
        w2 = Writer(C.PORTS['pgp'], 1, start_seq=len(before) + 1, limit=N2)
        w2.start()
        stalled = False
        while w2.is_alive():
            if time.monotonic() - w2.last_ack > STALL_S:
                stalled = True
                break
            time.sleep(0.05)
        after = list(w2.acked)
        cl.kill_primary(at=f'after {len(before) + len(after)} acknowledged commits')
        w2.join(20)
        after = list(w2.acked)          # nothing can be acknowledged after the kill
        cl.promote(at='after primary was killed')
        present = cl.s_exec('select writer, seq from acks')
        j = repl_loss_judge(before + after, present)
        return {**j, 'acked_before_cut': len(before), 'acked_after_cut': len(after), 'stalled': stalled,
                'stall_observed_s': STALL_S if stalled else 0, 'repl_state': [list(r) for r in repl_state],
                'events': cl.events, 'serving_node': 'promoted standby', 'version': cl.pg_version}
    finally:
        cl.down()


def run_kill(sc, names, restart_primary=False):
    cl, repl_state = _prepare(sc, names)
    try:
        ws = [Writer(C.PORTS['pgp'], i) for i in range(1, 5)]
        for w in ws:
            w.start()
        t0 = time.monotonic()
        while sum(len(w.acked) for w in ws) < KILL_AT and time.monotonic() - t0 < 120:
            time.sleep(0.001)
        n_at = sum(len(w.acked) for w in ws)
        cl.kill_primary(at=f'at about {KILL_AT} acknowledged commits (counted {n_at})')
        for w in ws:
            w.join(30)
        acked = [a for w in ws for a in w.acked]
        if restart_primary:
            cl.start_primary(at='after the kill (no failover)')
            present = cl.p_exec('select writer, seq from acks')
            node = 'restarted primary'
        else:
            cl.promote(at='after primary was killed')
            present = cl.s_exec('select writer, seq from acks')
            node = 'promoted standby'
        j = repl_loss_judge(acked, present)
        return {**j, 'writers': len(ws), 'repl_state': [list(r) for r in repl_state], 'events': cl.events,
                'serving_node': node, 'version': cl.pg_version}
    finally:
        cl.down()


def run(res, mem_check, log=print):
    plan = []
    for mode in ('cut', 'kill'):
        for names in STANDBY_NAMES:
            for sc in SYNC_COMMITS:
                plan += [(mode, sc, names, t) for t in range(1, TRIALS[mode] + 1)]
    for sc in ('off', 'on'):
        plan += [('crash_restart', sc, '', t) for t in range(1, TRIALS['crash_restart'] + 1)]
    for n, (mode, sc, names, trial) in enumerate(plan):
        rid = f'd4/{mode}/{sc}/{names or "empty"}/t{trial}'
        if res.has(rid):
            continue
        if n % 5 == 0:
            mem_check(f'd4 {rid}')
        t0 = time.monotonic()
        if mode == 'cut':
            r = run_cut(sc, names)
        else:
            r = run_kill(sc, names, restart_primary=(mode == 'crash_restart'))
        res.append({'run_id': rid, 'exp': 'd4', 'mode': mode, 'synchronous_commit': sc,
                    'synchronous_standby_names': names, 'trial': trial,
                    'duration_s': round(time.monotonic() - t0, 1), **r})
        log(f'  {rid}: acked={r["acked"]} lost={r["lost_acked"]} ({round(time.monotonic() - t0, 1)}s)')
