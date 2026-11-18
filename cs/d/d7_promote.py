"""D-7: promotion with and without fencing the old primary (split brain).

1. Writer A commits N_BASE rows on the primary; the standby replays them (LSN check).
2. The replication path is cut (the two DB nodes cannot see each other; clients
   can still reach both).
3. fenced:   the old primary is stopped (docker stop) BEFORE the promotion.
   unfenced: the old primary is left running.
4. The standby is promoted.
5. Writer A (still pointing at the old primary) tries N_AFTER more commits.
   Writer B commits N_AFTER rows on the new primary.
6. Both nodes are read and compared. The old primary is then restarted and read
   again (rows that were written locally but never acknowledged become visible).

Split-brain count = commits acknowledged by the old primary after the promotion.
"""
import time

from . import config as C
from .d4_failover import Writer
from .db import connect
from .judges import split_judge
from .repl import Cluster

REPL = {'async': ('on', ''), 'sync': ('on', 'dic_s1')}
N_BASE, N_AFTER = 100, 100
STALL_S = 5
TRIALS = 3


class _OrderWriter(Writer):
    def run(self):
        try:
            conn = connect('pg', port=self.port)
        except Exception as e:  # noqa: BLE001
            self.error = repr(e)[:200]
            return
        try:
            while not self.stop_flag.is_set() and len(self.acked) < self.limit:
                conn.q('insert into orders (writer, seq) values (%s, %s)', (self.wid, self.seq))
                self.acked.append((self.wid, self.seq))
                self.last_ack = time.monotonic()
                self.seq += 1
        except Exception as e:  # noqa: BLE001
            self.error = str(e)[:200]
        finally:
            conn.close()


def _write(port, wid, start, n):
    w = _OrderWriter(port, wid, start_seq=start, limit=n)
    w.start()
    stalled = False
    while w.is_alive():
        if time.monotonic() - w.last_ack > STALL_S:
            stalled = True
            break
        time.sleep(0.05)
    return w, stalled


def _rows(port):
    c = connect('pg', port=port, connect_timeout=3)
    try:
        return c.q('select id, writer, seq from orders order by id')
    finally:
        c.close()


def run_one(repl, fenced):
    sc, names = REPL[repl]
    cl = Cluster(n_standby=1)
    try:
        cl.up()
        cl.p_exec('create table orders (id bigserial primary key, writer text not null, seq int not null)')
        cl.set_sync(sc, names)
        a0, _ = _write(C.PORTS['pgp'], 'A', 1, N_BASE)
        cl.wait_caught_up()
        cl.cut(at=f'after {len(a0.acked)} acknowledged commits')
        if fenced:
            cl.stop_primary(at='fencing: before the promotion')
        cl.promote(at='old primary ' + ('stopped' if fenced else 'still running'))
        a1, a_stalled = _write(C.PORTS['pgp'], 'A', N_BASE + 1, N_AFTER)
        b1, b_stalled = _write(C.PORTS['pgs1'], 'B', 1, N_AFTER)
        new_rows = _rows(C.PORTS['pgs1'])
        try:
            old_rows = _rows(C.PORTS['pgp'])
            live = split_judge(old_rows, new_rows)
        except Exception:  # noqa: BLE001 - old primary is down (fenced)
            live = None
        # restart the old primary only to inspect what it holds
        if fenced:
            cl.start_primary(at='inspection only, after the measurement')
        else:
            cl.stop_primary(at='inspection only, after the measurement')
            cl.start_primary(at='inspection only')
        after = split_judge(_rows(C.PORTS['pgp']), new_rows)
        return {
            'base_acked': len(a0.acked),
            'old_acked_after_promotion': len(a1.acked),     # the split-brain count
            'old_writer_error': a1.error, 'old_writer_stalled': a_stalled,
            'new_acked': len(b1.acked), 'new_writer_error': b1.error, 'new_writer_stalled': b_stalled,
            'old_node_readable_during_test': live is not None,
            'live': live, 'after_old_restart': after, 'stall_observed_s': STALL_S,
            'events': cl.events, 'version': cl.version(),
        }
    finally:
        cl.down()


def run(res, mem_check, log=print):
    for repl in REPL:
        for fenced in (False, True):
            for trial in range(1, TRIALS + 1):
                rid = f'd7/{repl}/{"fenced" if fenced else "unfenced"}/t{trial}'
                if res.has(rid):
                    continue
                mem_check(rid)
                t0 = time.monotonic()
                r = run_one(repl, fenced)
                res.append({'run_id': rid, 'exp': 'd7', 'replication': repl, 'fenced': fenced, 'trial': trial,
                            'synchronous_commit': REPL[repl][0], 'synchronous_standby_names': REPL[repl][1],
                            'n_base': N_BASE, 'n_after': N_AFTER,
                            'duration_s': round(time.monotonic() - t0, 1), **r})
                log(f'  {rid}: old_acked_after_promotion={r["old_acked_after_promotion"]} new_acked={r["new_acked"]} '
                    f'after_restart={r["after_old_restart"]}')
