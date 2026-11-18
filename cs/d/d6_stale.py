"""D-6: reading your own write from the standby.

For each write: INSERT on the primary (commit acknowledged), then, as the very
next statement, look the row up on the standby. "Stale" = the row is not there.
The lookup is then repeated until the row appears, which gives the delay until
the write became visible on the standby.

Replication is delayed with recovery_min_apply_delay on the standby (0 or
APPLY_DELAY_MS). Modes: async (synchronous_standby_names empty), sync_on
(synchronous_commit=on, standby named), remote_apply (standby named).
"""
import time

from ..common.record import lat_summary
from .judges import stale_judge
from .repl import Cluster

MODES = {'async': ('on', ''), 'sync_on': ('on', 'dic_s1'), 'remote_apply': ('remote_apply', 'dic_s1')}
APPLY_DELAYS_MS = [0, 200]
N_WRITES = 200
TRIALS = 3
VISIBLE_CAP_S = 5


def run_one(mode, delay_ms):
    sc, names = MODES[mode]
    cl = Cluster(n_standby=1, apply_delay_ms=delay_ms)
    try:
        cl.up()
        cl.p_exec('create table t (id int primary key, v int not null)')
        cl.set_sync(sc, names)
        cl.wait_caught_up()
        p, s = cl.pconn(), cl.sconn()
        flags, commit_ms, visible_ms, never = [], [], [], 0
        for i in range(1, N_WRITES + 1):
            t0 = time.monotonic()
            p.q('insert into t (id, v) values (%s, 0)', (i,))
            t1 = time.monotonic()
            seen = bool(s.q('select 1 from t where id = %s', (i,)))
            flags.append(seen)
            commit_ms.append((t1 - t0) * 1000)
            while not seen and time.monotonic() - t1 < VISIBLE_CAP_S:
                time.sleep(0.002)
                seen = bool(s.q('select 1 from t where id = %s', (i,)))
            if seen:
                visible_ms.append((time.monotonic() - t1) * 1000)
            else:
                never += 1
        p.close()
        s.close()
        return {**stale_judge(flags), 'commit_latency': lat_summary(commit_ms),
                'visible_after_ack': lat_summary(visible_ms), 'not_visible_within_cap': never,
                'visible_cap_s': VISIBLE_CAP_S, 'version': cl.version()}
    finally:
        cl.down()


def run(res, mem_check, log=print):
    for mode in MODES:
        for delay in APPLY_DELAYS_MS:
            for trial in range(1, TRIALS + 1):
                rid = f'd6/{mode}/delay{delay}/t{trial}'
                if res.has(rid):
                    continue
                mem_check(rid)
                t0 = time.monotonic()
                r = run_one(mode, delay)
                res.append({'run_id': rid, 'exp': 'd6', 'mode': mode, 'synchronous_commit': MODES[mode][0],
                            'synchronous_standby_names': MODES[mode][1], 'apply_delay_ms': delay,
                            'n_writes': N_WRITES, 'trial': trial,
                            'duration_s': round(time.monotonic() - t0, 1), **r})
                log(f'  {rid}: stale={r["stale"]}/{r["n"]} commit_p50={r["commit_latency"]["p50_ms"]}ms')
