"""B-4: replacing a derived store that is built from the event log, while events keep
arriving and readers keep reading.

  n            one shot, in place: stop the follower, empty the store, build it again
               from the beginning of the log while the readers read the same store
  be           build the new store next to the old one, reconcile old and new, move the
               readers in steps (10 % -> 50 % -> 100 %), then drop the old store
  be_norecon   the same without the reconciliation (BE - reconciliation)

Variants of the new pipeline:
  clean        nothing wrong
  bug          planted defect: the new follower drops events with status 'cancelled'
  crash        the new follower dies once during the rebuild after the database commit and
               before the offset commit, and is started again (idempotent apply)
  crash_noidem the same with a follower that applies every delivery (BE - idempotent consumer)
"""
import random
import threading
import time

from . import config as C, derive as V, infra

TOPIC, PARTS = 'b4.orders', 4
N_ORDERS = 500
N_PRELOAD = 10000
N_LIVE = 14000             # upper bound of the events produced while the swap is running (70 s)
RATE = 200                 # live events per second
CRASH_AFTER = 7000
TRIALS = 5
RECON_ROUNDS, RECON_INTERVAL_S = 3, 1.0
LAG_OK = 60                # records behind the end that count as "caught up" while events keep arriving

CASES = [('n', 'clean'), ('be', 'clean'), ('be', 'bug'), ('be_norecon', 'clean'), ('be_norecon', 'bug'),
         ('be', 'crash'), ('be', 'crash_noidem'), ('be_norecon', 'crash_noidem')]
CONFIG_JA = {'n': 'N 一括で作り直す（同じ場所）', 'be': 'BE 並べて作り、照合して少しずつ切り替える', 'be_norecon': 'BE − 照合'}
VARIANT_JA = {'clean': '欠陥なし', 'bug': '新しい受け手に欠陥（cancelled を捨てる）',
              'crash': '作り直しの途中で受け手が 1 回落ちる（冪等）',
              'crash_noidem': '作り直しの途中で受け手が 1 回落ちる（冪等でない）'}


def plan():
    return [{'exp': 'b4', 'config': c, 'variant': v, 'trial': t, 'seed': C.SEED0 + t}
            for c, v in CASES for t in range(1, TRIALS + 1)]


def run_id_of(s):
    return f'b4:{s["config"]}:{s["variant"]}:t{s["trial"]}'


class Readers:
    """Two reader threads. Each read picks a random order and reads it from the old or
    the new store (the share of the new store is ``p_new``). Counted per store:
    not found, regress (an older version than the same reader has already seen for the
    order) and errors."""

    def __init__(self, seed, n=2):
        self.p_new = 0.0
        self.old_table, self.new_table = 'idx_old', 'idx_new'
        self.stats = {}
        self.first_bad = self.last_bad = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self.threads = [threading.Thread(target=self._loop, args=(random.Random(f'{seed}:r{i}'),), daemon=True)
                        for i in range(n)]

    def _bump(self, store, key):
        with self._lock:
            d = self.stats.setdefault(store, {'reads': 0, 'not_found': 0, 'regress': 0, 'error': 0})
            d[key] += 1
            if key != 'reads':
                now = time.monotonic()
                self.first_bad = self.first_bad or now
                self.last_bad = now

    def _loop(self, rng):
        conn, seen = None, {}
        while not self._stop.is_set():
            store = 'new' if rng.random() < self.p_new else 'old'
            table = self.new_table if store == 'new' else self.old_table
            o = rng.randrange(1, N_ORDERS + 1)
            try:
                if conn is None:
                    conn = infra.pg('pg2')
                row = conn.execute(f'select ver from {table} where order_id = %s', (o,)).fetchone()
                self._bump(store, 'reads')
                if row is None:
                    self._bump(store, 'not_found')
                elif row[0] < seen.get(o, 0):
                    self._bump(store, 'regress')
                else:
                    seen[o] = row[0]
            except Exception:  # noqa: BLE001
                self._bump(store, 'reads')
                self._bump(store, 'error')
                try:
                    conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = None
            time.sleep(0.002)

    def start(self):
        for t in self.threads:
            t.start()
        return self

    def stop(self):
        self._stop.set()
        for t in self.threads:
            t.join(timeout=10)
        bad_s = round(self.last_bad - self.first_bad, 2) if self.first_bad else 0.0
        return {'by_store': self.stats, 'bad_span_s': bad_s}


SNAP_SQL = """
select coalesce(o.order_id, n.order_id), o.status, o.ver, o.update_count, n.status, n.ver, n.update_count
from idx_old o full join idx_new n on o.order_id = n.order_id
"""


def snapshot():
    """Old and new store (and the aggregate of new) in one database snapshot."""
    with infra.pg('pg2', autocommit=False) as c:
        c.execute('set transaction isolation level repeatable read')
        rows = {r[0]: (None if r[2] is None else (r[1], r[2], r[3]), None if r[5] is None else (r[4], r[5], r[6]))
                for r in c.execute(SNAP_SQL).fetchall()}
        agg = c.execute('select coalesce(sum(n), 0) from agg_new').fetchone()[0]
        c.rollback()
    return {'rows': rows, 'agg_total': int(agg)}


def mismatched(snap):
    """{order: new-side tuple} for the orders that differ: absent on one side, another
    version, or the same version with another status or another number of applied events."""
    return {o: n for o, (old, n) in snap['rows'].items() if old is None or n is None or old != n}


def recon_judge(s1, s2):
    """Reconciliation while events keep arriving. An order counts only when it differs in
    both snapshots and the new store did not move in between (so it is not in flight).
    The aggregate of the new store must equal the events the new store has applied."""
    m1, m2 = mismatched(s1), mismatched(s2)
    persistent = sorted(o for o in m1 if o in m2 and m1[o] == m2[o])
    applied_new = sum(n[2] for _old, n in s2['rows'].values() if n is not None)
    return {'persistent': len(persistent), 'in_flight': max(len(m1), len(m2)) - len(persistent),
            'exact_equal_snapshots': int(not m1) + int(not m2),
            'agg_consistent': s2['agg_total'] == applied_new,
            'passed': not persistent and s2['agg_total'] == applied_new}


def reconcile(rounds=RECON_ROUNDS, interval=RECON_INTERVAL_S):
    out = []
    for _ in range(rounds):
        s1 = snapshot()
        time.sleep(interval)
        out.append(recon_judge(s1, snapshot()))
        if out[-1]['passed']:
            break
    return {'passed': out[-1]['passed'], 'rounds': len(out), 'persistent': out[-1]['persistent'],
            'in_flight_max': max(r['in_flight'] for r in out), 'agg_consistent': out[-1]['agg_consistent'],
            'exact_equal_snapshots': sum(r['exact_equal_snapshots'] for r in out), 'snapshots': 2 * len(out)}


def run_one(spec, log=print):
    t0 = time.monotonic()
    cfg, var, seed = spec['config'], spec['variant'], spec['seed']
    infra.down()
    infra.pg_up('pg2', args=['-c', 'synchronous_commit=off'], wait=False)
    infra.broker_up('redpanda')
    infra.pg_wait('pg2')
    infra.create_topic(TOPIC, PARTS)
    events = V.gen_events(seed, N_PRELOAD + N_LIVE, N_ORDERS)
    infra.produce_all(TOPIC, [V.encode(e) for e in events[:N_PRELOAD]])
    V.create_store('idx_old')
    old = V.Follower(TOPIC, PARTS, 'dic-b-old', 'idx_old')
    old.start()
    old, _ = V.wait_caught_up(old)
    writer = V.LiveWriter(TOPIC, events[N_PRELOAD:], RATE)
    writer.start()
    readers = Readers(seed).start()
    time.sleep(1.0)
    row = {'run_id': run_id_of(spec), **spec, 'n_orders': N_ORDERS, 'n_preload': N_PRELOAD, 'live_rate': RATE}
    new, serving, outcome, recon = None, 'old', None, None
    t_r0 = time.monotonic()
    if cfg == 'n':
        old.stop()
        infra.q('pg2', 'truncate idx_old')
        old = V.Follower(TOPIC, PARTS, 'dic-b-rebuild', 'idx_old', start={p: 0 for p in range(PARTS)})
        old.start()
        old, rebuild_s = V.wait_caught_up(old, max_lag=LAG_OK)
        outcome = 'rebuilt_in_place'
        time.sleep(1.0)
    else:
        V.create_store('idx_new', 'agg_new')
        kw = {'idempotent': var != 'crash_noidem', 'skip_status': 'cancelled' if var == 'bug' else None}

        def make(crash=0):
            return V.Follower(TOPIC, PARTS, 'dic-b-new', 'idx_new', 'agg_new', crash_after=crash, **kw)
        new = make(CRASH_AFTER if var.startswith('crash') else 0)
        new.start()
        new, rebuild_s = V.wait_caught_up(new, restart=make, max_lag=LAG_OK)
        if cfg == 'be':
            recon = reconcile()
        if recon is not None and not recon['passed']:
            outcome = 'aborted_by_reconciliation'          # readers stay on the old store
            time.sleep(1.0)
        else:
            for p in (0.1, 0.5, 1.0):
                readers.p_new = p
                time.sleep(1.0)
            old.stop()
            infra.q('pg2', 'drop table idx_old')
            old, serving, outcome = None, 'new', 'switched'
            time.sleep(1.0)
    acked = N_PRELOAD + writer.stop()
    truth_orders, truth_counts = V.truth_of(events[:acked])
    final = {}
    if old is not None:
        old, _ = V.wait_caught_up(old)
        old.stop()
        final['old'] = V.judge_store(truth_orders, truth_counts, *V.read_store('idx_old'))
    if new is not None:
        new, _ = V.wait_caught_up(new, restart=None)
        new.stop()
        final['new'] = V.judge_store(truth_orders, truth_counts, *V.read_store('idx_new', 'agg_new'))
    reads = readers.stop()
    sizes = {t: infra.q('pg2', 'select pg_total_relation_size(%s::regclass)', (t,))[0][0]
             for t in ('idx_old', 'idx_new', 'agg_new')
             if infra.q('pg2', 'select to_regclass(%s) is not null', (t,))[0][0]}
    s = final[serving]
    row.update({'events_acked': acked, 'rebuild_s': rebuild_s, 'swap_s': round(time.monotonic() - t_r0, 2),
                'outcome': outcome, 'serving': serving, 'reconciliation': recon, 'final': final, 'reads': reads,
                'store_bytes': sizes, 'versions': {'redpanda': infra.broker_version('redpanda'),
                                                   'pg': infra.q('pg2', 'show server_version')[0][0]},
                'container_memory': infra.mem()})
    bad = {k: sum(v[k] for v in reads['by_store'].values()) for k in ('not_found', 'regress', 'error')}
    row['brief'] = (f'{outcome} rebuild={rebuild_s}s recon={recon and (recon["passed"], recon["rounds"], recon["persistent"], recon["exact_equal_snapshots"])} '
                    f'serving={serving}: state={s["state_mismatch"]} count={s["count_mismatch"]} '
                    f'agg={s["agg_mismatch"]} reads={bad} ')
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row
