"""Building derived data from the event log (B-4, B-5): the event generator, the
follower that applies events to a derived store in PostgreSQL, and the judge of the
final state of a store against the generator.

Event: key = order id, value = JSON {"order_id", "ver", "status"}; a record with a null
value is a delete marker (tombstone) for the order. Versions of one order are produced in
order with the same key, so they stay ordered inside one partition.

Stores (tables in the derived database):
  index table   order_id, status, ver, update_count [, is_open]   one row per order
  agg table     status, n                                          number of applied events per status
"""
import json
import random
import threading
import time
from collections import Counter

from confluent_kafka import TopicPartition

from . import config as C, infra

COMMIT_BATCH = 200          # records per database transaction and per offset commit


def gen_events(seed, n_events, n_orders):
    """[(order_id, ver, status)] in the order they are produced."""
    rng = random.Random(seed)
    ver = Counter()
    out = []
    for _ in range(n_events):
        o = rng.randrange(1, n_orders + 1)
        ver[o] += 1
        out.append((o, ver[o], rng.choice(C.STATUSES)))
    return out


def encode(ev):
    return str(ev[0]), json.dumps({'order_id': ev[0], 'ver': ev[1], 'status': ev[2]}).encode()


def truth_of(events, deleted=()):
    """Final state implied by the events: ({order: (status, ver)}, {status: number of events})."""
    orders, counts = {}, Counter()
    for o, ver, status in events:
        orders[o] = (status, ver)
        counts[status] += 1
    for o in deleted:
        orders.pop(o, None)
    return orders, dict(counts)


def create_store(table, agg=None, which='pg2'):
    with infra.pg(which) as c:
        c.execute(f'create table {table} (order_id int primary key, status text not null, ver int not null, '
                  'update_count int not null, is_open boolean)')
        if agg:
            c.execute(f'create table {agg} (status text primary key, n bigint not null)')


def _apply_sql(table, agg, idempotent):
    guard = 'where t.ver < excluded.ver' if idempotent else ''
    up = (f'insert into {table} as t (order_id, status, ver, update_count, is_open) '
          'values (%(o)s, %(s)s, %(v)s, 1, %(s)s not in (\'completed\', \'cancelled\')) '
          'on conflict (order_id) do update set status = excluded.status, ver = excluded.ver, '
          f'is_open = excluded.is_open, update_count = t.update_count + 1 {guard}')
    if not agg:
        return up
    return (f'with up as ({up} returning 1) insert into {agg} as a (status, n) select %(s)s, 1 from up '
            'on conflict (status) do update set n = a.n + 1')


class Follower(threading.Thread):
    """Reads the topic with a consumer group and applies each batch in one database
    transaction; the offsets are committed to the broker after the transaction.

    idempotent   apply an event only when its version is newer than the stored one
    skip_status  planted defect: events with this status are dropped
    crash_after  after the batch in which this many records (of this incarnation) were
                 applied, stop without committing the offsets (what a kill between the
                 database commit and the offset commit leaves behind)
    deletes      apply delete markers (delete the row) - False = a follower that has no
                 delete handling
    start        None = committed offsets of the group (beginning when there are none),
                 or {partition: offset}
    """

    def __init__(self, topic, partitions, group, table, agg=None, idempotent=True, skip_status=None,
                 crash_after=0, deletes=True, start=None, which='pg2'):
        super().__init__(daemon=True)
        self.topic, self.partitions, self.group = topic, partitions, group
        self.table, self.agg, self.which = table, agg, which
        self.idempotent, self.skip_status, self.crash_after, self.deletes = idempotent, skip_status, crash_after, deletes
        self.start_at = start
        self.pos = {}                 # partition -> next offset to apply
        self.applied = 0
        self.tombstones_seen = 0
        self.crashed = False
        self.error = None
        self.delete_lat_ms = []       # produce timestamp -> delete applied (same host clock)
        self._halt = threading.Event()
        self._pause = threading.Event()

    def run(self):
        try:
            self._run()
        except Exception as e:  # noqa: BLE001 - surfaced by the controller
            self.error = repr(e)

    def _run(self):
        conn = infra.pg(self.which)
        cons = infra.consumer(self.group)
        tps = [TopicPartition(self.topic, p) for p in range(self.partitions)]
        if self.start_at is not None:
            tps = [TopicPartition(self.topic, p, self.start_at.get(p, 0)) for p in range(self.partitions)]
        else:
            t0 = time.monotonic()
            while True:           # the group coordinator may not be ready right after the broker starts
                try:
                    tps = cons.committed(tps, timeout=15)
                    break
                except Exception:  # noqa: BLE001
                    if time.monotonic() - t0 > 60:
                        raise
                    time.sleep(0.5)
        cons.assign(tps)
        sql = _apply_sql(self.table, self.agg, self.idempotent)
        while not self._halt.is_set():
            if self._pause.is_set():
                time.sleep(0.05)
                continue
            msgs = [m for m in cons.consume(COMMIT_BATCH, timeout=0.2) if not m.error()]
            if not msgs:
                continue
            seq, dels = [], []           # ('u', params) / ('d', order id) in log order
            for m in msgs:
                if (m.key() or b'').startswith(b'f'):      # filler records (B-5) carry no order
                    continue
                if m.value() is None:
                    self.tombstones_seen += 1
                    if self.deletes:
                        seq.append(('d', int(m.key())))
                        dels.append(m.timestamp()[1])
                    continue
                ev = json.loads(m.value())
                if self.skip_status and ev['status'] == self.skip_status:
                    continue
                seq.append(('u', {'o': ev['order_id'], 's': ev['status'], 'v': ev['ver']}))
            with conn.transaction():
                if dels:
                    for kind, x in seq:
                        if kind == 'd':
                            conn.execute(f'delete from {self.table} where order_id = %s', (x,))
                        else:
                            conn.execute(sql, x)
                elif seq:
                    conn.cursor().executemany(sql, [x for _k, x in seq])
            now_ms = time.time() * 1000
            self.delete_lat_ms += [now_ms - ts for ts in dels]
            for m in msgs:
                self.pos[m.partition()] = m.offset() + 1
            self.applied += len(msgs)
            if self.crash_after and self.applied >= self.crash_after:
                self.crashed = True          # database committed, offsets not committed
                cons.close()
                conn.close()
                return
            cons.commit(offsets=[TopicPartition(self.topic, p, o) for p, o in self.pos.items()], asynchronous=False)
        cons.close()
        conn.close()

    def stop(self):
        self._halt.set()
        self.join(timeout=30)

    def pause(self, on=True):
        (self._pause.set if on else self._pause.clear)()

    def lag(self):
        if self.error:
            raise RuntimeError(f'follower {self.group} failed: {self.error}')
        ends = infra.watermarks(self.topic, self.partitions)
        return sum(max(0, hi - self.pos.get(p, lo)) for p, (lo, hi) in enumerate(ends))


def wait_caught_up(follower, timeout=150, restart=None, max_lag=0):
    """Wait until the follower is at most ``max_lag`` records behind the end of the topic
    (0 = everything applied; a small allowance is needed while events keep arriving). When
    the follower crashed (crash_after), ``restart()`` must return a new one.
    Returns (follower, seconds)."""
    t0 = time.monotonic()
    while True:
        if follower.crashed:
            follower.join(timeout=10)
            follower = restart()
            follower.start()
        if follower.lag() <= max_lag:
            return follower, round(time.monotonic() - t0, 2)
        if time.monotonic() - t0 > timeout:
            raise RuntimeError(f'follower {follower.group} did not catch up in {timeout}s: {follower.pos}')
        time.sleep(0.05)


def read_store(table, agg=None, which='pg2'):
    rows = {r[0]: (r[1], r[2], r[3]) for r in infra.q(which, f'select order_id, status, ver, update_count from {table}')}
    counts = {r[0]: r[1] for r in infra.q(which, f'select status, n from {agg}')} if agg else None
    return rows, counts


def judge_store(truth_orders, truth_counts, rows, counts=None):
    """Final state of a derived store against the generator.
    truth_orders {order: (status, ver)}   truth_counts {status: number of events}
    rows {order: (status, ver, update_count)}   counts {status: n} or None (store has no aggregate)

    update_count must equal the version (every event of the order applied exactly once)."""
    out = {'orders_expected': len(truth_orders), 'orders_in_store': len(rows),
           'missing': len(set(truth_orders) - set(rows)), 'extra': len(set(rows) - set(truth_orders)),
           'state_mismatch': 0, 'count_mismatch': 0, 'count_surplus': 0, 'count_deficit': 0}
    for o, (status, ver) in truth_orders.items():
        got = rows.get(o)
        if got is None:
            continue
        if (got[0], got[1]) != (status, ver):
            out['state_mismatch'] += 1
        if got[2] != ver:
            out['count_mismatch'] += 1
            out['count_surplus'] += max(0, got[2] - ver)
            out['count_deficit'] += max(0, ver - got[2])
    if counts is None:
        out['agg_mismatch'] = out['agg_surplus'] = out['agg_deficit'] = None
    else:
        keys = set(truth_counts) | set(counts)
        out['agg_mismatch'] = sum(1 for k in keys if truth_counts.get(k, 0) != counts.get(k, 0))
        out['agg_surplus'] = sum(max(0, counts.get(k, 0) - truth_counts.get(k, 0)) for k in keys)
        out['agg_deficit'] = sum(max(0, truth_counts.get(k, 0) - counts.get(k, 0)) for k in keys)
    return out


class LiveWriter(threading.Thread):
    """Produces the given events at a fixed pace until stopped; ``acked`` counts the
    records acknowledged by the broker (a prefix of the list)."""

    def __init__(self, topic, events, rate_per_s):
        super().__init__(daemon=True)
        self.topic, self.events, self.rate = topic, events, rate_per_s
        self.sent = self.acked = 0
        self.failed = []
        self._halt = threading.Event()

    def run(self):
        p = infra.producer()

        def cb(err, _msg):
            if err is None:
                self.acked += 1
            else:
                self.failed.append(err.name())
        t0 = time.monotonic()
        for ev in self.events:
            if self._halt.is_set():
                break
            k, v = encode(ev)
            p.produce(self.topic, key=k, value=v, on_delivery=cb)
            self.sent += 1
            p.poll(0)
            delay = t0 + self.sent / self.rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        p.flush(30)

    def stop(self):
        self._halt.set()
        self.join(timeout=60)
        if self.failed or self.acked != self.sent:
            raise RuntimeError(f'live writer: sent={self.sent} acked={self.acked} failed={self.failed[:3]}')
        return self.acked
