"""A small request load against PostgreSQL (threads, one connection each) and the
measurements taken from its request log. Used by B-3 and B-6.

Every request is one line in ``ops``: kind ('w' or 'r'), thread, start, end
(time.monotonic), ok, error class, phase label, wrong (a read that returned a value the
generator did not write). ``max_gap`` is the judge of "writes stopped for how long".
"""
import threading
import time
from collections import Counter, namedtuple

import psycopg

from ..common.record import percentile

Op = namedtuple('Op', 'kind thread t0 t1 ok err phase wrong')


def err_class(e):
    code = getattr(e, 'sqlstate', None)
    return f'{type(e).__name__}:{code}' if code else type(e).__name__


class Load:
    """``request(kind, conn, ctx)`` performs one request; it raises on failure and may
    return True to flag a wrong read. ``ctx`` is a dict private to the thread (it holds
    'thread', 'group', 'rng' and whatever the request function stores)."""

    def __init__(self, connect, request, n_writers=4, n_readers=2, timeout_ms=3000, pause_s=0.002, groups=2):
        self.connect, self.request = connect, request
        self.timeout_ms, self.pause_s = timeout_ms, pause_s
        self.ops, self.phase = [], 'start'
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self.ctxs = []
        self.threads = []
        for i in range(n_writers + n_readers):
            kind = 'w' if i < n_writers else 'r'
            ctx = {'thread': i, 'kind': kind, 'group': i % groups}
            self.ctxs.append(ctx)
            self.threads.append(threading.Thread(target=self._loop, args=(kind, ctx), daemon=True))

    def _conn(self, ctx):
        c = self.connect(ctx)
        c.execute(f'set statement_timeout = {int(self.timeout_ms)}')
        return c

    def _loop(self, kind, ctx):
        conn = None
        while not self._stop.is_set():
            t0 = time.monotonic()
            ok, err, wrong = True, None, False
            try:
                if conn is None:
                    conn = self._conn(ctx)
                wrong = bool(self.request(kind, conn, ctx))
            except Exception as e:  # noqa: BLE001 - a failed request; drop the connection
                ok, err = False, err_class(e)
                try:
                    if conn is not None:
                        conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = None
            t1 = time.monotonic()
            with self._lock:
                self.ops.append(Op(kind, ctx['thread'], t0, t1, ok, err, self.phase, wrong))
            time.sleep(self.pause_s if ok else 0.05)
        if conn is not None:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass

    def start(self):
        for t in self.threads:
            t.start()
        return self

    def stop(self):
        self._stop.set()
        for t in self.threads:
            t.join(timeout=self.timeout_ms / 1000 + 10)
        return self.ops

    def snapshot(self):
        with self._lock:
            return list(self.ops)

    def count(self, kind='w'):
        with self._lock:
            return sum(1 for o in self.ops if o.kind == kind and o.ok)

    def wait_ops(self, n, kind='w', timeout=60):
        """Block until n more successful requests of the kind have completed."""
        target, t0 = self.count(kind) + n, time.monotonic()
        while self.count(kind) < target and time.monotonic() - t0 < timeout:
            time.sleep(0.01)


def max_gap(times, t_a, t_b):
    """Longest interval inside [t_a, t_b] without any completion (seconds). ``times`` are
    the completion times of successful requests."""
    pts = sorted(t for t in times if t_a <= t <= t_b)
    edges = [t_a] + pts + [t_b]
    return max(b - a for a, b in zip(edges, edges[1:]))


def window(ops, kind, t_a, t_b, slow_ms=100):
    """Measurements over the requests of one kind that completed inside [t_a, t_b]."""
    sel = [o for o in ops if o.kind == kind and t_a <= o.t1 <= t_b]
    oks = [o for o in sel if o.ok]
    lat = [(o.t1 - o.t0) * 1000 for o in oks]
    return {'ok': len(oks), 'failed': len(sel) - len(oks), 'wrong': sum(1 for o in oks if o.wrong),
            'errors': dict(Counter(o.err for o in sel if not o.ok)),
            'max_gap_ms': round(max_gap([o.t1 for o in oks], t_a, t_b) * 1000, 1),
            'p50_ms': _r(percentile(lat, 50)), 'p95_ms': _r(percentile(lat, 95)), 'max_ms': _r(max(lat) if lat else None),
            'slow': sum(1 for x in lat if x > slow_ms), 'seconds': round(t_b - t_a, 3)}


def by_phase(ops, kind):
    out = {}
    for o in ops:
        if o.kind != kind:
            continue
        d = out.setdefault(o.phase, {'ok': 0, 'failed': 0, 'wrong': 0, 'errors': Counter()})
        if o.ok:
            d['ok'] += 1
            d['wrong'] += 1 if o.wrong else 0
        else:
            d['failed'] += 1
            d['errors'][o.err] += 1
    for d in out.values():
        d['errors'] = dict(d['errors'])
    return out


def _r(x):
    return None if x is None else round(x, 2)


class LockSampler:
    """Polls pg_stat_activity (sessions waiting for a lock) and, when a backend pid is
    given, the relation locks that backend holds or waits for."""

    def __init__(self, connect, interval=0.05):
        self.connect, self.interval = connect, interval
        self.pid = None
        self.max_waiting, self.samples_waiting, self.samples = 0, 0, 0
        self.modes = set()
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        conn = self.connect()
        while not self._stop.is_set():
            try:
                n = conn.execute("select count(*) from pg_stat_activity where wait_event_type = 'Lock'").fetchone()[0]
                self.samples += 1
                self.max_waiting = max(self.max_waiting, n)
                self.samples_waiting += 1 if n else 0
                if self.pid:
                    for rel, mode, granted in conn.execute(
                            "select relation::regclass::text, mode, granted from pg_locks "
                            "where pid = %s and locktype = 'relation'", (self.pid,)).fetchall():
                        self.modes.add((rel, mode, granted))
            except psycopg.Error:
                pass
            self._stop.wait(self.interval)
        conn.close()

    def start(self):
        self._t.start()
        return self

    def stop(self):
        self._stop.set()
        self._t.join(timeout=10)
        return {'max_waiting_sessions': self.max_waiting, 'samples_with_waiting': self.samples_waiting,
                'samples': self.samples}
