"""One run of topic A: fresh containers, workload, fault, settle, judge, record.

A run is described by a spec (dict):

  exp          result file name (main / kafka)
  broker       redpanda | kafka
  config       n | b | be
  variant      '' | noidem (consumer does not deduplicate) | wm (watermark relay)
               | nokey (CDC keyed by event id instead of order id)
  case         normal | longtxn | writer_kill | broker_cut | broker_pause | broker_kill
               | consumer_kill | consumer_late | relay_kill
  concurrency, trial, seed, n_ops, n_orders
"""
import glob
import json
import os
import shutil
import signal
import subprocess
import sys
import time

from ..common import dockerutil as D
from ..common.record import lat_summary
from . import config as C
from . import infra
from . import judges as J
from . import workload as W
from .service import read_log

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))

# fault parameters (counts are numbers of requests attempted by the callers)
FAULT_AT = 600                 # inject when this many requests have been attempted
WRITER_CRASH_SEQS = [150, 300, 450]      # requests of the first writer of instance 0
RELAY_CRASH_AFTER = [300, 300, 300]      # rows published per incarnation before the relay dies
CONSUMER_CRASH_AFTER = [205, 205, 205]   # messages processed per incarnation before the consumer dies
LONG_TXN = {'long_txn_every': 25, 'long_txn_ms': 200}
BROKER_FAULT = {                # restore when min_s have passed and `more` further requests were attempted
    'broker_cut': {'min_s': 6.0, 'more': {'n': 12, 'b': 400, 'be': 400}},
    'broker_pause': {'min_s': 8.0, 'more': {'n': 12, 'b': 400, 'be': 400}},
    'broker_kill': {'min_s': 3.0, 'more': {'n': 8, 'b': 400, 'be': 400}},
}
QUIET_S = {'n': 5.0, 'b': 8.0, 'be': 15.0}
SETTLE_MAX_S = 150.0


class Proc:
    """A supervised client process. ``plan`` gives the crash parameter of each
    incarnation; a process that died by SIGKILL is started again."""

    def __init__(self, name, module, spec, rundir, crash_key=None, plan=()):
        self.name, self.module, self.spec, self.rundir = name, module, dict(spec), rundir
        self.crash_key, self.plan = crash_key, list(plan)
        self.inc, self.restarts, self.p = 0, 0, None

    def start(self):
        spec = dict(self.spec, incarnation=self.inc)
        if self.crash_key:
            spec[self.crash_key] = self.plan[self.inc] if self.inc < len(self.plan) else None
        out = open(os.path.join(self.rundir, f'{self.name}.{self.inc}.log'), 'w')
        self.p = subprocess.Popen([sys.executable, '-u', '-m', self.module, json.dumps(spec)], cwd=ROOT,
                                  stdout=out, stderr=subprocess.STDOUT)
        out.close()
        return self

    def tick(self):
        """Restart after an injected crash. Returns the exit code when finished."""
        rc = self.p.poll()
        if rc == -signal.SIGKILL:
            self.inc += 1
            self.restarts += 1
            self.start()
            return None
        if rc not in (None, 0, -signal.SIGTERM):
            raise RuntimeError(f'{self.name} exited with {rc}:\n{self.tail()}')
        return rc

    def stop(self, timeout=30):
        if self.p and self.p.poll() is None:
            self.p.terminate()
            try:
                self.p.wait(timeout)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait(10)

    def tail(self):
        try:
            with open(os.path.join(self.rundir, f'{self.name}.{self.inc}.log')) as f:
                return f.read()[-1500:]
        except OSError:
            return ''


def make_rundir(run_id):
    d = os.path.join(C.TMP, 'a', run_id.replace(':', '_').replace('/', '_'))
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    return d


def start_services(rundir, mode, seed, concurrency, n_ops, n_orders, crash_at=None, long_txn=None,
                   id_prefix=''):
    procs = []
    for i, ws in enumerate(W.instances(concurrency)):
        spec = {'mode': mode, 'instance': i, 'writers': ws, 'seed': seed, 'n_orders': n_orders, 'n_ops': n_ops,
                'concurrency': concurrency, 'dsn': infra.dsn('pg'), 'bootstrap': C.BOOTSTRAP, 'topic': C.TOPIC,
                'logdir': rundir, 'id_prefix': id_prefix}
        if i == 0:
            if crash_at:
                spec['crash_at'] = list(crash_at)
            if long_txn:
                spec.update(long_txn)
        procs.append(Proc(f'service-{i}', 'cs.a.service', spec, rundir).start())
    return procs


def start_consumers(rundir, idempotent, crash_plan=()):
    return [Proc(f'consumer-{role}', 'cs.a.consumer',
                 {'role': role, 'idempotent': idempotent, 'dsn': infra.dsn('pgd'), 'bootstrap': C.BOOTSTRAP},
                 rundir, crash_key='crash_at', plan=crash_plan).start() for role in J.ROLES]


def start_relay(rundir, mode, crash_plan=()):
    return Proc('relay', 'cs.a.relay', {'mode': mode, 'dsn': infra.dsn('pg'), 'bootstrap': C.BOOTSTRAP,
                                        'topic': C.TOPIC}, rundir, crash_key='crash_at', plan=crash_plan).start()


def attempted(rundir):
    n = 0
    for p in glob.glob(os.path.join(rundir, 'client-*.jsonl')):
        with open(p, 'rb') as f:
            n += f.read().count(b'\n')
    return n


def run_services(services, others, on_tick=None):
    """Supervise until every service instance has finished its request list."""
    while True:
        rcs = [s.tick() for s in services]
        for o in others:
            o.tick()
        if on_tick:
            on_tick()
        if all(rc == 0 for rc in rcs):
            return
        time.sleep(0.02)


def effect_counts():
    r = infra.q('pgd', "select (select count(*) from deliveries), "
                       "(select count(distinct event_id) from index_applies), "
                       "(select count(distinct event_id) from notifications)")[0]
    return tuple(r)


def settle(stack, config, committed_n, procs, extra_ok=None, quiet_s=None, max_s=SETTLE_MAX_S):
    """Wait until every committed event has an effect in both consumers, or until
    nothing has changed for quiet_s seconds while all components are up."""
    quiet_s = quiet_s or QUIET_S[config]
    t0 = time.monotonic()
    last_sig, last_change = None, t0
    while True:
        for p in procs:
            p.tick()
        now = time.monotonic()
        dl, idx, ntf = effect_counts()
        if idx >= committed_n and ntf >= committed_n:
            return {'reason': 'all_effects', 'seconds': round(now - t0, 2)}
        sig = (dl, idx, ntf)
        if config != 'n':
            sig += (infra.q('pg', 'select count(*) from outbox where not sent')[0][0],) if config == 'b' else ()
        ok = extra_ok() if extra_ok else True
        if sig != last_sig or not ok:
            last_sig, last_change = sig, now
        elif now - last_change >= quiet_s:
            return {'reason': 'quiet', 'seconds': round(now - t0, 2)}
        if now - t0 > max_s:
            return {'reason': 'timeout', 'seconds': round(now - t0, 2)}
        time.sleep(0.5)


def collect(stack, rundirs, expected):
    """Read every final state and judge. Returns (metrics, extra measurements)."""
    client = []
    for d in rundirs:
        for p in sorted(glob.glob(os.path.join(d, 'client-*.jsonl'))):
            client += read_log(p)
    history = infra.q('pg', 'select event_id, order_id, version from order_history')
    orders = {r[0]: (r[1], r[2], r[3]) for r in infra.q('pg', 'select id, status, last_event, version from orders')}
    log = [(p, o, k, json.loads(v)['event_id']) for p, o, k, v in stack.scan_topic()]
    deliveries, effects = {}, {}
    for role in J.ROLES:
        deliveries[role] = infra.q('pgd', 'select id, event_id, part, off from deliveries where role = %s order by id',
                                   (role,))
    applies = infra.q('pgd', 'select id, event_id, order_id, ts from index_applies order by id')
    effects['index'] = [a[:3] for a in applies]
    effects['notify'] = infra.q('pgd', 'select id, event_id, order_id from notifications order by id')
    index = {r[0]: (r[1], r[2]) for r in infra.q('pgd', 'select order_id, status, last_event from orders_index')}
    m = J.judge(expected, client, history, orders, log, deliveries, effects, index)
    # latency: commit in the system of record -> first effect in the search index (same host clock)
    first = {}
    for _id, eid, _o, ts in applies:
        first.setdefault(eid, ts)
    lag = [(first[r['e']] - r['tc']) * 1000 for r in client if r.get('tc') and r['e'] in first]
    resp = [r['ms'] for r in client if r['ack']]
    extra = {'lag_ms': lat_summary(lag), 'response_ms': lat_summary(resp),
             'failures_by_reason': _count(r.get('res') for r in client if not r['ack']),
             'index_update_count_sum': infra.q('pgd', 'select coalesce(sum(update_count), 0) from orders_index')[0][0]}
    return m, extra, (orders, index)


def _count(it):
    out = {}
    for x in it:
        out[x or 'unknown'] = out.get(x or 'unknown', 0) + 1
    return out


def reconcile(orders, index):
    """E-A3: detect differences between the system of record and the search
    index, repair the index from the system of record, and check again."""
    bad = J.reconcile_diff(orders, index)
    if bad:
        with infra.pg('pgd') as c:
            for o in bad:
                status, last_event, _ver = orders[o]
                c.execute('insert into orders_index (order_id, status, last_event, update_count) values (%s, %s, %s, 1) '
                          'on conflict (order_id) do update set status = excluded.status, '
                          'last_event = excluded.last_event', (o, status, last_event))
    index2 = {r[0]: (r[1], r[2]) for r in infra.q('pgd', 'select order_id, status, last_event from orders_index')}
    return {'detected': len(bad), 'after_repair': len(J.reconcile_diff(orders, index2))}


def run_id_of(spec):
    v = f'-{spec["variant"]}' if spec.get('variant') else ''
    return f'{spec["exp"]}:{spec["broker"]}:{spec["config"]}{v}:{spec["case"]}:c{spec["concurrency"]}:t{spec["trial"]}'


def run_one(spec, log=print):
    config, variant, case = spec['config'], spec.get('variant', ''), spec['case']
    c, seed, n_ops, n_orders = spec['concurrency'], spec['seed'], spec['n_ops'], spec['n_orders']
    run_id = run_id_of(spec)
    rundir = make_rundir(run_id)
    idempotent = config != 'n' and variant != 'noidem'
    t_start = time.monotonic()
    stack = infra.Stack(broker=spec['broker'], connect=(config == 'be'))
    procs, row = [], None
    try:
        stack.up(n_orders=n_orders)
        connector = None
        if config == 'be':
            connector = stack.register_connector(key_field='event_id' if variant == 'nokey' else 'aggregateid')
        consumers, relay = [], None
        cplan = CONSUMER_CRASH_AFTER if case == 'consumer_kill' else ()
        if case != 'consumer_late':
            consumers = start_consumers(rundir, idempotent, cplan)
        if config == 'b':
            relay = start_relay(rundir, 'watermark' if variant == 'wm' else 'unsent',
                                RELAY_CRASH_AFTER if case == 'relay_kill' else ())
        procs = consumers + ([relay] if relay else [])
        time.sleep(1.0)                      # consumers and relay connected before the first request
        stack.t0 = time.monotonic()
        services = start_services(rundir, 'n' if config == 'n' else 'outbox', seed, c, n_ops, n_orders,
                                  crash_at=WRITER_CRASH_SEQS if case == 'writer_kill' else None,
                                  long_txn=LONG_TXN if case == 'longtxn' else None)
        fault = {'state': 'idle', 't': None, 'at': None}

        def on_tick():
            if case not in BROKER_FAULT and not (case == 'relay_kill' and config == 'be'):
                return
            n = attempted(rundir)
            if fault['state'] == 'idle' and n >= FAULT_AT:
                fault.update(state='on', t=time.monotonic(), at=n)
                if case == 'broker_cut':
                    stack.broker_cut(f'attempted={n}')
                elif case == 'broker_pause':
                    stack.broker_pause(f'attempted={n}')
                elif case == 'broker_kill':
                    stack.broker_kill(f'attempted={n}')
                else:
                    stack.connect_kill(f'attempted={n}')
                    stack.connect_start(f'attempted={attempted(rundir)}')
                    fault['state'] = 'done'
            elif fault['state'] == 'on':
                f = BROKER_FAULT[case]
                if time.monotonic() - fault['t'] >= f['min_s'] and n >= fault['at'] + f['more'][config]:
                    restore(n)

        def restore(n):
            if case == 'broker_cut':
                stack.broker_restore(f'attempted={n}')
            elif case == 'broker_pause':
                stack.broker_unpause(f'attempted={n}')
            elif case == 'broker_kill':
                stack.broker_start(f'attempted={n}')
            fault['state'] = 'done'

        run_services(services, procs, on_tick)
        writers_s = round(time.monotonic() - stack.t0, 2)
        stack.mark('writers_done', f'attempted={attempted(rundir)}')
        if fault['state'] == 'on':           # the callers finished while the fault was still active
            f = BROKER_FAULT[case]
            while time.monotonic() - fault['t'] < f['min_s']:
                for p in procs:
                    p.tick()
                time.sleep(0.1)
            restore(attempted(rundir))
        if case == 'consumer_late':
            if config == 'b':                # let the relay drain first: the whole backlog waits in the broker
                D.wait_ready('dic-a-pg', lambda: infra.q('pg', 'select count(*) from outbox where not sent')[0][0] == 0,
                             timeout=60)
            elif config == 'be':
                want = infra.q('pg', 'select count(*) from outbox')[0][0]
                D.wait_ready(C.CONNECT, lambda: sum(stack.high_watermarks()) >= want, timeout=90)
            stack.mark('consumers_start', 'after the writers finished')
            consumers = start_consumers(rundir, idempotent)
            procs = consumers + procs
        committed_n = infra.q('pg', 'select count(*) from order_history')[0][0]
        extra_ok = (lambda: stack.connector_state()['task'] == 'RUNNING') if config == 'be' else None
        settled = settle(stack, config, committed_n, procs, extra_ok)
        cstate = stack.connector_state() if config == 'be' else None
        mem = stack.mem()
        for p in procs:
            p.stop()
        expected = W.expected(seed, n_orders, n_ops, c)
        m, extra, (orders, index) = collect(stack, [rundir], expected)
        rec = reconcile(orders, index)
        row = {
            'run_id': run_id, **{k: spec[k] for k in ('exp', 'broker', 'config', 'case', 'concurrency', 'trial',
                                                      'seed', 'n_ops', 'n_orders')},
            'variant': variant, 'idempotent_consumer': idempotent, 'metrics': m, **extra, 'reconcile': rec,
            'settled': settled, 'writers_seconds': writers_s, 'events': stack.events,
            'restarts': {p.name: p.restarts for p in services + procs if p.restarts},
            'connector_state': cstate, 'versions': stack.info, 'container_memory': mem,
            'connector_config': None if connector is None else {k: v for k, v in connector.items() if 'password' not in k},
            'seconds': round(time.monotonic() - t_start, 1),
            'settings': {'partitions': C.PARTITIONS, 'producer_timeout_ms': C.PRODUCER_TIMEOUT_MS,
                         'consumer_commit_every': C.CONSUMER_COMMIT_EVERY, 'relay_batch': C.RELAY_BATCH,
                         'relay_poll_ms': C.RELAY_POLL_MS,
                         'connect_offset_flush_ms': C.CONNECT_OFFSET_FLUSH_MS if config == 'be' else None},
        }
        shutil.rmtree(rundir, ignore_errors=True)
        return row
    finally:
        for p in procs:
            try:
                p.stop(5)
            except Exception:  # noqa: BLE001
                pass
        if row is None:
            log(f'run {run_id} failed; client logs kept in {os.path.basename(rundir)}')
        stack.down()
