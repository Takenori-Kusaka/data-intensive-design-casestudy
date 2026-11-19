"""CDC pitfall: the consumer of the replication slot (Debezium) is stopped while
the order service keeps writing. How much WAL does the primary keep, and what
happens to the slot and to the events when a limit is set?

Conditions
  default  max_slot_wal_keep_size = -1 (PostgreSQL default: no limit)
  cap      max_slot_wal_keep_size = 64MB
  idle     idle_replication_slot_timeout = 20s (new in PostgreSQL 18; default 0 = off)

Timeline of one run (configuration BE, idempotent consumers)
  phase p1   400 requests with Debezium running; wait until all have an effect
  stop       docker stop of Kafka Connect (graceful)
  steps      STEPS times: 100 requests + WAL filler + CHECKPOINT + one sample of the slot
  start      docker start of Kafka Connect; observe the connector; judge (a)
  recovery   only when the slot was invalidated: drop the slot, register a new
             connector with snapshot.mode=initial, 100 more requests (p3); judge (b)
  reconcile  compare the system of record with the search index and repair
"""
import time

from ..common import dockerutil as D
from . import cases, config as C, infra
from . import workload as W

CONDITIONS = {
    'default': {'pg_args': ['-c', 'max_slot_wal_keep_size=-1'], 'filler_mb': 32, 'sleep_s': 0, 'steps': 8},
    'cap': {'pg_args': ['-c', 'max_slot_wal_keep_size=64MB'], 'filler_mb': 32, 'sleep_s': 0, 'steps': 8},
    'idle': {'pg_args': ['-c', 'idle_replication_slot_timeout=20s'], 'filler_mb': 4, 'sleep_s': 4, 'steps': 8},
}
P1_OPS, STEP_OPS, P3_OPS, CONC = 400, 100, 100, 4
RECOVERY_CONNECTOR, RECOVERY_SLOT = 'dic-a-outbox-r', 'dic_a_slot_r'

SAMPLE_SQL = """
select s.wal_status, s.active, s.restart_lsn is not null,
       pg_wal_lsn_diff(pg_current_wal_lsn(), s.restart_lsn)::bigint,
       s.safe_wal_size, s.invalidation_reason, s.inactive_since is not null,
       (select sum(size) from pg_ls_waldir())::bigint, pg_current_wal_lsn()::text
from pg_replication_slots s where s.slot_name = %s
"""


def sample(label, slot=C.SLOT, stop_lsn=None):
    r = infra.q('pg', SAMPLE_SQL, (slot,))
    if not r:
        return {'label': label, 'slot': None}
    r = r[0]
    out = {'label': label, 'wal_status': r[0], 'active': r[1], 'has_restart_lsn': r[2],
           'retained_by_slot_mb': None if r[3] is None else round(r[3] / 2**20, 1),
           'safe_wal_size_mb': None if r[4] is None else round(r[4] / 2**20, 1),
           'invalidation_reason': r[5], 'inactive': r[6], 'pg_wal_dir_mb': round(r[7] / 2**20, 1), 'lsn': r[8]}
    if stop_lsn:
        out['wal_since_stop_mb'] = round(float(infra.q('pg', 'select pg_wal_lsn_diff(%s::pg_lsn, %s::pg_lsn)',
                                                       (r[8], stop_lsn))[0][0]) / 2**20, 1)
    return out


def gen_wal(mb):
    """Write about ``mb`` MB of WAL into a table that is not captured."""
    with infra.pg('pg') as c:
        c.execute('truncate filler')
        start = c.execute('select pg_current_wal_lsn()').fetchone()[0]
        while c.execute('select pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)', (start,)).fetchone()[0] < mb * 2**20:
            c.execute("insert into filler (pad) select repeat(md5(g::text || random()::text), 30) "
                      "from generate_series(1, 4000) g")


def connect_log_excerpt(n=3):
    """First WARN/ERROR lines of the Kafka Connect log that mention the replication slot."""
    r = D.docker('logs', '--since', '10m', C.CONNECT, check=False)
    out = []
    for ln in (r.stdout + r.stderr).splitlines():
        if (' ERROR ' in ln or ' WARN ' in ln) and ('slot' in ln.lower() or 'wal' in ln.lower()):
            ln = ' '.join(ln.split())[:400]
            if ln[30:] not in [x[30:] for x in out]:
                out.append(ln)
        if len(out) >= n:
            break
    return out


def missing_by_phase():
    committed = [r[0] for r in infra.q('pg', 'select event_id from order_history')]
    applied = {r[0] for r in infra.q('pgd', 'select distinct event_id from index_applies')}
    out = {}
    for e in committed:
        ph = e.split('-')[0]
        ph = 'steps' if ph.startswith('s') else ph
        d = out.setdefault(ph, {'committed': 0, 'missing': 0})
        d['committed'] += 1
        d['missing'] += 0 if e in applied else 1
    return out


def run_one(spec, log=print):
    cond, trial, seed = spec['cond'], spec['trial'], spec['seed']
    cfg = CONDITIONS[cond]
    run_id = f'wal:{spec["broker"]}:{cond}:t{trial}'
    t_start = time.monotonic()
    stack = infra.Stack(broker=spec['broker'], connect=True, pg_args=cfg['pg_args'])
    procs, rundirs, expected, row = [], [], {}, None

    def phase(prefix, n_ops, seed_):
        rd = cases.make_rundir(f'{run_id}-{prefix}')
        rundirs.append(rd)
        expected.update(W.expected(seed_, C.N_ORDERS, n_ops, CONC, id_prefix=prefix))
        sv = cases.start_services(rd, 'outbox', seed_, CONC, n_ops, C.N_ORDERS, id_prefix=prefix)
        cases.run_services(sv, procs)

    def committed_n():
        return infra.q('pg', 'select count(*) from order_history')[0][0]

    try:
        stack.up()
        stack.register_connector()
        settings = dict(infra.q('pg', "select name, setting || coalesce(unit, '') from pg_settings where name in "
                                      "('max_slot_wal_keep_size', 'idle_replication_slot_timeout', 'wal_segment_size', "
                                      "'max_wal_size', 'min_wal_size', 'wal_level')"))
        procs = cases.start_consumers(cases.make_rundir(f'{run_id}-consumers'), True)
        time.sleep(1.0)
        stack.t0 = time.monotonic()
        phase('p1-', P1_OPS, seed)
        s1 = cases.settle(stack, 'be', committed_n(), procs)
        samples = [sample('before_stop')]
        stack.connect_stop(f'committed={committed_n()}')
        t_stop = time.monotonic()
        stop_lsn = infra.q('pg', 'select pg_current_wal_lsn()::text')[0][0]
        samples.append(sample('stopped', stop_lsn=stop_lsn))
        for k in range(1, cfg['steps'] + 1):
            phase(f's{k:02d}-', STEP_OPS, seed + k)
            gen_wal(cfg['filler_mb'])
            if cfg['sleep_s']:
                time.sleep(cfg['sleep_s'])
            infra.q('pg', 'checkpoint')
            s = sample(f'step{k:02d}', stop_lsn=stop_lsn)
            s['seconds_since_stop'] = round(time.monotonic() - t_stop, 1)
            samples.append(s)
        stack.mark('writes_while_stopped_done', f'committed={committed_n()}')
        before_start = samples[-1]
        invalid = before_start.get('invalidation_reason') is not None or before_start.get('wal_status') == 'lost'

        stack.connect_start(f'committed={committed_n()}')
        t_restart = time.monotonic()
        # the task either keeps running and catches up, or fails
        state = None
        while time.monotonic() - t_restart < 45:
            for p in procs:
                p.tick()
            state = stack.connector_state()
            if state['task'] == 'FAILED':
                break
            if state['task'] == 'RUNNING' and cases.effect_counts()[1] >= committed_n():
                break
            time.sleep(1.0)
        catchup_s = round(time.monotonic() - t_restart, 1)
        s2 = cases.settle(stack, 'be', committed_n(), procs, quiet_s=10.0, max_s=60)
        state = stack.connector_state()
        state['log_excerpt'] = connect_log_excerpt()
        m_a, extra_a, (orders, index) = cases.collect(stack, rundirs, dict(expected))
        row = {
            'run_id': run_id, 'exp': 'wal', 'broker': spec['broker'], 'cond': cond, 'trial': trial, 'seed': seed,
            'pg_settings': settings, 'pg_args': cfg['pg_args'], 'filler_mb_per_step': cfg['filler_mb'],
            'sleep_s_per_step': cfg['sleep_s'], 'ops': {'p1': P1_OPS, 'per_step': STEP_OPS, 'steps': cfg['steps'],
                                                        'p3': P3_OPS, 'concurrency': CONC},
            'samples': samples, 'slot_invalid_before_restart': invalid,
            'after_restart': {'connector_state': state, 'seconds_observed': catchup_s, 'settled': s2,
                              'slot': sample('after_restart', stop_lsn=stop_lsn), 'metrics': m_a,
                              'missing_by_phase': missing_by_phase(),
                              'would_detect_by_reconcile': len(cases.J.reconcile_diff(orders, index))},
            'settled_p1': s1,
        }
        if state['task'] == 'FAILED' or invalid:
            # recovery by an operator: new slot + snapshot of the outbox table
            stack.delete_connector()
            D.wait_ready(C.PG, lambda: infra.q('pg', 'select count(*) from pg_replication_slots where active')[0][0] == 0,
                         timeout=60)
            infra.q('pg', 'select pg_drop_replication_slot(%s)', (C.SLOT,))
            stack.mark('slot_dropped', C.SLOT)
            rec = {'snapshot_mode': 'initial', 'error': None}
            try:
                stack.register_connector(name=RECOVERY_CONNECTOR, snapshot_mode='initial', slot=RECOVERY_SLOT)
                stack.mark('recovery_connector_running', RECOVERY_CONNECTOR)
                phase('p3-', P3_OPS, seed + 100)
                rec['settled'] = cases.settle(
                    stack, 'be', committed_n(), procs, quiet_s=15.0,
                    extra_ok=lambda: stack.connector_state(RECOVERY_CONNECTOR)['task'] == 'RUNNING')
            except Exception as e:  # noqa: BLE001 - record what happened, do not invent a result
                rec['error'] = str(e)[:600]
            rec['connector_state'] = stack.connector_state(RECOVERY_CONNECTOR)
            m_b, _extra_b, (orders, index) = cases.collect(stack, rundirs, dict(expected))
            rec.update(metrics=m_b, missing_by_phase=missing_by_phase(),
                       slot=sample('after_recovery', slot=RECOVERY_SLOT))
            row['recovery'] = rec
        else:
            # healthy catch-up: let the connector confirm its position, then look at the WAL again
            time.sleep(C.CONNECT_OFFSET_FLUSH_MS / 1000 + 3)
            infra.q('pg', 'checkpoint')
            infra.q('pg', 'checkpoint')
            row['after_catchup'] = sample('after_catchup', stop_lsn=stop_lsn)
            row['recovery'] = None
        row['container_memory'] = stack.mem()
        for p in procs:
            p.stop()
        orders = {r[0]: (r[1], r[2], r[3]) for r in infra.q('pg', 'select id, status, last_event, version from orders')}
        index = {r[0]: (r[1], r[2]) for r in infra.q('pgd', 'select order_id, status, last_event from orders_index')}
        row['reconcile'] = cases.reconcile(orders, index)
        row.update(events=stack.events, versions=stack.info, seconds=round(time.monotonic() - t_start, 1),
                   settings={'connect_offset_flush_ms': C.CONNECT_OFFSET_FLUSH_MS, 'partitions': C.PARTITIONS})
        return row
    finally:
        for p in procs:
            try:
                p.stop(5)
            except Exception:  # noqa: BLE001
                pass
        if row is None:
            log(f'run {run_id} failed')
        stack.down()
