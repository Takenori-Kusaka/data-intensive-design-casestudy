"""B-5: what has to hold before derived data can be rebuilt from the log - retention,
snapshots, compaction and delete markers (tombstones). Runs on Redpanda and, for the
cross-check, on Apache Kafka.

  retention   cleanup.policy=delete. Phase 1 of the events is produced, a snapshot of the
              derived store is taken at a known offset, the old segments expire (retention.ms
              scaled down to 5 s; 'default' leaves the product default), phase 2 is produced,
              and the store is rebuilt:
                log_only     from the earliest offset that is left (BE - snapshot)
                snap_pos     snapshot + the log from the offset recorded with the snapshot
                snap_latest  snapshot + the log from its end (the offset was not recorded)
                default      product default retention, from the beginning
  compact     cleanup.policy=compact: how many records survive, whether offsets and order
              are kept, and what a rebuild gets (latest state / number of events)
  tombstone   compact topic with deletes. A follower that has read the orders is stopped, the
              orders are deleted (tombstones), the tombstones expire (delete.retention.ms
              scaled down to 3 s; 'default' leaves the product default), the follower resumes.
              Also in every run: a follower that never stops, a follower without delete
              handling, and a rebuild from the beginning at the end.
  cdc_delete  PostgreSQL DELETE -> Debezium (defaults) -> topic -> derived store: what is
              emitted for a delete and how long it takes to arrive

Time is scaled: 5 s stands for the 7 days of retention.ms and 3 s for the 1 day of
delete.retention.ms. Filler records (keys starting with "f") are produced once a second so
that segments roll and the cleaner has something to do; followers skip them.
"""
import json
import random
import time

from ..common import dockerutil as D
from ..common.record import lat_summary
from . import config as C, derive as V, infra

TOPIC = 'b5.orders'
N_ORDERS = 500
PAD = 'x' * 200
SEG = {'segment.bytes': 1048576, 'segment.ms': 2000}
RETENTION_MS, TOMBSTONE_MS = 5000, 3000
TRIALS = 3
KAFKA_ENV = {'KAFKA_LOG_CLEANER_BACKOFF_MS': '1000', 'KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS': '1000',
             'KAFKA_LOG_CLEANER_DELETE_RETENTION_MS': None}
REPORTED_KEYS = ['cleanup.policy', 'retention.ms', 'retention.bytes', 'delete.retention.ms', 'segment.bytes',
                 'segment.ms', 'min.cleanable.dirty.ratio', 'min.compaction.lag.ms']
CASES = (
    [('redpanda', 'retention', v) for v in ('default', 'log_only', 'snap_pos', 'snap_latest')]
    + [('redpanda', 'compact', 'compact'), ('redpanda', 'tombstone', 'short'), ('redpanda', 'tombstone', 'default'),
       ('kafka', 'retention', 'log_only'), ('kafka', 'compact', 'compact'), ('kafka', 'tombstone', 'short'),
       ('kafka', 'tombstone', 'default'), ('redpanda', 'cdc_delete', 'debezium')])
VARIANT_JA = {'default': '製品の既定のまま', 'log_only': 'ログだけから（スナップショットなし）',
              'snap_pos': 'スナップショット＋記録した位置からのログ', 'snap_latest': 'スナップショット＋ログの末尾から（位置の記録なし）',
              'compact': 'コンパクション', 'short': '削除の印の保持を 3 秒に縮める', 'debezium': 'Debezium の既定'}


def plan():
    return [{'exp': 'b5', 'broker': b, 'case': c, 'variant': v, 'trial': t, 'seed': C.SEED0 + t}
            for b, c, v in CASES for t in range(1, TRIALS + 1)]


def run_id_of(s):
    return f'b5:{s["broker"]}:{s["case"]}:{s["variant"]}:t{s["trial"]}'


def enc(ev):
    return str(ev[0]), json.dumps({'order_id': ev[0], 'ver': ev[1], 'status': ev[2], 'pad': PAD}).encode()


def tombstone_alarm(lag_age_ms, delete_retention_ms):
    """E-B3: a follower that has been behind for longer than the tombstones are kept may
    have missed deletes. None or a negative retention means tombstones are never removed."""
    if delete_retention_ms is None or delete_retention_ms < 0:
        return False
    return lag_age_ms >= delete_retention_ms


def log_state(events=None):
    """What is in the topic now (filler records are not counted)."""
    recs = [r for r in infra.scan_topic(TOPIC) if not r[2].startswith('f')]
    lo, hi = infra.watermarks(TOPIC)[0]
    out = {'records': len(recs), 'tombstones': sum(1 for r in recs if r[3] is None), 'low': lo, 'high': hi}
    if events is not None:        # offsets and order of what survived (events were produced first, in order)
        moved, last, disorder = 0, {}, 0
        for _p, off, key, val in recs:
            if val is None:
                continue
            d = json.loads(val)
            if off >= len(events) or (events[off][0], events[off][1]) != (d['order_id'], d['ver']):
                moved += 1
            if d['ver'] < last.get(key, 0):
                disorder += 1
            last[key] = d['ver']
        out.update({'offset_changed': moved, 'order_violations': disorder})
    return out


def log_judge(recs, events):
    """Pure part of the offset / order check, for the audit: recs = [(offset, order, ver)]."""
    moved = sum(1 for off, o, v in recs if off >= len(events) or (events[off][0], events[off][1]) != (o, v))
    last, disorder = {}, 0
    for _off, o, v in recs:
        if v < last.get(o, 0):
            disorder += 1
        last[o] = v
    return {'offset_changed': moved, 'order_violations': disorder}


def audit_cases():
    ev = [(1, 1, 'a'), (2, 1, 'a'), (1, 2, 'b'), (2, 2, 'b'), (1, 3, 'c')]
    return [
        ('alarm_over', lambda: tombstone_alarm(5000, 3000), True),
        ('alarm_under', lambda: tombstone_alarm(5000, 86400000), False),
        ('alarm_never_removed', lambda: tombstone_alarm(5000, -1), False),
        ('log_clean_after_compaction', lambda: log_judge([(3, 2, 2), (4, 1, 3)], ev), {}),
        ('log_offset_changed_1_disorder_1', lambda: log_judge([(0, 1, 3), (2, 1, 2), (3, 2, 2)], ev),
         {'offset_changed': 1, 'order_violations': 1}),
    ]


class Churn:
    """Produces one filler record a second and samples the log until a condition holds."""

    def __init__(self):
        self.p = infra.producer()
        self.n = 0
        self.series = []
        self.t0 = time.monotonic()

    def until(self, cond, timeout, min_s=0, events=None):
        t0 = time.monotonic()
        while True:
            self.n += 1
            self.p.produce(TOPIC, key=f'f{self.n}', value=PAD.encode())
            self.p.flush(10)
            st = log_state()
            st['t'] = round(time.monotonic() - self.t0, 1)
            self.series.append(st)
            el = time.monotonic() - t0
            if el >= min_s and cond(st):
                return True
            if el > timeout:
                return False
            time.sleep(1.0)


def _broker(kind):
    if kind == 'kafka':
        infra.broker_up('kafka', kafka_env={k: v for k, v in KAFKA_ENV.items() if v is not None})
    else:
        infra.broker_up('redpanda')
        infra.rp_set('log_compaction_interval_ms', '1000')      # housekeeping (retention and compaction)
        infra.rp_set('log_segment_ms_min', '1000')              # allow segment.ms below the default floor
    return infra.broker_version(kind)


def _reported():
    cfg = infra.topic_config(TOPIC, REPORTED_KEYS)
    return {k: cfg.get(k) for k in REPORTED_KEYS}


def _int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def _follow(group, table, **kw):
    f = V.Follower(TOPIC, 1, group, table, **kw)
    f.start()
    return f


# ------------------------------------------------------------------ retention
def run_retention(spec):
    var = spec['variant']
    cfg = {'cleanup.policy': 'delete', **SEG}
    if var != 'default':
        cfg['retention.ms'] = RETENTION_MS
    infra.create_topic(TOPIC, 1, cfg)
    # orders 1..100 are old: all their events are in the first 2000 records
    events = V.gen_events(spec['seed'], 2000, 100) + [(o + 100, v, st) for o, v, st in
                                                      V.gen_events(spec['seed'] + 7, 10000, N_ORDERS - 100)]
    n1 = 8000
    infra.produce_all(TOPIC, [enc(e) for e in events[:n1]])
    V.create_store('idx_live')
    live = _follow('dic-b-live', 'idx_live')
    live, _ = V.wait_caught_up(live)
    live.stop()
    snap_pos = dict(live.pos)                       # the snapshot and the offset it corresponds to
    infra.q('pg2', 'create table snap as select * from idx_live')
    ch = Churn()
    if var == 'default':        # wait as long as the other variants do; nothing should expire
        ch.until(lambda st: True, timeout=20, min_s=15)
        expired = ch.series[-1]['low'] > 0
    else:
        expired = ch.until(lambda st: st['low'] > 0, timeout=70)
    infra.produce_all(TOPIC, [enc(e) for e in events[n1:]])
    state = log_state()
    V.create_store('idx_new')
    if var.startswith('snap'):
        infra.q('pg2', 'insert into idx_new select * from snap')
    start = {'snap_pos': snap_pos, 'snap_latest': {0: state['high']}}.get(var, {0: 0})
    t0 = time.monotonic()
    new = _follow('dic-b-new', 'idx_new', start=start)
    new, _ = V.wait_caught_up(new)
    new.stop()
    rebuild_s = round(time.monotonic() - t0, 2)
    final = V.judge_store(*V.truth_of(events), V.read_store('idx_new')[0])
    return {'topic_config_set': cfg, 'topic_config_reported': _reported(), 'events': len(events),
            'snapshot_offset': snap_pos.get(0), 'expired': bool(expired), 'log': state, 'series': ch.series[-3:],
            'waited_s': ch.series[-1]['t'], 'rebuild_from': start.get(0), 'rebuild_s': rebuild_s, 'final': final,
            'brief': f'low={state["low"]} left={state["records"]}/{len(events)} from={start.get(0)} '
                     f'missing={final["missing"]} state={final["state_mismatch"]} count={final["count_mismatch"]}'}


# -------------------------------------------------------------------- compact
def run_compact(spec):
    cfg = {'cleanup.policy': 'compact', **SEG, 'min.cleanable.dirty.ratio': 0.01}
    infra.create_topic(TOPIC, 1, cfg)
    events = V.gen_events(spec['seed'], 12000, N_ORDERS)
    infra.produce_all(TOPIC, [enc(e) for e in events])
    before = log_state(events)
    ch = Churn()
    stable = []

    def cond(st):
        stable.append(st['records'])
        return st['records'] <= len(events) // 2 and len(stable) >= 5 and len(set(stable[-5:])) == 1
    compacted = ch.until(cond, timeout=90)
    after = log_state(events)
    V.create_store('idx_new', 'agg_new')
    t0 = time.monotonic()
    new = _follow('dic-b-new', 'idx_new', agg='agg_new', start={0: 0})
    new, _ = V.wait_caught_up(new)
    new.stop()
    final = V.judge_store(*V.truth_of(events), *V.read_store('idx_new', 'agg_new'))
    return {'topic_config_set': cfg, 'topic_config_reported': _reported(), 'events': len(events),
            'compacted': compacted, 'log_before': before, 'log_after': after, 'waited_s': ch.series[-1]['t'],
            'rebuild_s': round(time.monotonic() - t0, 2), 'final': final,
            'brief': f'records {before["records"]}->{after["records"]} offset_changed={after["offset_changed"]} '
                     f'order={after["order_violations"]} state={final["state_mismatch"]} '
                     f'count={final["count_mismatch"]} agg_deficit={final["agg_deficit"]}'}


# ------------------------------------------------------------------ tombstone
def run_tombstone(spec):
    var = spec['variant']
    rng = random.Random(spec['seed'])
    cfg = {'cleanup.policy': 'compact', **SEG, 'min.cleanable.dirty.ratio': 0.01}
    if var == 'short':
        cfg['delete.retention.ms'] = TOMBSTONE_MS
    infra.create_topic(TOPIC, 1, cfg)
    reported = _reported()
    events = V.gen_events(spec['seed'], 6000, N_ORDERS)
    infra.produce_all(TOPIC, [enc(e) for e in events])
    stores = {'fast': {}, 'nodel': {'deletes': False}, 'slow': {}}
    fol = {}
    for name, kw in stores.items():
        V.create_store(f'idx_{name}')
        fol[name] = _follow(f'dic-b-{name}', f'idx_{name}', **kw)
    for name in fol:
        fol[name], _ = V.wait_caught_up(fol[name])
    fol['slow'].stop()                                # the follower that falls behind
    t_stop = time.monotonic()
    deleted = sorted(rng.sample(range(1, N_ORDERS + 1), 100))
    infra.produce_all(TOPIC, [(str(o), None) for o in deleted])
    for name in ('fast', 'nodel'):
        fol[name], _ = V.wait_caught_up(fol[name])
    delete_lat = lat_summary(fol['fast'].delete_lat_ms)
    ch = Churn()
    if var == 'short':
        gone = ch.until(lambda st: st['tombstones'] == 0, timeout=80)
    else:
        ch.until(lambda st: True, timeout=45, min_s=40)        # the same order of waiting; nothing should expire
        gone = ch.series[-1]['tombstones'] == 0
    state = log_state()
    lag_age_ms = round((time.monotonic() - t_stop) * 1000)
    alarm = tombstone_alarm(lag_age_ms, _int(reported.get('delete.retention.ms')))
    fol['slow'] = _follow('dic-b-slow', 'idx_slow')           # resumes from its committed offsets
    V.create_store('idx_fresh')
    fol['fresh'] = _follow('dic-b-fresh', 'idx_fresh', start={0: 0})
    truth_orders, truth_counts = V.truth_of(events, deleted=deleted)
    final = {}
    for name in fol:
        fol[name], _ = V.wait_caught_up(fol[name])
        fol[name].stop()
        j = V.judge_store(truth_orders, truth_counts, V.read_store(f'idx_{name}')[0])
        final[name] = {'deleted_left': j['extra'], 'missing': j['missing'], 'state_mismatch': j['state_mismatch'],
                       'tombstones_seen': fol[name].tombstones_seen}
    first_gone = next((s['t'] for s in ch.series if s['tombstones'] == 0), None)
    return {'topic_config_set': cfg, 'topic_config_reported': reported, 'events': len(events), 'deleted': len(deleted),
            'tombstones_gone': gone, 'tombstones_gone_after_s': first_gone, 'log': state, 'waited_s': ch.series[-1]['t'],
            'slow_lag_age_ms': lag_age_ms, 'alarm': alarm, 'delete_latency_fast': delete_lat, 'final': final,
            'series': [ch.series[i] for i in sorted({0, len(ch.series) // 2, len(ch.series) - 1})],
            'brief': f'drm={reported.get("delete.retention.ms")} gone={gone}@{first_gone}s tomb_left={state["tombstones"]} '
                     f'alarm={alarm} left: slow={final["slow"]["deleted_left"]} fast={final["fast"]["deleted_left"]} '
                     f'nodel={final["nodel"]["deleted_left"]} fresh={final["fresh"]["deleted_left"]} '
                     f'del_p50={delete_lat["p50_ms"]}ms'}


# ----------------------------------------------------------------- cdc delete
CDC_TOPIC = 'dicb.public.orders_src'
CONNECTOR = {
    'connector.class': 'io.debezium.connector.postgresql.PostgresConnector', 'tasks.max': '1',
    'plugin.name': 'pgoutput', 'database.hostname': C.PG, 'database.port': '5432', 'database.user': 'postgres',
    'database.password': C.PASSWORD, 'database.dbname': 'postgres', 'topic.prefix': 'dicb',
    'table.include.list': 'public.orders_src', 'publication.name': 'dic_b_pub',
    'publication.autocreate.mode': 'filtered', 'slot.name': 'dic_b_slot', 'snapshot.mode': 'no_data',
    'topic.creation.default.replication.factor': '1', 'topic.creation.default.partitions': '1',
    'key.converter': 'org.apache.kafka.connect.json.JsonConverter', 'key.converter.schemas.enable': 'false',
    'value.converter': 'org.apache.kafka.connect.json.JsonConverter', 'value.converter.schemas.enable': 'false',
}       # tombstones.on.delete is deliberately not set (default)


def run_cdc_delete(spec):
    rng = random.Random(spec['seed'])
    infra.pg_up('pg', args=['-c', 'wal_level=logical', '-c', 'max_wal_senders=4', '-c', 'max_replication_slots=4'])
    infra.connect_up()
    infra.q('pg', 'create table orders_src (id int primary key, status text not null)')
    infra.q('pg2', 'create table idx_cdc (id int primary key, status text not null)')
    infra.connect_wait()
    st, body = infra.http('PUT', infra.connect_url('/connectors/dic-b-src/config'), CONNECTOR, timeout=30)
    if st not in (200, 201):
        raise D.CmdError(f'connector registration failed: {st} {body}')
    D.wait_ready(C.CONNECT, lambda: infra.connector_task_state('dic-b-src') == 'RUNNING', timeout=120)
    D.wait_ready(C.PG, lambda: infra.q('pg', "select active from pg_replication_slots where slot_name = 'dic_b_slot'")[0][0],
                 timeout=60)
    n_rows, n_del = 200, 50
    infra.q('pg', "insert into orders_src select g, 'accepted' from generate_series(1, %s) g", (n_rows,))
    D.wait_ready(C.BROKER, lambda: CDC_TOPIC in infra.admin().list_topics(timeout=3).topics, timeout=60)
    deleted = sorted(rng.sample(range(1, n_rows + 1), n_del))
    t_commit = {}
    with infra.pg('pg') as c:
        for i in deleted:
            c.execute('delete from orders_src where id = %s', (i,))
            t_commit[i] = time.time()
    cons = infra.consumer('dic-b-cdc')
    cons.subscribe([CDC_TOPIC])
    conn = infra.pg('pg2')
    lat, seen = [], {'c': 0, 'd': 0, 'tombstone': 0, 'other': 0}
    t0 = time.monotonic()
    while seen['c'] < n_rows or seen['d'] < n_del or seen['tombstone'] < n_del:
        if time.monotonic() - t0 > 60:
            break
        for m in cons.consume(200, timeout=0.5):
            if m.error():
                continue
            key = json.loads(m.key())['id']
            if m.value() is None:
                seen['tombstone'] += 1
                continue
            v = json.loads(m.value())
            if v['op'] == 'd':
                seen['d'] += 1
                conn.execute('delete from idx_cdc where id = %s', (key,))
                lat.append((time.time() - t_commit[key]) * 1000)
            elif v['op'] in ('c', 'r', 'u'):
                seen['c'] += 1
                conn.execute('insert into idx_cdc values (%s, %s) on conflict (id) do update set status = excluded.status',
                             (key, v['after']['status']))
            else:
                seen['other'] += 1
    cons.close()
    conn.close()
    recs = infra.scan_topic(CDC_TOPIC)
    pairs = sum(1 for a, b in zip(recs, recs[1:]) if a[3] is not None and json.loads(a[3])['op'] == 'd'
                and b[3] is None and a[2] == b[2])
    left = {r[0] for r in infra.q('pg2', 'select id from idx_cdc')}
    return {'rows': n_rows, 'deleted': n_del, 'connector_config': CONNECTOR, 'tombstones_on_delete_set': False,
            'topic': {'records': len(recs), 'delete_events': sum(1 for r in recs if r[3] and json.loads(r[3])['op'] == 'd'),
                      'tombstones': sum(1 for r in recs if r[3] is None), 'delete_followed_by_tombstone': pairs},
            'topic_config_reported': {k: v for k, v in infra.topic_config(CDC_TOPIC, REPORTED_KEYS).items()},
            'consumed': seen, 'delete_latency': lat_summary(lat),
            'final': {'deleted_left': len(left & set(deleted)), 'missing': len(set(range(1, n_rows + 1)) - set(deleted) - left)},
            'brief': f'consumed={seen} left={len(left & set(deleted))} lat={lat_summary(lat)}'}


def run_one(spec, log=print):
    t0 = time.monotonic()
    infra.down()
    infra.pg_up('pg2', args=['-c', 'synchronous_commit=off'], wait=False)
    ver = _broker(spec['broker'])
    infra.pg_wait('pg2')
    body = {'retention': run_retention, 'compact': run_compact, 'tombstone': run_tombstone,
            'cdc_delete': run_cdc_delete}[spec['case']](spec)
    row = {'run_id': run_id_of(spec), **spec, **body, 'scaled': {'retention_ms': RETENTION_MS, 'tombstone_ms': TOMBSTONE_MS},
           'versions': {spec['broker']: ver, 'pg': infra.q('pg2', 'show server_version')[0][0]},
           'container_memory': infra.mem()}
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row
