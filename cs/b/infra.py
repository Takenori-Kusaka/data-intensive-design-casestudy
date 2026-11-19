"""Containers of topic B: start, wait, remove. Everything is named dic-b-*.

  host clients --127.0.0.1:25632--> dic-b-pg     (system of record)
  host clients --127.0.0.1:25633--> dic-b-pg2    (derived data / new system of record)
  host clients --127.0.0.1:25692--> dic-b-broker (Redpanda or Apache Kafka, EXTERNAL listener)
  host clients --127.0.0.1:25681--> dic-b-broker (Redpanda schema registry)
  host clients --127.0.0.1:25683--> dic-b-connect (Kafka Connect with Debezium)
  host clients --127.0.0.1:25623--> dic-b-ch     (ClickHouse HTTP)
"""
import json
import time
import urllib.error
import urllib.parse
import urllib.request

import psycopg
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from ..common import dockerutil as D
from . import config as C


def down():
    return D.cleanup(C.PREFIX)


def http(method, url, body=None, timeout=10, ctype='application/json', raw=False):
    data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
    req = urllib.request.Request(url, method=method, data=data,
                                 headers={'Content-Type': ctype, 'Accept': 'application/json', 'User-Agent': 'dic-cs'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = r.read()
            return r.status, (out.decode() if raw else (json.loads(out) if out else None))
    except urllib.error.HTTPError as e:
        out = e.read()
        try:
            return e.code, (out.decode() if raw else json.loads(out))
        except Exception:  # noqa: BLE001
            return e.code, out.decode('utf-8', 'replace')


# ------------------------------------------------------------------ PostgreSQL
def pg(which='pg', autocommit=True, **kw):
    kw.setdefault('connect_timeout', 5)
    return psycopg.connect(host='127.0.0.1', port=C.PORTS[which], user='postgres', password=C.PASSWORD,
                           dbname='postgres', autocommit=autocommit, **kw)


def q(which, sql, params=None):
    with pg(which) as c:
        cur = c.execute(sql, params)
        return cur.fetchall() if cur.description is not None else cur.rowcount


def pg_name(which):
    return C.PG if which == 'pg' else C.PG2


def pg_up(which='pg', args=(), memory='512m', wait=True):
    name = pg_name(which)
    D.network_create(C.NET)
    D.run(name, C.IMAGES['pg'], network=C.NET, ports={C.PORTS[which]: 5432},
          env={'POSTGRES_PASSWORD': C.PASSWORD}, volumes={f'{name}-data': '/var/lib/postgresql'}, memory=memory,
          cmd=['-c', 'shared_buffers=64MB', '-c', 'max_connections=100', *args])
    if wait:
        pg_wait(which)
    return name


def pg_wait(which='pg'):
    # the entrypoint starts a temporary server first; require several good probes in a row
    ok = [0]

    def probe():
        try:
            good = q(which, 'select 1')[0][0] == 1
        except Exception:  # noqa: BLE001
            ok[0] = 0
            raise
        ok[0] = ok[0] + 1 if good else 0
        return ok[0] >= 3
    D.wait_ready(pg_name(which), probe, timeout=120, interval=0.3)


# ---------------------------------------------------------------------- broker
def admin():
    return AdminClient({'bootstrap.servers': C.BOOTSTRAP, 'socket.timeout.ms': 5000, 'log_level': 0})


def broker_up(kind='redpanda', kafka_env=None, registry=False):
    D.network_create(C.NET)
    ext = C.BOOTSTRAP
    ports = {C.PORTS['broker']: 19092}
    if kind == 'redpanda':
        if registry:
            ports[C.PORTS['registry']] = 8081
        # durable configuration as in topic A (no dev-container defaults)
        D.run(C.BROKER, C.IMAGES['redpanda'], network=C.NET, memory='1536m', ports=ports,
              volumes={'dic-b-broker-data': '/var/lib/redpanda/data'},
              cmd=['redpanda', 'start', '--smp', '1', '--memory', '1G', '--reserve-memory', '0M',
                   '--overprovisioned', '--node-id', '0', '--check=false',
                   '--unsafe-bypass-fsync=false', '--set', 'redpanda.developer_mode=false',
                   '--kafka-addr', 'internal://0.0.0.0:9092,external://0.0.0.0:19092',
                   '--advertise-kafka-addr', f'internal://{C.BROKER}:9092,external://{ext}',
                   '--schema-registry-addr', '0.0.0.0:8081',
                   '--rpc-addr', '0.0.0.0:33145', '--advertise-rpc-addr', f'{C.BROKER}:33145'])
    elif kind == 'kafka':
        env = {
            'KAFKA_NODE_ID': '1', 'KAFKA_PROCESS_ROLES': 'broker,controller',
            'KAFKA_LISTENERS': 'INTERNAL://:9092,EXTERNAL://:19092,CONTROLLER://:9093',
            'KAFKA_ADVERTISED_LISTENERS': f'INTERNAL://{C.BROKER}:9092,EXTERNAL://{ext}',
            'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP': 'CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT',
            'KAFKA_CONTROLLER_QUORUM_VOTERS': '1@localhost:9093',
            'KAFKA_CONTROLLER_LISTENER_NAMES': 'CONTROLLER',
            'KAFKA_INTER_BROKER_LISTENER_NAME': 'INTERNAL',
            'KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR': '1',
            'KAFKA_TRANSACTION_STATE_LOG_MIN_ISR': '1',
            'KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS': '0',
            'KAFKA_AUTO_CREATE_TOPICS_ENABLE': 'false',
            'KAFKA_LOG_DIRS': '/var/lib/kafka/data',
            'KAFKA_HEAP_OPTS': '-Xms256m -Xmx512m',
            **(kafka_env or {}),
        }
        D.run(C.BROKER, C.IMAGES['kafka'], network=C.NET, memory='1536m', env=env, ports=ports,
              volumes={'dic-b-broker-data': '/var/lib/kafka/data'})
    else:
        raise ValueError(kind)
    D.wait_ready(C.BROKER, lambda: len(admin().list_topics(timeout=3).brokers) >= 1, timeout=120, interval=1.0)
    if kind == 'redpanda':
        rp_set('write_caching_default', 'false')
        rp_set('auto_create_topics_enabled', 'false')
        if registry:
            D.wait_ready(C.BROKER, lambda: http('GET', f'{C.REGISTRY}/subjects', timeout=3)[0] == 200,
                         timeout=60, interval=0.5)


def rp_set(key, val):
    D.wait_ready(C.BROKER, lambda: D.docker('exec', C.BROKER, 'rpk', 'cluster', 'config', 'set', key, str(val),
                                            check=False).returncode == 0, timeout=60, interval=1.0)
    D.wait_ready(C.BROKER, lambda: rp_get(key) == str(val), timeout=30)


def rp_get(key):
    return D.docker('exec', C.BROKER, 'rpk', 'cluster', 'config', 'get', key, check=False).stdout.strip().strip('"')


def broker_version(kind):
    if kind == 'redpanda':
        v = D.docker('exec', C.BROKER, 'rpk', 'version', check=False).stdout.split()
        return next((x for x in v if x[:1] == 'v' and x[1:2].isdigit()), '')
    r = D.docker('exec', C.BROKER, 'sh', '-c', 'ls /opt/kafka/libs | grep -E "^kafka-server-[0-9]"', check=False)
    return r.stdout.strip()[:80]


def create_topic(name, partitions=1, config=None):
    adm = admin()
    for f in adm.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1,
                                         config={k: str(v) for k, v in (config or {}).items()})]).values():
        f.result(timeout=30)
    D.wait_ready(C.BROKER, lambda: len(admin().list_topics(name, timeout=3).topics[name].partitions) == partitions,
                 timeout=30)


def topic_config(name, keys=None):
    adm = admin()
    res = ConfigResource(ConfigResource.Type.TOPIC, name)
    cfg = adm.describe_configs([res])[res].result(timeout=15)
    return {k: v.value for k, v in cfg.items() if keys is None or k in keys}


def producer(**kw):
    return Producer({'bootstrap.servers': C.BOOTSTRAP, 'acks': 'all', 'enable.idempotence': True,
                     'linger.ms': 5, 'log_level': 0, **kw})


def produce_all(topic, records, flush_s=60):
    """records: iterable of (key, value-bytes-or-None[, partition]). Raises unless every
    record is acknowledged by the broker."""
    p = producer()
    failed = []

    def cb(err, _msg):
        if err is not None:
            failed.append(err.name())
    n = 0
    for rec in records:
        kw = {'partition': rec[2]} if len(rec) > 2 else {}
        while True:
            try:
                p.produce(topic, key=rec[0], value=rec[1], on_delivery=cb, **kw)
                break
            except BufferError:
                p.poll(0.2)
        n += 1
        if n % 2000 == 0:
            p.poll(0)
    left = p.flush(flush_s)
    if failed or left:
        raise D.CmdError(f'produce failed: {failed[:3]} left={left}')
    return n


def consumer(group, **kw):
    return Consumer({'bootstrap.servers': C.BOOTSTRAP, 'group.id': group, 'enable.auto.commit': False,
                     'auto.offset.reset': 'earliest', 'log_level': 0, **kw})


def watermarks(topic, partitions=1):
    c = consumer('dic-b-scan')
    try:
        return [c.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
                for p in range(partitions)]
    finally:
        c.close()


def scan_topic(topic, partitions=1, timeout=60, start=None):
    """Read the topic from the beginning (or from ``start`` = {partition: offset}) to the
    current end: [(partition, offset, key, value-bytes-or-None)]."""
    c = consumer('dic-b-scan')
    try:
        ends, lows = {}, {}
        for p in range(partitions):
            lows[p], ends[p] = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10, cached=False)
        pos = {p: max(lows[p], (start or {}).get(p, 0)) for p in ends}
        c.assign([TopicPartition(topic, p, pos[p]) for p in ends])
        out = []
        t0 = time.monotonic()
        while any(pos[p] < ends[p] for p in ends):
            if time.monotonic() - t0 > timeout:
                raise D.CmdError(f'topic scan timed out: {pos} / {ends}')
            for m in c.consume(1000, timeout=1.0):
                if m.error():
                    continue
                if m.offset() < ends[m.partition()]:
                    out.append((m.partition(), m.offset(), (m.key() or b'').decode(), m.value()))
                pos[m.partition()] = max(pos[m.partition()], m.offset() + 1)
        return sorted(out, key=lambda x: (x[0], x[1]))
    finally:
        c.close()


# -------------------------------------------------------------- schema registry
def sr(method, path, body=None):
    return http(method, C.REGISTRY + path, body, ctype='application/vnd.schemaregistry.v1+json')


# ------------------------------------------------------------------ ClickHouse
def ch_up():
    D.network_create(C.NET)
    D.run(C.CH, C.IMAGES['clickhouse'], network=C.NET, memory='1536m', ports={C.PORTS['clickhouse']: 8123},
          env={'CLICKHOUSE_PASSWORD': C.PASSWORD, 'CLICKHOUSE_USER': 'default',
               'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT': '1'},
          volumes={'dic-b-ch-data': '/var/lib/clickhouse'}, extra=['--ulimit', 'nofile=262144:262144'])
    D.wait_ready(C.CH, lambda: ch('select 1') == [['1']], timeout=120, interval=0.5)


def ch(sql, settings=None):
    """Run one statement through the HTTP interface; rows as lists of strings (TSV)."""
    params = {'user': 'default', 'password': C.PASSWORD, **(settings or {})}
    url = f'http://127.0.0.1:{C.PORTS["clickhouse"]}/?' + urllib.parse.urlencode(params)
    st, body = http('POST', url, sql.encode(), timeout=120, ctype='text/plain', raw=True)
    if st != 200:
        raise D.CmdError(f'clickhouse {st}: {body[:500]}')
    return [ln.split('\t') for ln in body.splitlines()]


# --------------------------------------------------------------- Kafka Connect
def connect_up():
    D.run(C.CONNECT, C.IMAGES['connect'], network=C.NET, memory='1g', ports={C.PORTS['connect']: 8083},
          env={'BOOTSTRAP_SERVERS': f'{C.BROKER}:9092', 'GROUP_ID': 'dic-b-connect',
               'CONFIG_STORAGE_TOPIC': 'dic_b_connect_configs', 'OFFSET_STORAGE_TOPIC': 'dic_b_connect_offsets',
               'STATUS_STORAGE_TOPIC': 'dic_b_connect_statuses', 'HEAP_OPTS': '-Xms256M -Xmx512M',
               'OFFSET_FLUSH_INTERVAL_MS': '10000'})


def connect_url(path=''):
    return f'http://127.0.0.1:{C.PORTS["connect"]}{path}'


def connect_wait(timeout=240):
    return D.wait_ready(C.CONNECT, lambda: http('GET', connect_url('/connectors'), timeout=3)[0] == 200,
                        timeout=timeout, interval=1.0)


def connector_task_state(name):
    st, body = http('GET', connect_url(f'/connectors/{name}/status'), timeout=5)
    if st != 200 or not isinstance(body, dict):
        return None
    return (body.get('tasks') or [{}])[0].get('state')


def mem():
    return D.mem_usage(C.PREFIX)
