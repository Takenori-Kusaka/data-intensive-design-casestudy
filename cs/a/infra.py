"""Containers of topic A: start, wait, inject faults, remove.

Layout (docker network dic-a-net):

  host clients --127.0.0.1:25592--> dic-a-toxi --> dic-a-broker:19092 (EXTERNAL listener)
  dic-a-connect (Debezium) -------------------> dic-a-broker:9092  (INTERNAL listener)
  dic-a-connect --> dic-a-pg:5432 (logical replication slot)
  host clients --127.0.0.1:25532--> dic-a-pg     (system of record)
  host clients --127.0.0.1:25533--> dic-a-pgd    (derived data)

The broker container has the same name for Redpanda and Apache Kafka, so that
everything else is identical; the two are never started together.
"""
import json
import time
import urllib.error
import urllib.request

import psycopg
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from ..common import dockerutil as D
from . import config as C

VOLS = ['dic-a-pg-data', 'dic-a-pgd-data', 'dic-a-broker-data']
NAMES = [C.CONNECT, C.TOXI, C.BROKER, C.PG, C.PGD]


def http(method, url, body=None, timeout=10):
    req = urllib.request.Request(url, method=method, data=None if body is None else json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json', 'Accept': 'application/json',
                                          'User-Agent': 'dic-cs'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            return e.code, json.loads(raw)
        except Exception:  # noqa: BLE001
            return e.code, raw.decode('utf-8', 'replace')


def toxi(method, path, body=None):
    return http(method, f'http://127.0.0.1:{C.PORTS["toxi"]}{path}', body)[1]


def down():
    D.rm(*NAMES, volumes=VOLS)
    D.docker('network', 'rm', C.NET, check=False)
    return D.leftovers(C.PREFIX)


def pg(which='pg', autocommit=True, **kw):
    kw.setdefault('connect_timeout', 5)
    return psycopg.connect(host='127.0.0.1', port=C.PORTS[which], user='postgres', password=C.PASSWORD,
                           dbname='postgres', autocommit=autocommit, **kw)


def dsn(which='pg'):
    return f'host=127.0.0.1 port={C.PORTS[which]} user=postgres password={C.PASSWORD} dbname=postgres'


def q(which, sql, params=None):
    with pg(which) as c:
        cur = c.execute(sql, params)
        return cur.fetchall() if cur.description is not None else cur.rowcount


def _pg_probe(which):
    def p():
        return q(which, 'select 1')[0][0] == 1
    return p


def admin():
    return AdminClient({'bootstrap.servers': C.BOOTSTRAP, 'socket.timeout.ms': 5000, 'log_level': 0})


def _broker_probe():
    md = admin().list_topics(timeout=3)
    return len(md.brokers) >= 1


SOR_DDL = """
create table orders (id int primary key, status text not null, version int not null default 0, last_event text);
create table order_history (event_id text primary key, order_id int not null, version int not null,
                            writer text not null, seq int not null, status text not null);
create table outbox (id bigserial primary key, event_id text not null unique, aggregatetype text not null,
                     aggregateid text not null, type text not null, payload text not null,
                     sent boolean not null default false);
create index outbox_unsent on outbox (id) where not sent;
create table relay_state (k int primary key, last_id bigint not null);
insert into relay_state values (1, 0);
create table filler (id bigserial primary key, pad text);
"""

DERIVED_DDL = """
create table deliveries (id bigserial primary key, role text not null, event_id text not null, part int not null,
                         off bigint not null, incarnation int not null, ts double precision not null);
create table processed (role text not null, event_id text not null, primary key (role, event_id));
create table orders_index (order_id int primary key, status text not null, last_event text not null,
                           update_count int not null);
create table index_applies (id bigserial primary key, event_id text not null, order_id int not null,
                            ts double precision not null);
create table notifications (id bigserial primary key, event_id text not null, order_id int not null,
                            status text not null, ts double precision not null);
"""


class Stack:
    """One fresh set of containers and volumes for one run."""

    def __init__(self, broker='redpanda', connect=False, pg_args=()):
        self.broker_kind = broker
        self.with_connect = connect
        self.pg_args = list(pg_args)
        self.events = []       # [label, detail, seconds since the stack object was created]
        self.t0 = time.monotonic()
        self.info = {}

    # ------------------------------------------------------------------ up
    def up(self, n_orders=C.N_ORDERS):
        down()
        D.network_create(C.NET)
        D.run(C.PG, C.IMAGES['pg'], network=C.NET, ports={C.PORTS['pg']: 5432},
              env={'POSTGRES_PASSWORD': C.PASSWORD}, volumes={'dic-a-pg-data': '/var/lib/postgresql'},
              memory='512m',
              cmd=['-c', 'shared_buffers=64MB', '-c', 'max_connections=100', '-c', 'wal_level=logical',
                   '-c', 'max_wal_senders=4', '-c', 'max_replication_slots=4', *self.pg_args])
        D.run(C.PGD, C.IMAGES['pg'], network=C.NET, ports={C.PORTS['pgd']: 5432},
              env={'POSTGRES_PASSWORD': C.PASSWORD}, volumes={'dic-a-pgd-data': '/var/lib/postgresql'},
              memory='512m',
              # the derived store is never crashed in these experiments; without the
              # per-commit fsync the consumers keep up with the writers
              cmd=['-c', 'shared_buffers=64MB', '-c', 'max_connections=100', '-c', 'synchronous_commit=off'])
        D.run(C.TOXI, C.IMAGES['toxi'], network=C.NET, memory='128m',
              ports={C.PORTS['toxi']: 8474, C.PORTS['broker']: C.PORTS['broker']}, cmd=['-host', '0.0.0.0'])
        self._start_broker()
        D.wait_ready(C.TOXI, lambda: bool(toxi('GET', '/version')), timeout=30)
        toxi('POST', '/proxies', {'name': 'broker', 'listen': f'0.0.0.0:{C.PORTS["broker"]}',
                                  'upstream': f'{C.BROKER}:19092', 'enabled': True})
        D.wait_ready(C.PG, _pg_probe('pg'), timeout=120)
        D.wait_ready(C.PGD, _pg_probe('pgd'), timeout=120)
        D.wait_ready(C.BROKER, _broker_probe, timeout=120, interval=1.0)
        if self.broker_kind == 'redpanda':
            self._harden_redpanda()
        if self.with_connect:      # the JVM is slow to start: start it before the schema work
            self._run_connect()
        with pg('pg') as c:
            c.execute(SOR_DDL)
            c.execute("insert into orders (id, status) select g, 'new' from generate_series(1, %s) g", (n_orders,))
        with pg('pgd') as c:
            c.execute(DERIVED_DDL)
        adm = admin()              # keep the handle alive until the futures resolve
        fs = adm.create_topics([NewTopic(C.TOPIC, num_partitions=C.PARTITIONS, replication_factor=1)])
        for f in fs.values():
            f.result(timeout=30)
        D.wait_ready(C.BROKER, lambda: len(admin().list_topics(C.TOPIC, timeout=3).topics[C.TOPIC].partitions)
                     == C.PARTITIONS, timeout=30)
        self.info['pg_version'] = q('pg', 'show server_version')[0][0]
        self.info['broker'] = self.broker_version()
        if self.with_connect:
            self.wait_connect()
        return self

    def _start_broker(self):
        ext = f'127.0.0.1:{C.PORTS["broker"]}'
        if self.broker_kind == 'redpanda':
            # The image starts in "dev-container" mode unless told otherwise: it passes
            # --unsafe-bypass-fsync to the broker and sets write_caching_default=true
            # (acks before fsync). This experiment counts lost messages, so both are
            # switched off here and in _harden_redpanda(), and the result is recorded.
            D.run(C.BROKER, C.IMAGES['redpanda'], network=C.NET, memory='1536m',
                  volumes={'dic-a-broker-data': '/var/lib/redpanda/data'},
                  cmd=['redpanda', 'start', '--smp', '1', '--memory', '1G', '--reserve-memory', '0M',
                       '--overprovisioned', '--node-id', '0', '--check=false',
                       '--unsafe-bypass-fsync=false', '--set', 'redpanda.developer_mode=false',
                       '--kafka-addr', 'internal://0.0.0.0:9092,external://0.0.0.0:19092',
                       '--advertise-kafka-addr', f'internal://{C.BROKER}:9092,external://{ext}',
                       '--rpc-addr', '0.0.0.0:33145', '--advertise-rpc-addr', f'{C.BROKER}:33145'])
        elif self.broker_kind == 'kafka':
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
            }
            D.run(C.BROKER, C.IMAGES['kafka'], network=C.NET, memory='1536m', env=env,
                  volumes={'dic-a-broker-data': '/var/lib/kafka/data'})
        else:
            raise ValueError(self.broker_kind)

    def _harden_redpanda(self):
        def setp(key, val):
            D.wait_ready(C.BROKER, lambda: D.docker('exec', C.BROKER, 'rpk', 'cluster', 'config', 'set', key, val,
                                                    check=False).returncode == 0, timeout=60, interval=1.0)
            D.wait_ready(C.BROKER, lambda: D.docker('exec', C.BROKER, 'rpk', 'cluster', 'config', 'get', key,
                                                    check=False).stdout.strip().strip('"') == val, timeout=30)
        setp('write_caching_default', 'false')
        setp('auto_create_topics_enabled', 'false')

    def broker_version(self):
        if self.broker_kind == 'redpanda':
            v = D.docker('exec', C.BROKER, 'rpk', 'version', check=False).stdout.split()
            out = {'product': 'redpanda', 'version': next((x for x in v if x[:1] == 'v' and x[1:2].isdigit()), '')}
            for key in ('write_caching_default', 'auto_create_topics_enabled'):
                r = D.docker('exec', C.BROKER, 'rpk', 'cluster', 'config', 'get', key, check=False)
                out[key] = r.stdout.strip().strip('"')
            r = D.docker('exec', C.BROKER, 'sh', '-c', 'grep -E "developer_mode" /etc/redpanda/redpanda.yaml '
                         '|| echo "developer_mode: false (absent)"', check=False)
            out['developer_mode'] = ' '.join(r.stdout.split())
            logs = D.docker('logs', C.BROKER, check=False)
            cmdline = next((ln for ln in (logs.stdout + logs.stderr).splitlines() if 'Command line:' in ln), '')
            out['unsafe_bypass_fsync'] = ('true' if '--unsafe-bypass-fsync=true' in cmdline else
                                          'false' if '--unsafe-bypass-fsync=false' in cmdline else 'unknown')
            if out['write_caching_default'] != 'false' or out['unsafe_bypass_fsync'] != 'false':
                raise D.CmdError(f'Redpanda is not in a durable configuration: {out}')
            return out
        r = D.docker('exec', C.BROKER, 'sh', '-c', 'ls /opt/kafka/libs | grep -E "^kafka-server-[0-9]"', check=False)
        return {'product': 'kafka', 'version': r.stdout.strip()[:80],
                'flush': 'defaults (log.flush.interval.messages not set: no fsync per message)'}

    # ------------------------------------------------------------- connect
    def _run_connect(self):
        D.run(C.CONNECT, C.IMAGES['connect'], network=C.NET, memory='1g', ports={C.PORTS['connect']: 8083},
              env={'BOOTSTRAP_SERVERS': f'{C.BROKER}:9092', 'GROUP_ID': 'dic-a-connect',
                   'CONFIG_STORAGE_TOPIC': 'dic_a_connect_configs', 'OFFSET_STORAGE_TOPIC': 'dic_a_connect_offsets',
                   'STATUS_STORAGE_TOPIC': 'dic_a_connect_statuses', 'HEAP_OPTS': C.CONNECT_HEAP,
                   'OFFSET_FLUSH_INTERVAL_MS': str(C.CONNECT_OFFSET_FLUSH_MS)})

    def connect_url(self, path=''):
        return f'http://127.0.0.1:{C.PORTS["connect"]}{path}'

    def wait_connect(self, timeout=240):
        return D.wait_ready(C.CONNECT, lambda: http('GET', self.connect_url('/connectors'), timeout=3)[0] == 200,
                            timeout=timeout, interval=1.0)

    def connector_config(self, key_field='aggregateid', snapshot_mode='no_data', slot=C.SLOT):
        return {
            'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
            'tasks.max': '1',
            'plugin.name': 'pgoutput',                    # default is decoderbufs
            'database.hostname': C.PG, 'database.port': '5432', 'database.user': 'postgres',
            'database.password': C.PASSWORD, 'database.dbname': 'postgres',
            'topic.prefix': 'dica',
            'table.include.list': 'public.outbox',
            'publication.name': C.PUBLICATION,
            'publication.autocreate.mode': 'filtered',    # default all_tables
            'slot.name': slot,
            'snapshot.mode': snapshot_mode,
            'heartbeat.interval.ms': '1000',              # default 0 (no heartbeats)
            'tombstones.on.delete': 'false',
            # the brokers run with topic auto-creation off; Connect creates the
            # heartbeat topic (__debezium-heartbeat.<prefix>) through the admin API
            'topic.creation.default.replication.factor': '1',
            'topic.creation.default.partitions': '1',
            'key.converter': 'org.apache.kafka.connect.storage.StringConverter',
            'value.converter': 'org.apache.kafka.connect.storage.StringConverter',
            'transforms': 'outbox',
            'transforms.outbox.type': 'io.debezium.transforms.outbox.EventRouter',
            'transforms.outbox.table.field.event.id': 'event_id',
            'transforms.outbox.table.field.event.key': key_field,
            'transforms.outbox.table.field.event.payload': 'payload',
            'transforms.outbox.route.by.field': 'aggregatetype',
            'transforms.outbox.route.topic.replacement': C.TOPIC,
        }

    def register_connector(self, name=C.CONNECTOR, timeout=120, **kw):
        cfg = self.connector_config(**kw)
        st, body = http('PUT', self.connect_url(f'/connectors/{name}/config'), cfg, timeout=30)
        if st not in (200, 201):
            raise D.CmdError(f'connector registration failed: {st} {body}')
        D.wait_ready(C.CONNECT, lambda: self.connector_state(name)['task'] == 'RUNNING', timeout=timeout)
        D.wait_ready(C.PG, lambda: q('pg', 'select active from pg_replication_slots where slot_name = %s',
                                     (cfg['slot.name'],))[0][0] is True, timeout=60)
        return cfg

    def connector_state(self, name=C.CONNECTOR):
        try:
            st, body = http('GET', self.connect_url(f'/connectors/{name}/status'), timeout=5)
        except Exception as e:  # noqa: BLE001 - Connect is down
            return {'connector': None, 'task': None, 'trace': f'unreachable: {type(e).__name__}'}
        if st != 200 or not isinstance(body, dict):
            return {'connector': None, 'task': None, 'trace': None}
        tasks = body.get('tasks') or [{}]
        trace = tasks[0].get('trace')
        if trace:
            lines = [ln.strip() for ln in trace.splitlines()]
            keep = [ln for ln in lines if ln and not ln.startswith('at ') and not ln.startswith('...')]
            trace = ' | '.join(keep)[:1200]
        return {'connector': body.get('connector', {}).get('state'), 'task': tasks[0].get('state'), 'trace': trace}

    def delete_connector(self, name=C.CONNECTOR):
        http('DELETE', self.connect_url(f'/connectors/{name}'), timeout=30)

    # --------------------------------------------------------------- faults
    def mark(self, label, detail=''):
        self.events.append([label, detail, round(time.monotonic() - self.t0, 3)])

    def broker_cut(self, at=''):
        toxi('POST', '/proxies/broker', {'enabled': False})
        self.mark('broker_cut', at)

    def broker_restore(self, at=''):
        toxi('POST', '/proxies/broker', {'enabled': True})
        self.mark('broker_restore', at)

    def broker_pause(self, at=''):
        D.docker('pause', C.BROKER)
        self.mark('broker_pause', at)

    def broker_unpause(self, at=''):
        D.docker('unpause', C.BROKER)
        self.mark('broker_unpause', at)

    def broker_kill(self, at=''):
        D.docker('kill', '-s', 'KILL', C.BROKER)
        self.mark('broker_kill', at)

    def broker_start(self, at=''):
        D.docker('start', C.BROKER)
        D.wait_ready(C.BROKER, _broker_probe, timeout=120, interval=0.5)
        self.mark('broker_start', at)

    def connect_kill(self, at=''):
        D.docker('kill', '-s', 'KILL', C.CONNECT)
        self.mark('connect_kill', at)

    def connect_stop(self, at=''):
        D.docker('stop', '-t', '30', C.CONNECT)
        self.mark('connect_stop', at)

    def connect_start(self, at=''):
        D.docker('start', C.CONNECT)
        self.wait_connect()
        self.mark('connect_start', at)

    # ----------------------------------------------------------------- log
    def scan_topic(self, timeout=60):
        """Read the whole topic from the beginning: [(partition, offset, key, value)]."""
        c = Consumer({'bootstrap.servers': C.BOOTSTRAP, 'group.id': 'dic-a-scan', 'enable.auto.commit': False,
                      'auto.offset.reset': 'earliest', 'log_level': 0})
        try:
            ends = {}
            for p in range(C.PARTITIONS):
                ends[p] = c.get_watermark_offsets(TopicPartition(C.TOPIC, p), timeout=10, cached=False)[1]
            c.assign([TopicPartition(C.TOPIC, p, 0) for p in range(C.PARTITIONS)])
            out, pos = [], {p: 0 for p in ends}
            t0 = time.monotonic()
            while any(pos[p] < ends[p] for p in ends):
                if time.monotonic() - t0 > timeout:
                    raise D.CmdError(f'topic scan timed out: {pos} / {ends}')
                for m in c.consume(500, timeout=1.0):
                    if m.error():
                        continue
                    if m.offset() < ends[m.partition()]:
                        out.append((m.partition(), m.offset(), (m.key() or b'').decode(), (m.value() or b'').decode()))
                    pos[m.partition()] = max(pos[m.partition()], m.offset() + 1)
            return sorted(out)
        finally:
            c.close()

    def high_watermarks(self):
        c = Consumer({'bootstrap.servers': C.BOOTSTRAP, 'group.id': 'dic-a-scan', 'enable.auto.commit': False,
                      'log_level': 0})
        try:
            return [c.get_watermark_offsets(TopicPartition(C.TOPIC, p), timeout=5, cached=False)[1]
                    for p in range(C.PARTITIONS)]
        finally:
            c.close()

    def mem(self):
        return D.mem_usage(C.PREFIX)

    def down(self):
        return down()
