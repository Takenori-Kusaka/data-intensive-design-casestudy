"""Topic B: pinned images, names, ports and workload defaults. Tags were checked on
the official registries on 2026-11-19 (Docker Hub library/postgres, library/clickhouse,
apache/kafka; docker.redpanda.com; quay.io/debezium/connect)."""
import os

PREFIX = 'dic-b-'          # every container / volume / network of topic B
NET = 'dic-b-net'
PASSWORD = 'dicpass'       # fixed test password (local containers only)

IMAGES = {
    'pg': 'postgres:18.7-alpine3.24',
    'redpanda': 'docker.redpanda.com/redpandadata/redpanda:v26.2.7',
    'kafka': 'apache/kafka:4.4.0',
    'connect': 'quay.io/debezium/connect:3.7.1.Final',
    'clickhouse': 'clickhouse:26.8.36.3',
}

# host ports (127.0.0.1 only)
PORTS = {'pg': 25632, 'pg2': 25633, 'broker': 25692, 'registry': 25681, 'connect': 25683, 'clickhouse': 25623}

PG = 'dic-b-pg'            # system of record (old)
PG2 = 'dic-b-pg2'          # derived data, or the new system of record (B-6)
BROKER = 'dic-b-broker'    # Redpanda or Apache Kafka (never both at once)
CONNECT = 'dic-b-connect'  # Kafka Connect with Debezium (B-5 CDC delete only)
CH = 'dic-b-ch'            # ClickHouse (B-7 only)

BOOTSTRAP = f'127.0.0.1:{PORTS["broker"]}'
REGISTRY = f'http://127.0.0.1:{PORTS["registry"]}'
SEED0 = 20261119
STATUSES = ['accepted', 'cooking', 'delivering', 'completed', 'cancelled']

TMP = os.environ.get('DIC_TMP', os.path.expanduser('~/dic-cs/tmp'))
