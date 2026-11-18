"""Topic A: pinned images, names, ports and workload defaults. Tags were checked
on the official registries on 2026-11-18 (Docker Hub library/postgres,
redpandadata/redpanda, apache/kafka; quay.io/debezium/connect; ghcr.io/shopify/toxiproxy)."""
import os

PREFIX = 'dic-a-'          # every container / volume / network of topic A
NET = 'dic-a-net'
PASSWORD = 'dicpass'       # fixed test password (local containers only)

IMAGES = {
    'pg': 'postgres:18.7-alpine3.24',
    'toxi': 'ghcr.io/shopify/toxiproxy:2.13.0',
    'redpanda': 'docker.redpanda.com/redpandadata/redpanda:v26.2.7',
    'kafka': 'apache/kafka:4.4.0',
    'connect': 'quay.io/debezium/connect:3.7.1.Final',
}

# host ports (127.0.0.1 only)
PORTS = {'pg': 25532, 'pgd': 25533, 'toxi': 25574, 'broker': 25592, 'connect': 25583}

PG = 'dic-a-pg'            # system of record (orders, outbox)
PGD = 'dic-a-pgd'          # derived data (search index, notification history)
BROKER = 'dic-a-broker'    # Redpanda or Apache Kafka (never both at once)
TOXI = 'dic-a-toxi'        # clients on the host reach the broker through this proxy
CONNECT = 'dic-a-connect'  # Kafka Connect with Debezium

TOPIC = 'dic.orders'
PARTITIONS = 4
BOOTSTRAP = f'127.0.0.1:{PORTS["broker"]}'
SLOT = 'dic_a_slot'
PUBLICATION = 'dic_a_pub'
CONNECTOR = 'dic-a-outbox'

STATUSES = ['accepted', 'cooking', 'delivering', 'completed']

# workload defaults (one run)
N_ORDERS = 200
N_OPS = 2000
SEED0 = 20261118

# clients
PRODUCER_TIMEOUT_MS = 3000        # N: how long the order service waits for the broker
CONSUMER_COMMIT_EVERY = 10        # offsets are committed after this many processed messages
RELAY_BATCH = 100
RELAY_POLL_MS = 50
CONNECT_OFFSET_FLUSH_MS = 10000   # Kafka Connect default is 60000
CONNECT_HEAP = '-Xms256M -Xmx512M'

TMP = os.environ.get('DIC_TMP', os.path.expanduser('~/dic-cs/tmp'))

CONFIG_JA = {'n': 'N 二重書き込み', 'b': 'B アウトボックス＋ポーリング', 'be': 'BE アウトボックス＋CDC'}
