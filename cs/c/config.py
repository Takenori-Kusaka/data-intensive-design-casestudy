"""Topic C: pinned images, names, ports, memory limits and data sizes.

Images are the ones already pulled for topics B / D (tags were checked on Docker Hub
library/postgres and library/clickhouse on 2026-11-19). DuckDB is the Python package in
the experiment venv (version recorded in results/env.json)."""
import os

PREFIX = 'dic-c-'            # every container / volume / network of topic C
PASSWORD = 'dicpass'         # fixed test password (local containers only)

IMAGES = {
    'pg': 'postgres:18.7-alpine3.24',
    'clickhouse': 'clickhouse:26.8.36.3',
    # C-6 only. The Collector tag was checked on Docker Hub on 2026-11-20 (0.165.0, pushed 2026-11-07)
    'otelcol': 'otel/opentelemetry-collector-contrib:0.165.0',
    'redpanda': 'docker.redpanda.com/redpandadata/redpanda:v26.2.7',
}
PORTS = {'pg': 25732, 'clickhouse': 25723}   # host ports, 127.0.0.1 only (C-6: 25718 Collector, 25719 sink)
PG = 'dic-c-pg'
CH = 'dic-c-ch'

# Memory limits. Only one server container runs at a time.
PG_MEMORY = '1536m'
PG_SHARED_BUFFERS = '384MB'
CH_MEMORY = '1536m'
CH_MAX_SERVER_MEMORY = 1_000_000_000      # bytes (max_server_memory_usage)
DUCK_MEMORY = '1GB'                       # SET memory_limit

SEED0 = 20261120
# generated files live outside the repository (WSL home by default) and are removed at the end
DATA = os.environ.get('DIC_DATA_C', os.path.expanduser('~/dic-cs/data-c'))

# data sets: name -> rows
SCALES = {'S': 100_000, 'M': 1_000_000, 'L': 3_000_000}
REPS = 7                     # timed repetitions of each query (after 1 untimed warm-up)
TRIALS = 5
