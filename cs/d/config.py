"""Topic D: pinned images, names and ports. Tags were checked on the official
registries on 2026-11-18 (Docker Hub library/postgres, library/mysql,
cockroachdb/cockroach; ghcr.io/shopify/toxiproxy)."""

PREFIX = 'dic-d-'          # every container / volume / network of topic D
NET = 'dic-d-net'
PASSWORD = 'dicpass'       # fixed test password (local containers only)

IMAGES = {
    'pg': 'postgres:18.7-alpine3.24',
    'mysql': 'mysql:8.4.13',
    'crdb': 'cockroachdb/cockroach:v26.4.0',
    'toxi': 'ghcr.io/shopify/toxiproxy:2.13.0',
}

PORTS = {
    'pg': 25432, 'mysql': 25436, 'crdb': 25457,
    'pgp': 25441, 'pgs1': 25442, 'pgs2': 25443, 'toxi': 25474,
}

LEVELS = {
    'ru': 'READ UNCOMMITTED', 'rc': 'READ COMMITTED',
    'rr': 'REPEATABLE READ', 'ser': 'SERIALIZABLE',
}
LEVEL_JA = {'ru': 'Read Uncommitted', 'rc': 'Read Committed', 'rr': 'Repeatable Read', 'ser': 'Serializable'}
PRODUCT_JA = {'pg': 'PostgreSQL', 'mysql': 'MySQL（InnoDB）', 'crdb': 'CockroachDB'}

# isolation levels exercised per product
PRODUCT_LEVELS = {
    'pg': ['ru', 'rc', 'rr', 'ser'],
    'mysql': ['ru', 'rc', 'rr', 'ser'],
    'crdb': ['ru', 'rc', 'rr', 'ser'],
}
