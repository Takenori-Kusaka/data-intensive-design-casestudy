"""Start / stop the single-node databases used by D-1 to D-3."""
from ..common import dockerutil as D
from . import config as C
from .db import connect


def _probe(product):
    def p():
        c = connect(product, connect_timeout=2)
        try:
            return c.q('SELECT 1')[0][0] == 1
        finally:
            c.close()
    return p


def stop(product):
    D.rm(f'{C.PREFIX}{product}-fwd', f'{C.PREFIX}{product}', volumes=[f'{C.PREFIX}{product}-data'])


def start(product):
    """Fresh container + fresh named volume. Returns the server version string."""
    stop(product)
    name, vol, img = f'{C.PREFIX}{product}', f'{C.PREFIX}{product}-data', C.IMAGES[product]
    if product == 'pg':
        D.run(name, img, ports={C.PORTS['pg']: 5432}, env={'POSTGRES_PASSWORD': C.PASSWORD},
              volumes={vol: '/var/lib/postgresql'}, memory='512m',
              cmd=['-c', 'shared_buffers=64MB', '-c', 'max_connections=100'])
    elif product == 'mysql':
        D.run(name, img, ports={C.PORTS['mysql']: 3306},
              env={'MYSQL_ROOT_PASSWORD': C.PASSWORD, 'MYSQL_DATABASE': 'dic'},
              volumes={vol: '/var/lib/mysql'}, memory='1g',
              cmd=['--innodb-buffer-pool-size=128M', '--performance-schema=OFF', '--max-connections=100'])
    elif product == 'crdb':
        # v26.4 start-single-node only accepts a loopback listen address, so the SQL
        # port is forwarded by a sidecar (Toxiproxy, no toxics) in the same network
        # namespace: host 25457 -> sidecar 0.0.0.0:26258 -> 127.0.0.1:26257.
        D.run(name, img, ports={C.PORTS['crdb']: 26258}, volumes={vol: '/cockroach/cockroach-data'},
              memory='1536m',
              cmd=['start-single-node', '--insecure', '--cache=256MiB', '--max-sql-memory=256MiB',
                   '--store=/cockroach/cockroach-data', '--listen-addr=localhost:26257',
                   '--http-addr=localhost:8080'])
        D.run(f'{name}-fwd', C.IMAGES['toxi'], memory='128m', extra=['--network', f'container:{name}'],
              cmd=['-host', '127.0.0.1', '-port', '8474'])
        D.wait_ready(f'{name}-fwd', lambda: D.docker(
            'exec', f'{name}-fwd', '/toxiproxy-cli', 'create', '-l', '0.0.0.0:26258', '-u', '127.0.0.1:26257',
            'crdb', check=False).returncode == 0, timeout=30)
    else:
        raise ValueError(product)
    D.wait_ready(name, _probe(product), timeout=180, interval=1.0)
    return version(product)


def version(product):
    c = connect(product)
    try:
        return c.q('SELECT version()')[0][0]
    finally:
        c.close()
