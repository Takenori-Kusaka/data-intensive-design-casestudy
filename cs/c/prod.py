"""The three products of topic C: start, wait, query. Containers are named dic-c-*.

  host clients --127.0.0.1:25732--> dic-c-pg  (PostgreSQL 18, row store)
  host clients --127.0.0.1:25723--> dic-c-ch  (ClickHouse HTTP, column store)
  DuckDB runs inside the Python process (files: Parquet / NDJSON under the data directory)

Only one server container runs at a time. The generated files (WSL home) are mounted
read-only at /data (in ClickHouse it is the user_files_path).
"""
import json
import os
import shutil
import time
import urllib.error
import urllib.parse
import urllib.request

import duckdb
import psycopg

from ..common import dockerutil as D
from . import config as C


def down():
    return D.cleanup(C.PREFIX)


def mem():
    return D.mem_usage(C.PREFIX)


def workdir(run_id):
    """Scratch directory of one run (under the data directory, never in the repository)."""
    d = os.path.join(C.DATA, '_work', run_id.replace(':', '_'))
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    os.chmod(d, 0o777)
    return d


def clean_work():
    shutil.rmtree(os.path.join(C.DATA, '_work'), ignore_errors=True)


def du(path):
    if os.path.isfile(path):
        return os.path.getsize(path)
    return sum(os.path.getsize(os.path.join(r, f)) for r, _d, fs in os.walk(path) for f in fs)


def timed(fn):
    t0 = time.perf_counter()
    out = fn()
    return round((time.perf_counter() - t0) * 1000, 3), out


# ------------------------------------------------------------------ PostgreSQL
def pg(autocommit=True, **kw):
    kw.setdefault('connect_timeout', 5)
    return psycopg.connect(host='127.0.0.1', port=C.PORTS['pg'], user='postgres', password=C.PASSWORD,
                           dbname='postgres', autocommit=autocommit, **kw)


def pgq(sql, params=None):
    with pg() as c:
        cur = c.execute(sql, params)
        return cur.fetchall() if cur.description is not None else cur.rowcount


def pg_up(args=()):
    os.makedirs(C.DATA, exist_ok=True)
    D.run(C.PG, C.IMAGES['pg'], ports={C.PORTS['pg']: 5432}, env={'POSTGRES_PASSWORD': C.PASSWORD},
          volumes={'dic-c-pg-data': '/var/lib/postgresql'}, memory=C.PG_MEMORY,
          extra=['-v', f'{C.DATA}:/data:ro', '--shm-size', '256m'],
          cmd=['-c', f'shared_buffers={C.PG_SHARED_BUFFERS}', '-c', 'max_connections=50', *args])
    ok = [0]

    def probe():
        try:
            good = pgq('select 1')[0][0] == 1
        except Exception:  # noqa: BLE001
            ok[0] = 0
            raise
        ok[0] = ok[0] + 1 if good else 0
        return ok[0] >= 3
    D.wait_ready(C.PG, probe, timeout=120, interval=0.3)
    return pgq('show server_version')[0][0]


PG_SETTINGS = ['shared_buffers', 'work_mem', 'maintenance_work_mem', 'max_parallel_workers_per_gather',
               'max_wal_size', 'synchronous_commit', 'fsync', 'default_toast_compression', 'jit']


def pg_settings():
    return {k: pgq(f'show {k}')[0][0] for k in PG_SETTINGS}


# ------------------------------------------------------------------ ClickHouse
_CH_CONF = """<clickhouse>
  <max_server_memory_usage>{mem}</max_server_memory_usage>
  <user_files_path>/data/</user_files_path>
  <mark_cache_size>134217728</mark_cache_size>
  <uncompressed_cache_size>0</uncompressed_cache_size>
  <logger><level>warning</level><console>1</console></logger>
</clickhouse>
"""


def ch_up(extra_conf=''):
    conf = os.path.join(C.DATA, '_chconf')
    os.makedirs(conf, exist_ok=True)
    os.chmod(conf, 0o755)
    with open(os.path.join(conf, 'dic.xml'), 'w', encoding='utf-8') as f:
        f.write(_CH_CONF.format(mem=C.CH_MAX_SERVER_MEMORY))
    os.chmod(os.path.join(conf, 'dic.xml'), 0o644)
    D.run(C.CH, C.IMAGES['clickhouse'], memory=C.CH_MEMORY, ports={C.PORTS['clickhouse']: 8123},
          env={'CLICKHOUSE_PASSWORD': C.PASSWORD, 'CLICKHOUSE_USER': 'default',
               'CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT': '1', 'CLICKHOUSE_DO_NOT_CHOWN': '1'},
          volumes={'dic-c-ch-data': '/var/lib/clickhouse'},
          extra=['--ulimit', 'nofile=262144:262144', '-v', f'{C.DATA}:/data:ro',
                 '-v', f'{conf}/dic.xml:/etc/clickhouse-server/config.d/dic.xml:ro'])
    D.wait_ready(C.CH, lambda: ch('select 1') == [[1]], timeout=120, interval=0.5)
    return ch('select version()')[0][0]


class ChError(RuntimeError):
    pass


def ch_raw(sql, settings=None, body=None, timeout=300):
    """One statement over HTTP. ``body`` (bytes or a file object) is the data of an INSERT.
    Returns (text, summary) where summary is the X-ClickHouse-Summary header (dict)."""
    params = {'user': 'default', 'password': C.PASSWORD, **(settings or {})}
    if body is not None:
        params['query'] = sql
        data = body
    else:
        data = sql.encode()
    url = f'http://127.0.0.1:{C.PORTS["clickhouse"]}/?' + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, method='POST', data=data, headers={'Content-Type': 'text/plain', 'User-Agent': 'dic-cs'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            out = r.read().decode('utf-8', 'replace')
            summ = r.headers.get('X-ClickHouse-Summary')
            exc = r.headers.get('X-ClickHouse-Exception-Code')
            if exc and exc != '0':
                raise ChError(f'clickhouse exception {exc}: {out[-600:]}')
            return out, (json.loads(summ) if summ else {})
    except urllib.error.HTTPError as e:
        raise ChError(f'clickhouse {e.code}: {e.read().decode("utf-8", "replace")[:800]}') from None


def ch(sql, settings=None, timeout=300):
    """SELECT -> rows (lists, JSONCompact; 64-bit integers come back as numbers)."""
    s = {'output_format_json_quote_64bit_integers': 0, **(settings or {})}
    head = sql.lstrip()[:8].lower()
    if head.startswith(('select', 'with', 'show', 'explain', 'describe', 'exists')):
        out, _ = ch_raw(sql + ' FORMAT JSONCompact', s, timeout=timeout)
        return json.loads(out)['data']
    out, _ = ch_raw(sql, settings, timeout=timeout)
    return out


CH_SETTINGS = ['max_threads', 'max_memory_usage', 'max_insert_threads', 'async_insert', 'optimize_on_insert',
               'input_format_skip_unknown_fields', 'input_format_defaults_for_omitted_fields',
               'input_format_null_as_default', 'input_format_allow_errors_num',
               'input_format_json_read_numbers_as_strings', 'input_format_try_infer_integers',
               'lightweight_deletes_sync', 'mutations_sync']


def ch_settings(names=None):
    names = names or CH_SETTINGS
    lst = ','.join(f"'{n}'" for n in names)
    return {r[0]: r[1] for r in ch(f'select name, value from system.settings where name in ({lst}) order by name')}


def ch_table_size(table, database='default'):
    r = ch(f"select count(), sum(rows), sum(bytes_on_disk), sum(data_compressed_bytes), sum(data_uncompressed_bytes) "
           f"from system.parts where active and database='{database}' and table='{table}'")[0]
    return {'parts': int(r[0]), 'rows': int(r[1] or 0), 'bytes_on_disk': int(r[2] or 0),
            'compressed_bytes': int(r[3] or 0), 'uncompressed_bytes': int(r[4] or 0)}


# ---------------------------------------------------------------------- DuckDB
def duck(path=':memory:', tmp=None):
    con = duckdb.connect(path)
    con.execute(f"SET memory_limit='{C.DUCK_MEMORY}'")
    if tmp:
        con.execute(f"SET temp_directory='{tmp}'")
    return con


DUCK_SETTINGS = ['memory_limit', 'threads', 'preserve_insertion_order']


def duck_settings(con):
    return {k: str(con.execute(f"select current_setting('{k}')").fetchone()[0]) for k in DUCK_SETTINGS}


def self_rchar():
    """Bytes this process asked the kernel to read so far (read syscalls, cached or not)."""
    try:
        with open('/proc/self/io') as f:
            for line in f:
                if line.startswith('rchar:'):
                    return int(line.split()[1])
    except OSError:
        pass
    return None


def versions_client():
    return {'duckdb': duckdb.__version__, 'psycopg': psycopg.__version__}
