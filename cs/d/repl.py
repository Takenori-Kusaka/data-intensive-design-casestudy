"""PostgreSQL primary + streaming standbys for D-4 to D-7.

Layout (all on the docker network dic-d-net):

  client --(127.0.0.1:25441)--> dic-d-pgp   (primary)
  client --(127.0.0.1:25442)--> dic-d-pgs1  (standby 1)   [25443 -> dic-d-pgs2]
  dic-d-pgs<i> --> dic-d-toxi:600<i> --> dic-d-pgp:5432   (replication path)

The replication path goes through Toxiproxy so that it can be cut (proxy
disabled) or delayed (latency toxic) at a chosen point of the workload,
without touching the client paths.
"""
import json
import time
import urllib.request

from ..common import dockerutil as D
from . import config as C
from .db import connect

PRIMARY = 'dic-d-pgp'
TOXI = 'dic-d-toxi'
_NAMES = [PRIMARY, TOXI, 'dic-d-pgs1', 'dic-d-pgs2', 'dic-d-pgs1-init', 'dic-d-pgs2-init']
_VOLS = ['dic-d-pgp-data', 'dic-d-pgs1-data', 'dic-d-pgs2-data']
_pgdata = None


def pgdata():
    global _pgdata
    if _pgdata is None:
        _pgdata = D.docker('run', '--rm', '--name', 'dic-d-pgenv', C.IMAGES['pg'], 'printenv', 'PGDATA').stdout.strip()
    return _pgdata


def _toxi(method, path, body=None):
    req = urllib.request.Request(f'http://127.0.0.1:{C.PORTS["toxi"]}{path}', method=method,
                                 data=None if body is None else json.dumps(body).encode(),
                                 headers={'Content-Type': 'application/json', 'User-Agent': 'dic-cs'})
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        return json.loads(raw) if raw else None


def down():
    D.rm(*_NAMES, volumes=_VOLS)
    D.docker('network', 'rm', C.NET, check=False)


class Cluster:
    def __init__(self, n_standby=1, apply_delay_ms=0):
        self.n = n_standby
        self.apply_delay_ms = apply_delay_ms
        self.events = []          # (label, detail) in the order they were injected

    # ---------------------------------------------------------------- setup
    def up(self):
        down()
        D.network_create(C.NET)
        D.run(TOXI, C.IMAGES['toxi'], network=C.NET, ports={C.PORTS['toxi']: 8474}, memory='128m',
              cmd=['-host', '0.0.0.0'])
        D.run(PRIMARY, C.IMAGES['pg'], network=C.NET, ports={C.PORTS['pgp']: 5432},
              env={'POSTGRES_PASSWORD': C.PASSWORD}, volumes={'dic-d-pgp-data': '/var/lib/postgresql'},
              memory='512m', cmd=['-c', 'shared_buffers=64MB', '-c', 'wal_keep_size=256MB'])
        D.wait_ready(PRIMARY, self._probe('pgp'), timeout=120)
        D.docker('exec', '-u', 'postgres', PRIMARY, 'sh', '-c',
                 'echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"')
        self.p_exec('select pg_reload_conf()')
        D.wait_ready(TOXI, lambda: bool(_toxi('GET', '/version')), timeout=30)
        for i in range(1, self.n + 1):
            self._add_standby(i)
        return self

    def _add_standby(self, i):
        name, vol = f'dic-d-pgs{i}', f'dic-d-pgs{i}-data'
        _toxi('POST', '/proxies', {'name': f'repl{i}', 'listen': f'0.0.0.0:600{i}',
                                   'upstream': f'{PRIMARY}:5432', 'enabled': True})
        conninfo = (f'host={TOXI} port=600{i} user=postgres password={C.PASSWORD} '
                    f'application_name=dic_s{i}')
        extra = f"primary_conninfo = '{conninfo}'\n"
        if self.apply_delay_ms:
            extra += f"recovery_min_apply_delay = '{self.apply_delay_ms}ms'\n"
        script = (f'pg_basebackup -h {PRIMARY} -U postgres -D "$PGDATA" -X stream -c fast '
                  f'&& touch "$PGDATA/standby.signal" && cat >> "$PGDATA/postgresql.auto.conf"')
        D.docker('run', '--rm', '-i', '--name', f'{name}-init', '--network', C.NET,
                 '-v', f'{vol}:/var/lib/postgresql', '-e', f'PGPASSWORD={C.PASSWORD}',
                 '--user', 'postgres', '--entrypoint', 'sh', C.IMAGES['pg'], '-c', script, input=extra)
        D.run(name, C.IMAGES['pg'], network=C.NET, ports={C.PORTS[f'pgs{i}']: 5432},
              volumes={vol: '/var/lib/postgresql'}, memory='512m', cmd=['-c', 'shared_buffers=64MB'])
        D.wait_ready(name, self._probe(f'pgs{i}'), timeout=120)
        D.wait_ready(name, lambda: self.p_exec(
            "select count(*) from pg_stat_replication where application_name = %s and state = 'streaming'",
            (f'dic_s{i}',))[0][0] == 1, timeout=60)

    @staticmethod
    def _probe(key):
        def p():
            c = connect('pg', port=C.PORTS[key], connect_timeout=2)
            try:
                return c.q('select 1')[0][0] == 1
            finally:
                c.close()
        return p

    # ---------------------------------------------------------------- access
    def pconn(self):
        return connect('pg', port=C.PORTS['pgp'])

    def sconn(self, i=1):
        return connect('pg', port=C.PORTS[f'pgs{i}'])

    def p_exec(self, sql, params=None):
        c = self.pconn()
        try:
            return c.q(sql, params)
        finally:
            c.close()

    def s_exec(self, sql, params=None, i=1):
        c = self.sconn(i)
        try:
            return c.q(sql, params)
        finally:
            c.close()

    def version(self):
        return self.p_exec('select version()')[0][0]

    # ---------------------------------------------------------------- settings
    def set_sync(self, synchronous_commit, standby_names, expect_sync=True):
        c = self.pconn()
        try:
            c.q(f"alter system set synchronous_commit = '{synchronous_commit}'")
            assert "'" not in standby_names
            c.q(f"alter system set synchronous_standby_names = '{standby_names}'")
            c.q('select pg_reload_conf()')
        finally:
            c.close()

        def applied():
            c2 = self.pconn()
            try:
                return (c2.q('show synchronous_commit')[0][0] == synchronous_commit
                        and c2.q('show synchronous_standby_names')[0][0] == standby_names)
            finally:
                c2.close()
        D.wait_ready(PRIMARY, applied, timeout=20, interval=0.2)
        if standby_names and expect_sync:
            D.wait_ready(PRIMARY, lambda: self.p_exec(
                "select count(*) from pg_stat_replication where sync_state in ('sync', 'quorum')")[0][0] >= 1,
                timeout=30, interval=0.2)
        return self.p_exec('select application_name, state, sync_state from pg_stat_replication order by 1')

    def wait_caught_up(self, i=1, timeout=30, what='replay'):
        """Wait until standby i has received (what='flush') or replayed everything
        the primary has written so far. Compared by LSN, not by time."""
        # insert position, not write position: with synchronous_commit=off the last
        # commits may still sit in the WAL buffers and would otherwise be missed
        lsn = self.p_exec('select pg_current_wal_insert_lsn()::text')[0][0]
        col = 'pg_last_wal_replay_lsn()' if what == 'replay' else 'pg_last_wal_receive_lsn()'
        c = self.sconn(i)
        try:
            t0 = time.monotonic()
            while time.monotonic() - t0 < timeout:
                if c.q(f'select {col} >= %s::pg_lsn', (lsn,))[0][0]:
                    return lsn
                time.sleep(0.02)
        finally:
            c.close()
        raise RuntimeError(f'standby {i} did not reach {lsn} within {timeout}s')

    # ---------------------------------------------------------------- faults
    def cut(self, i=1, at=''):
        _toxi('POST', f'/proxies/repl{i}', {'enabled': False})
        self.events.append(['cut_replication', f'standby {i}; {at}'])

    def restore(self, i=1, at=''):
        _toxi('POST', f'/proxies/repl{i}', {'enabled': True})
        self.events.append(['restore_replication', f'standby {i}; {at}'])

    def set_latency(self, ms, i=1):
        """Add ``ms`` of delay in each direction of the replication path (0 removes it)."""
        for stream in ('upstream', 'downstream'):
            try:
                _toxi('DELETE', f'/proxies/repl{i}/toxics/lat_{stream}')
            except Exception:  # noqa: BLE001 - toxic not present
                pass
            if ms:
                _toxi('POST', f'/proxies/repl{i}/toxics', {'name': f'lat_{stream}', 'type': 'latency',
                                                          'stream': stream, 'attributes': {'latency': ms, 'jitter': 0}})

    def kill_primary(self, at=''):
        D.docker('kill', '-s', 'KILL', PRIMARY)
        self.events.append(['kill_primary', at])

    def stop_primary(self, at=''):
        D.docker('stop', '-t', '20', PRIMARY)
        self.events.append(['stop_primary', at])

    def start_primary(self, at=''):
        D.docker('start', PRIMARY)
        D.wait_ready(PRIMARY, self._probe('pgp'), timeout=120)
        self.events.append(['start_primary', at])

    def stop_standby(self, i=1, at=''):
        D.docker('stop', '-t', '20', f'dic-d-pgs{i}')
        self.events.append(['stop_standby', f'standby {i}; {at}'])

    def start_standby(self, i=1, at=''):
        D.docker('start', f'dic-d-pgs{i}')
        self.events.append(['start_standby', f'standby {i}; {at}'])

    def promote(self, i=1, at=''):
        c = self.sconn(i)
        try:
            ok = c.q('select pg_promote(true, 60)')[0][0]
            rec = c.q('select pg_is_in_recovery()')[0][0]
        finally:
            c.close()
        self.events.append(['promote_standby', f'standby {i}; {at}'])
        if not ok or rec:
            raise RuntimeError('promotion failed')

    def mem(self):
        return D.mem_usage(C.PREFIX)

    def down(self):
        down()
