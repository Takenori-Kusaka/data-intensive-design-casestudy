"""Thin uniform layer over psycopg (PostgreSQL, CockroachDB) and PyMySQL (MySQL).

All connections run in driver autocommit mode and transactions are opened and
closed with explicit SQL, so that the statement order of a scenario is exactly
what is sent to the server.
"""
import time

import psycopg
import pymysql

from . import config as C

# errors after which the whole transaction should be retried
# ('app:version' = optimistic-lock conflict raised by the application itself)
RETRYABLE = {'40001', '40P01', 'mysql:1213', 'mysql:1205', 'app:version'}


class DBError(Exception):
    def __init__(self, code, msg):
        super().__init__(f'{code}: {msg}')
        self.code = code            # SQLSTATE, or 'mysql:<errno>'
        self.msg = (msg or '')[:300]

    @property
    def retryable(self):
        return self.code in RETRYABLE


class Conn:
    def __init__(self, product, raw):
        self.product = product
        self.raw = raw
        self.level = None

    # -- statements -------------------------------------------------------
    def q(self, sql, params=None):
        """Run one statement. Returns a list of tuples for row-returning
        statements, otherwise the affected row count."""
        try:
            if self.product == 'mysql':
                with self.raw.cursor() as cur:
                    n = cur.execute(sql, params)
                    if cur.description is not None:
                        return [tuple(r) for r in cur.fetchall()]
                    return n
            cur = self.raw.execute(sql, params)
            if cur.description is not None:
                return [tuple(r) for r in cur.fetchall()]
            return cur.rowcount
        except psycopg.Error as e:
            raise DBError(e.sqlstate or type(e).__name__, str(e).strip().splitlines()[0] if str(e) else '') from None
        except pymysql.MySQLError as e:
            errno = e.args[0] if e.args else 0
            raise DBError(f'mysql:{errno}', str(e.args[1]) if len(e.args) > 1 else str(e)) from None

    # -- transactions -----------------------------------------------------
    def set_level(self, level):
        """Remember the isolation level; MySQL takes it as a session setting."""
        self.level = level
        if self.product == 'mysql' and level:
            self.q(f'SET SESSION TRANSACTION ISOLATION LEVEL {C.LEVELS[level]}')

    def begin(self):
        if self.product == 'mysql' or not self.level:
            self.q('BEGIN')
        else:
            self.q(f'BEGIN ISOLATION LEVEL {C.LEVELS[self.level]}')

    def commit(self):
        self.q('COMMIT')

    def rollback(self):
        try:
            self.q('ROLLBACK')
        except DBError:
            pass

    def actual_level(self):
        """Isolation level the server reports inside the current transaction."""
        if self.product == 'mysql':
            return self.q('SELECT @@transaction_isolation')[0][0].lower().replace('-', ' ')
        return self.q('SHOW transaction_isolation')[0][0].lower()

    def close(self):
        try:
            self.raw.close()
        except Exception:  # noqa: BLE001
            pass


def connect(product, port=None, db=None, lock_timeout_s=None, connect_timeout=5):
    port = port or C.PORTS[product]
    if product == 'mysql':
        raw = pymysql.connect(host='127.0.0.1', port=port, user='root', password=C.PASSWORD,
                              database=db or 'dic', autocommit=True, connect_timeout=connect_timeout)
        c = Conn(product, raw)
        if lock_timeout_s:
            c.q(f'SET SESSION innodb_lock_wait_timeout = {int(lock_timeout_s)}')
        return c
    if product == 'crdb':
        dsn = f'host=127.0.0.1 port={port} user=root dbname={db or "defaultdb"} sslmode=disable connect_timeout={connect_timeout}'
    else:
        dsn = f'host=127.0.0.1 port={port} user=postgres password={C.PASSWORD} dbname={db or "postgres"} connect_timeout={connect_timeout}'
    raw = psycopg.connect(dsn, autocommit=True, prepare_threshold=None)
    c = Conn(product, raw)
    if lock_timeout_s:
        c.q(f"SET lock_timeout = '{int(lock_timeout_s)}s'")
    return c


def run_txn(conn, body, max_attempts=50, backoff_s=0.001):
    """Run ``body(conn)`` in a transaction at conn.level, retrying the whole
    transaction on serialization failures / deadlocks. Returns
    (result, retries, error_codes). Raises DBError when attempts are exhausted
    or on a non-retryable error."""
    codes = []
    for attempt in range(1, max_attempts + 1):
        try:
            conn.begin()
            res = body(conn)
            conn.commit()
            return res, attempt - 1, codes
        except DBError as e:
            conn.rollback()
            codes.append(e.code)
            if not e.retryable or attempt == max_attempts:
                e.retries = attempt - 1
                e.codes = codes
                raise
            time.sleep(min(backoff_s * attempt, 0.02))
    raise AssertionError('unreachable')
