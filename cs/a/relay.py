"""Outbox relay by polling (configuration B).

    python -m cs.a.relay '<json spec>'

mode 'unsent'     read rows with sent = false in id order, publish them, wait for
                  the broker's acknowledgement, then mark them sent
mode 'watermark'  the common mistake: remember the largest id read so far and read
                  only rows with a larger id. A row whose transaction commits after
                  a larger id has been read is never read.

A crash is injected by SIGKILL to the own process after a batch was acknowledged
by the broker and before it is marked sent (crash_at = number of rows published
in this incarnation at which to die).
"""
import json
import os
import signal
import sys
import time

import psycopg
from confluent_kafka import Producer

from . import config as C

_stop = False


def _term(*_):
    global _stop
    _stop = True


def main(spec):
    signal.signal(signal.SIGTERM, _term)
    mode = spec['mode']
    crash_at = spec.get('crash_at') or 0
    conn = psycopg.connect(spec['dsn'], autocommit=True)
    producer = Producer({'bootstrap.servers': spec['bootstrap'], 'acks': 'all', 'enable.idempotence': True,
                         'message.timeout.ms': 10000, 'log_level': 0})
    published = 0
    while not _stop:
        if mode == 'unsent':
            rows = conn.execute('select id, aggregateid, payload from outbox where not sent order by id limit %s',
                                (C.RELAY_BATCH,)).fetchall()
        else:
            last = conn.execute('select last_id from relay_state where k = 1').fetchone()[0]
            rows = conn.execute('select id, aggregateid, payload from outbox where id > %s order by id limit %s',
                                (last, C.RELAY_BATCH)).fetchall()
        if not rows:
            time.sleep(C.RELAY_POLL_MS / 1000)
            continue
        failed = []

        def cb(err, msg):
            if err is not None:
                failed.append(err.name())
        for _id, key, body in rows:
            producer.produce(spec['topic'], key=key, value=body, on_delivery=cb)
        left = producer.flush(15)
        if failed or left:
            # not acknowledged: keep the rows and publish the whole batch again
            # (rows of the batch that did reach the broker are then sent twice)
            time.sleep(0.2)
            continue
        published += len(rows)
        if crash_at and published >= crash_at:
            os.kill(os.getpid(), signal.SIGKILL)      # published, but not yet marked
        ids = [r[0] for r in rows]
        with conn.transaction():
            conn.execute('update outbox set sent = true where id = any(%s)', (ids,))
            if mode == 'watermark':
                conn.execute('update relay_state set last_id = %s where k = 1', (ids[-1],))
    producer.flush(5)
    conn.close()


if __name__ == '__main__':
    main(json.loads(sys.argv[1]))
