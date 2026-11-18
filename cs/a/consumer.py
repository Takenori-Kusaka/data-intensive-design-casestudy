"""Event consumer that writes derived data.

    python -m cs.a.consumer '<json spec>'

role 'index'   keeps the search index (orders_index): the row of the order is
               overwritten with the status carried by the event
role 'notify'  records one row in the notification history per event (stands for
               sending one notification)

Processing is at-least-once: the effect is committed to the derived database
first and the offset is committed to the broker afterwards (every
CONSUMER_COMMIT_EVERY messages and when idle). With idempotent=true the event id
is inserted into ``processed`` in the same transaction as the effect, and an
event whose id is already there is skipped.

Every message handed to the consumer is logged in ``deliveries`` (autocommit,
before processing) so that deliveries and effects can be counted separately.
A crash is injected by SIGKILL to the own process after the effect of the
crash_at-th message of this incarnation was committed and before its offset is.
"""
import json
import os
import signal
import sys
import time

import psycopg
from confluent_kafka import Consumer, TopicPartition

from . import config as C

_stop = False


def _term(*_):
    global _stop
    _stop = True


def main(spec):
    signal.signal(signal.SIGTERM, _term)
    role, idem, inc = spec['role'], spec['idempotent'], spec.get('incarnation', 0)
    crash_at = spec.get('crash_at') or 0
    conn = psycopg.connect(spec['dsn'], autocommit=True)
    cons = Consumer({'bootstrap.servers': spec['bootstrap'], 'group.id': f'dic-a-{role}',
                     'enable.auto.commit': False, 'auto.offset.reset': 'earliest', 'log_level': 0})
    # fixed assignment of all partitions (one consumer per role); positions come
    # from the offsets committed for the group
    tps = [TopicPartition(C.TOPIC, p) for p in range(C.PARTITIONS)]
    while True:
        try:
            committed = cons.committed(tps, timeout=10)
            break
        except Exception:  # noqa: BLE001 - broker not reachable yet
            if _stop:
                return
            time.sleep(0.5)
    cons.assign(committed)
    n, pending = 0, 0
    done = {}          # partition -> offset of the next message to process

    def commit():
        # Commit exactly what has been processed. commit() without offsets would
        # commit the fetch position, which is ahead of the processing inside a batch.
        nonlocal pending
        if not pending:
            return
        try:
            cons.commit(offsets=[TopicPartition(C.TOPIC, p, o) for p, o in done.items()], asynchronous=False)
            pending = 0
        except Exception:  # noqa: BLE001 - broker unreachable: try again later
            pass

    while not _stop:
        first = cons.poll(0.2)             # return as soon as one message is there
        if first is None:
            commit()
            continue
        msgs = [first] + cons.consume(49, timeout=0)
        for m in msgs:
            if m.error():
                continue
            ev = json.loads(m.value())
            eid = ev['event_id']
            conn.execute('insert into deliveries (role, event_id, part, off, incarnation, ts) '
                         'values (%s, %s, %s, %s, %s, %s)', (role, eid, m.partition(), m.offset(), inc, time.time()))
            with conn.transaction():
                fresh = True
                if idem:
                    fresh = conn.execute('insert into processed (role, event_id) values (%s, %s) '
                                         'on conflict do nothing', (role, eid)).rowcount == 1
                if fresh and role == 'index':
                    conn.execute('insert into orders_index (order_id, status, last_event, update_count) '
                                 'values (%s, %s, %s, 1) on conflict (order_id) do update set '
                                 'status = excluded.status, last_event = excluded.last_event, '
                                 'update_count = orders_index.update_count + 1',
                                 (ev['order_id'], ev['status'], eid))
                    conn.execute('insert into index_applies (event_id, order_id, ts) values (%s, %s, %s)',
                                 (eid, ev['order_id'], time.time()))
                elif fresh:
                    conn.execute('insert into notifications (event_id, order_id, status, ts) values (%s, %s, %s, %s)',
                                 (eid, ev['order_id'], ev['status'], time.time()))
            done[m.partition()] = m.offset() + 1
            n += 1
            pending += 1
            if crash_at and n >= crash_at:
                os.kill(os.getpid(), signal.SIGKILL)      # effect committed, offset not committed
            if pending >= C.CONSUMER_COMMIT_EVERY:
                commit()
    commit()
    cons.close()
    conn.close()


if __name__ == '__main__':
    main(json.loads(sys.argv[1]))
