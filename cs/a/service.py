"""Order service instance (one process, one thread per writer).

    python -m cs.a.service '<json spec>'

mode 'n'      dual write: commit the order in PostgreSQL, then publish to the broker
              and wait for the broker's acknowledgement (two separate operations)
mode 'outbox' the order and an outbox row are written in the same transaction;
              nothing is sent to the broker from here (configurations B and BE)

Each request appends one line to the client log ``<logdir>/client-<instance>.jsonl``.
That file stands for what the caller of the service saw: ack=1 only when the
service answered success. A crash is injected by SIGKILL to the own process
right after the commit of a given request (before the publish in mode 'n');
the client log then gets a line with ack=0 and res='no_response'. Requests of the other writers of the
same process that were committed but not yet answered at that moment are logged
the same way when the process is started again; no request is sent twice.
"""
import json
import os
import signal
import sys
import threading
import time

import psycopg
from confluent_kafka import Producer

from . import config as C
from . import workload as W


class ClientLog:
    def __init__(self, path):
        self.f = open(path, 'a', encoding='utf-8')
        self.lock = threading.Lock()

    def write(self, row):
        with self.lock:
            self.f.write(json.dumps(row) + '\n')
            self.f.flush()


def read_log(path):
    rows = []
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
    return rows


def payload(eid, order_id, status, writer, seq):
    return json.dumps({'event_id': eid, 'order_id': order_id, 'status': status, 'writer': writer, 'seq': seq})


def main(spec):
    mode, inst = spec['mode'], spec['instance']
    prefix = spec.get('id_prefix', '')
    logpath = os.path.join(spec['logdir'], f'client-{inst}.jsonl')
    done = {}
    for r in read_log(logpath):                       # resume after a crash
        done[r['w']] = max(done.get(r['w'], 0), r['s'])
    log = ClientLog(logpath)
    crash_at = set(spec.get('crash_at') or [])        # seq numbers of this instance's first writer
    long_every, long_ms = spec.get('long_txn_every', 0), spec.get('long_txn_ms', 0)

    producer, stop_poll = None, threading.Event()
    if mode == 'n':
        producer = Producer({'bootstrap.servers': spec['bootstrap'], 'acks': 'all', 'enable.idempotence': True,
                             'message.timeout.ms': C.PRODUCER_TIMEOUT_MS, 'log_level': 0})

        def poller():
            while not stop_poll.is_set():
                producer.poll(0.05)
        threading.Thread(target=poller, daemon=True).start()

    def publish(order_id, body):
        ev, res = threading.Event(), {}

        def cb(err, msg):
            res['err'] = None if err is None else err.name()
            ev.set()
        try:
            producer.produce(spec['topic'], key=str(order_id), value=body, on_delivery=cb)
        except Exception as e:  # noqa: BLE001 - local queue full etc.
            return type(e).__name__
        if not ev.wait(C.PRODUCER_TIMEOUT_MS / 1000 + 2):
            return 'no_callback'
        return res['err']

    def writer(w, is_first):
        conn = psycopg.connect(spec['dsn'], autocommit=True)
        ops = W.ops_for(w, spec['seed'], spec['n_orders'], spec['n_ops'], spec['concurrency'])
        resumed = w in done or any(done.values())
        for seq, order_id, status in ops:
            if seq <= done.get(w, 0):
                continue
            eid = prefix + W.event_id(w, seq)
            if resumed and seq == done.get(w, 0) + 1:
                # The previous incarnation was killed while this request may have been
                # in flight. If it was committed, the caller saw the connection drop:
                # log it as unanswered and do not send it again.
                if conn.execute('select 1 from order_history where event_id = %s', (eid,)).fetchone():
                    log.write({'w': w, 's': seq, 'e': eid, 'o': order_id, 'db': 1, 'pub': 'na', 'ack': 0,
                               'res': 'no_response'})
                    continue
            body = payload(eid, order_id, status, w, seq)
            t0 = time.monotonic()
            row = {'w': w, 's': seq, 'e': eid, 'o': order_id, 'db': 0, 'pub': 'na', 'ack': 0}
            try:
                with conn.transaction():
                    ver = conn.execute('update orders set status = %s, version = version + 1, last_event = %s '
                                       'where id = %s returning version', (status, eid, order_id)).fetchone()[0]
                    conn.execute('insert into order_history (event_id, order_id, version, writer, seq, status) '
                                 'values (%s, %s, %s, %s, %s, %s)', (eid, order_id, ver, w, seq, status))
                    if mode == 'outbox':
                        conn.execute('insert into outbox (event_id, aggregatetype, aggregateid, type, payload) '
                                     "values (%s, 'order', %s, 'status_changed', %s)", (eid, str(order_id), body))
                    if long_every and is_first and seq % long_every == 0:
                        time.sleep(long_ms / 1000)          # a long transaction: holds its outbox id uncommitted
                row['db'] = 1
                row['tc'] = time.time()
            except psycopg.Error as e:
                row['res'] = f'db_error:{e.sqlstate}'
                row['ms'] = round((time.monotonic() - t0) * 1000, 3)
                log.write(row)
                conn.close()
                conn = psycopg.connect(spec['dsn'], autocommit=True)
                continue
            if is_first and seq in crash_at:
                row['res'] = 'no_response'
                log.write(row)
                os.kill(os.getpid(), signal.SIGKILL)        # between the DB commit and the publish
            if mode == 'n':
                err = publish(order_id, body)
                row['pub'] = 'ok' if err is None else f'fail:{err}'
                row['ack'] = 1 if err is None else 0
                if err is not None:
                    row['res'] = 'publish_failed'
            else:
                row['ack'] = 1
            row['ms'] = round((time.monotonic() - t0) * 1000, 3)
            log.write(row)
        conn.close()

    threads = [threading.Thread(target=writer, args=(w, i == 0)) for i, w in enumerate(spec['writers'])]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop_poll.set()
    if producer is not None:
        producer.flush(5)


if __name__ == '__main__':
    main(json.loads(sys.argv[1]))
