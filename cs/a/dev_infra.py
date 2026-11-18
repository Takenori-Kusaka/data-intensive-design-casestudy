"""Development smoke test: bring the stack up once and print what it reports.

    python -m cs.a.dev_infra redpanda|kafka 0|1 [keep]
"""
import json
import sys
import time

from . import infra


def main(kind, conn, keep=False):
    t = time.monotonic()
    s = infra.Stack(broker=kind, connect=conn).up()
    print('up', round(time.monotonic() - t, 1), json.dumps(s.info))
    if conn:
        t = time.monotonic()
        s.register_connector()
        print('connector', round(time.monotonic() - t, 1), s.connector_state())
        infra.q('pg', "insert into outbox(event_id, aggregatetype, aggregateid, type, payload) "
                      "values ('e1', 'order', '7', 'status', %s)", (json.dumps({'event_id': 'e1'}),))
        time.sleep(3)
        print(infra.q('pg', 'select slot_name, plugin, active, wal_status, restart_lsn, confirmed_flush_lsn '
                            'from pg_replication_slots'))
        print(infra.q('pg', 'select * from pg_publication_tables'))
        print(infra.q('pg', "select name, setting, unit from pg_settings where name in "
                            "('idle_replication_slot_timeout', 'max_slot_wal_keep_size', 'wal_segment_size')"))
    print(s.scan_topic())
    print(s.mem())
    if not keep:
        print(infra.down())


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2] == '1', len(sys.argv) > 3)
