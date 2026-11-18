"""Development helper: BE stack + workload, then print Connect state and logs."""
import sys
import time

from ..common import dockerutil as D
from . import cases, config as C, infra


def main(c):
    stack = infra.Stack(connect=True).up()
    stack.register_connector()
    rundir = cases.make_rundir('dev-be')
    sv = cases.start_services(rundir, 'outbox', C.SEED0, c, 400, C.N_ORDERS)
    cases.run_services(sv, [])
    for i in range(10):
        time.sleep(2)
        print(i, stack.high_watermarks(), stack.connector_state(),
              infra.q('pg', 'select active, wal_status, restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn() '
                            'from pg_replication_slots'))
    r = D.docker('logs', '--tail', '60', C.CONNECT, check=False)
    print((r.stdout + r.stderr)[-6000:])
    infra.down()


if __name__ == '__main__':
    main(int(sys.argv[1]))
