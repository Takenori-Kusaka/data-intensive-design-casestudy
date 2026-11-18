"""Development helper: one WAL run, printed compactly (not recorded)."""
import json
import sys

from . import config as C, wal


def main(cond):
    row = wal.run_one({'broker': 'redpanda', 'cond': cond, 'trial': 0, 'seed': C.SEED0 + 500})
    for s in row['samples']:
        print(s)
    a = row['after_restart']
    print('after_restart', a['connector_state'], a['seconds_observed'], a['settled'], a['slot'])
    print(' missing', a['missing_by_phase'], 'final', a['metrics']['final'], 'detect', a['would_detect_by_reconcile'])
    print('after_catchup', row.get('after_catchup'))
    r = row.get('recovery')
    if r:
        print('recovery', r['connector_state'], r.get('error'), r.get('settled'), r['missing_by_phase'], r['slot'])
        print(' log', r['metrics']['log'], 'idx', r['metrics']['index'], 'ntf', r['metrics']['notify'])
    print('reconcile', row['reconcile'], 'pg', row['pg_settings'], 'sec', row['seconds'], row['events'])
    print(row['container_memory'])


if __name__ == '__main__':
    main(sys.argv[1])
