#!/usr/bin/env bash
# Development helper: run several dev specs and print a compact line for each.
# usage: dev_many.sh "<config> <case> <conc> [broker] [n_ops]" ...
cd "$(dirname "$0")/../.."
for a in "$@"; do
  "${DIC_VENV:-$HOME/dic-cs/.venv}/bin/python" -u -m cs.a.dev_one $a > /tmp/dic-a-dev.json 2> /tmp/dic-a-dev.err || { echo "FAILED $a"; tail -15 /tmp/dic-a-dev.err; continue; }
  "${DIC_VENV:-$HOME/dic-cs/.venv}/bin/python" - "$a" <<'PY'
import json, sys
r = json.load(open('/tmp/dic-a-dev.json')); m = r['metrics']
print(sys.argv[1], '| att', m['attempted'], 'ack', m['acked'], 'com', m['committed'], 'unacked', m['committed_unacked'],
      '| log', {k: v for k, v in m['log'].items() if v and k != 'messages'},
      '| idx', {k: v for k, v in m['index'].items() if v and k not in ('deliveries', 'effects')},
      '| ntf', {k: v for k, v in m['notify'].items() if v and k not in ('deliveries', 'effects')},
      '| final', m['final']['mismatch'], m['final']['status_mismatch'], 'unk', m['unknown_events'],
      '| lag', r['lag_ms']['p50_ms'], r['lag_ms']['p95_ms'], 'resp', r['response_ms']['p50_ms'], r['response_ms']['p95_ms'],
      '| rec', r['reconcile'], r['settled'], 'restarts', r['restarts'], 'fail', r['failures_by_reason'],
      'ev', r['events'], 'conn', (r['connector_state'] or {}).get('task'), 'sec', r['seconds'], 'w', r['writers_seconds'])
PY
done
