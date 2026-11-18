"""Result recording: one run = one JSONL line; completed run ids are skipped on resume."""
import datetime
import json
import os
import threading

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
RESULTS = os.path.join(ROOT, 'results')


def read_jsonl(path):
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


class Results:
    def __init__(self, relpath):
        self.path = os.path.join(RESULTS, relpath)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.done = {r['run_id'] for r in read_jsonl(self.path)}
        self._lock = threading.Lock()

    def has(self, run_id):
        return run_id in self.done

    def append(self, row):
        assert 'run_id' in row
        row.setdefault('recorded_at', datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'))
        with self._lock:
            with open(self.path, 'a', encoding='utf-8', newline='\n') as f:
                f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n')
            self.done.add(row['run_id'])


def update_env(section, data):
    """Merge ``data`` under ``section`` in results/env.json (shared by all topics)."""
    path = os.path.join(RESULTS, 'env.json')
    os.makedirs(RESULTS, exist_ok=True)
    env = {}
    if os.path.exists(path):
        with open(path, encoding='utf-8') as f:
            env = json.load(f)
    cur = env.get(section)
    if isinstance(cur, dict) and isinstance(data, dict):
        cur.update(data)
    else:
        env[section] = data
    with open(path, 'w', encoding='utf-8', newline='\n') as f:
        json.dump(env, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.write('\n')


def percentile(values, p):
    """Nearest-rank percentile (p in 0..100). None for an empty list."""
    if not values:
        return None
    s = sorted(values)
    k = max(0, min(len(s) - 1, int(-(-p * len(s) // 100)) - 1))
    return s[k]


def lat_summary(ms):
    return {'n': len(ms), 'p50_ms': _r(percentile(ms, 50)), 'p95_ms': _r(percentile(ms, 95)),
            'max_ms': _r(max(ms) if ms else None)}


def _r(x):
    return None if x is None else round(x, 3)
