"""Topic C from start to finish.

    bash cs/c/run.sh                    # everything (completed runs are skipped)
    bash cs/c/run.sh c1 c2              # selected steps
    bash cs/c/run.sh c1 --match=:S:     # only runs whose id contains the text
    bash cs/c/run.sh --fresh            # delete results/c/*.jsonl first
    bash cs/c/run.sh --list             # print the planned run ids and exit
    bash cs/c/run.sh clean              # remove dic-c-* and the generated data

Steps: audit, c1, c2, c3, c4, c5, c6, summary, clean.
Only containers / volumes / networks named dic-c-* are created and removed. A run that
was interrupted is simply done again: every run starts by removing dic-c-*.
"""
import datetime
import glob
import importlib
import os
import shutil
import sys
import time

from ..common import dockerutil as D
from ..common import memcheck
from ..common.record import RESULTS, Results, read_jsonl, update_env
from . import config as C, prod

STEPS = ['audit', 'c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'summary', 'clean']
DEFAULT_STEPS = [s for s in STEPS if s != 'clean']
# step -> (module, plan function); modules are imported lazily
MODULES = {'c1': ('c1_rowcol', 'plan'), 'c2': ('c2_schema', 'plan'), 'c3': ('c3_retention', 'plan'),
           'c4': ('c4_rerun', 'plan'), 'c5': ('c5_index', 'plan'), 'c6': ('c6_collector', 'plan')}


def log(msg):
    print(f'[{datetime.datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def all_plans(steps=None):
    out = []
    for step, (mod, fn) in MODULES.items():
        if steps and step not in steps:
            continue
        m = importlib.import_module(f'.{mod}', __package__)
        out.append((step, getattr(m, fn)(), m))
    return out


def _cpu_model():
    try:
        with open('/proc/cpuinfo') as f:
            for line in f:
                if line.startswith('model name'):
                    return line.split(':', 1)[1].strip()
    except OSError:
        pass
    return None


def _mib(s):
    try:
        return float(s.replace('MiB', '')) if s.endswith('MiB') else float(s.replace('GiB', '')) * 1024
    except ValueError:
        return 0.0


def record_env(mem_peak, versions, free_min):
    from importlib.metadata import version as pkgv
    update_env('images', {img: D.image_info(img) for img in C.IMAGES.values() if D.image_info(img)})
    update_env('host_topic_c', {**D.host_info(), 'cpu_model': _cpu_model(), 'memory_at_end': memcheck.snapshot()})
    stamps = sorted(r['recorded_at'][:10] for f in glob.glob(os.path.join(RESULTS, 'c', 'c*.jsonl')) for r in read_jsonl(f))
    update_env('topic_c', {'run_date': stamps[0] if stamps else datetime.date.today().isoformat(),
                           'run_date_last': stamps[-1] if stamps else datetime.date.today().isoformat(),
                           'python': sys.version.split()[0], 'duckdb': pkgv('duckdb'), 'psycopg': pkgv('psycopg'),
                           'numpy': pkgv('numpy'), 'pyarrow': pkgv('pyarrow'),
                           'limits': {'pg_container': C.PG_MEMORY, 'pg_shared_buffers': C.PG_SHARED_BUFFERS,
                                      'clickhouse_container': C.CH_MEMORY,
                                      'clickhouse_max_server_memory_usage': C.CH_MAX_SERVER_MEMORY,
                                      'duckdb_memory_limit': C.DUCK_MEMORY}})
    if versions:
        update_env('topic_c_server_versions', versions)
    if mem_peak:
        update_env('topic_c_container_memory_seen', mem_peak)
    if free_min is not None:
        import json
        path = os.path.join(RESULTS, 'env.json')
        with open(path, encoding='utf-8') as f:
            prev = (json.load(f).get('topic_c_windows_free_gb_min') or {}).get('value')
        update_env('topic_c_windows_free_gb_min', {'value': free_min if prev is None else min(prev, free_min)})


def clean():
    left = D.cleanup(C.PREFIX)
    shutil.rmtree(C.DATA, ignore_errors=True)
    return {'leftovers': left, 'data_dir_exists': os.path.exists(C.DATA)}


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    match = next((a.split('=', 1)[1] for a in argv if a.startswith('--match=')), None)
    steps = args or DEFAULT_STEPS
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f'unknown steps {unknown}; choose from {STEPS}')
    if '--list' in argv:
        for _step, specs, mod in all_plans():
            for s in specs:
                print(mod.run_id_of(s))
        return
    if '--fresh' in argv:
        for f in glob.glob(os.path.join(RESULTS, 'c', '*.jsonl')):
            os.remove(f)
    t0 = time.monotonic()
    snap = memcheck.ensure('start')
    free_min = [snap['windows_free_gb']]
    log(f'steps={steps} memory={snap}')
    D.cleanup(C.PREFIX)                 # leftovers of an interrupted run (dic-c-* only)
    prod.clean_work()
    mem_peak, versions = {}, {}

    def note(row):
        for k, v in (row.get('container_memory') or {}).items():
            if _mib(v) > _mib(mem_peak.get(k, '0MiB')):
                mem_peak[k] = v
        versions.update(row.get('versions') or {})

    try:
        if 'audit' in steps:
            from . import audit
            audit.run(log=log)
        for step, specs, mod in all_plans(steps):
            res = Results(f'c/{step}.jsonl')
            todo = [s for s in specs if not res.has(mod.run_id_of(s)) and (not match or match in mod.run_id_of(s))]
            log(f'{step}: {len(todo)} of {len(specs)} runs to do')
            for i, s in enumerate(todo, 1):
                snap = memcheck.ensure(mod.run_id_of(s))
                if snap['windows_free_gb'] is not None:
                    free_min[0] = snap['windows_free_gb'] if free_min[0] is None else min(free_min[0], snap['windows_free_gb'])
                row = mod.run_one(s, log=log)
                row['windows_free_gb_before'] = snap['windows_free_gb']
                res.append(row)
                note(row)
                log(f'{step} {i}/{len(todo)} {row["run_id"]} ({row["seconds"]}s) {row.get("brief", "")}')
    except memcheck.LowMemory as e:
        log(f'STOPPED: {e}')
        prod.down()
        raise SystemExit(3)
    finally:
        log(f'cleanup: leftovers={D.cleanup(C.PREFIX)}')
        prod.clean_work()
    record_env(mem_peak, versions, free_min[0])
    if 'summary' in steps:
        from . import aggregate
        aggregate.main()
        log('summary written')
    if 'clean' in steps:
        log(f'clean: {clean()}')
    log(f'finished in {round((time.monotonic() - t0) / 60, 1)} min')


if __name__ == '__main__':
    main(sys.argv[1:])
