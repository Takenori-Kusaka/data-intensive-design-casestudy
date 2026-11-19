"""Topic B from start to finish.

    bash cs/b/run.sh                   # everything (completed runs are skipped)
    bash cs/b/run.sh b3 b4             # selected steps
    bash cs/b/run.sh b5 --match=:kafka # only runs whose id contains the text
    bash cs/b/run.sh --fresh           # delete results/b/*.jsonl first
    bash cs/b/run.sh --list            # print the planned run ids and exit

Steps: audit, b1, b2, b3, b4, b5, b6, b7, summary.
Only containers / volumes / networks named dic-b-* are created and removed. A run that
was interrupted is simply done again: every run starts by removing dic-b-*.
"""
import datetime
import glob
import importlib
import os
import sys
import time

from ..common import dockerutil as D
from ..common import memcheck
from ..common.record import RESULTS, Results, update_env
from . import config as C, infra

STEPS = ['audit', 'b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'summary']
# step -> (module, plan function); modules are imported lazily
MODULES = {'b1': ('b12_schema', 'plan_b1'), 'b2': ('b12_schema', 'plan_b2'), 'b3': ('b3_ddl', 'plan'),
           'b4': ('b4_swap', 'plan'), 'b5': ('b5_rebuild', 'plan'), 'b6': ('b6_sor', 'plan'), 'b7': ('b7_mv', 'plan')}


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


def record_env(mem_peak, versions):
    import confluent_kafka
    import fastavro
    import google.protobuf
    import psycopg
    from importlib.metadata import version as pkgv
    update_env('images', {img: D.image_info(img) for img in C.IMAGES.values() if D.image_info(img)})
    update_env('host_topic_b', {**D.host_info(), 'cpu_model': _cpu_model(), 'memory_at_end': memcheck.snapshot()})
    from ..common.record import read_jsonl
    stamps = sorted(r['recorded_at'][:10] for f in glob.glob(os.path.join(RESULTS, 'b', 'b*.jsonl')) for r in read_jsonl(f))
    update_env('topic_b', {'run_date': stamps[0] if stamps else datetime.date.today().isoformat(),
                           'run_date_last': stamps[-1] if stamps else datetime.date.today().isoformat(),
                           'python': sys.version.split()[0],
                           'confluent_kafka': confluent_kafka.version()[0],
                           'librdkafka': confluent_kafka.libversion()[0], 'psycopg': psycopg.__version__,
                           'fastavro': fastavro.__version__, 'protobuf': google.protobuf.__version__,
                           'jsonschema': pkgv('jsonschema')})
    if versions:
        update_env('topic_b_server_versions', versions)
    if mem_peak:
        update_env('topic_b_container_memory_seen', mem_peak)


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    match = next((a.split('=', 1)[1] for a in argv if a.startswith('--match=')), None)
    steps = args or STEPS
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f'unknown steps {unknown}; choose from {STEPS}')
    if '--list' in argv:
        for _step, specs, mod in all_plans():
            for s in specs:
                print(mod.run_id_of(s))
        return
    if '--fresh' in argv:
        for f in glob.glob(os.path.join(RESULTS, 'b', '*.jsonl')):
            os.remove(f)
    t0 = time.monotonic()
    log(f'steps={steps} memory={memcheck.ensure("start")}')
    D.cleanup(C.PREFIX)                 # leftovers of an interrupted run (dic-b-* only)
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
            res = Results(f'b/{step}.jsonl')
            todo = [s for s in specs if not res.has(mod.run_id_of(s)) and (not match or match in mod.run_id_of(s))]
            log(f'{step}: {len(todo)} of {len(specs)} runs to do')
            for i, s in enumerate(todo, 1):
                memcheck.ensure(mod.run_id_of(s))
                row = mod.run_one(s, log=log)
                res.append(row)
                note(row)
                log(f'{step} {i}/{len(todo)} {row["run_id"]} ({row["seconds"]}s) {row.get("brief", "")}')
    except memcheck.LowMemory as e:
        log(f'STOPPED: {e}')
        infra.down()
        raise SystemExit(3)
    finally:
        log(f'cleanup: leftovers={D.cleanup(C.PREFIX)}')
    record_env(mem_peak, versions)
    if 'summary' in steps:
        from . import aggregate
        aggregate.main()
        log('summary written')
    log(f'finished in {round((time.monotonic() - t0) / 60, 1)} min')


if __name__ == '__main__':
    main(sys.argv[1:])
