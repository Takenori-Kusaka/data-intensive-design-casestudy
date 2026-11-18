"""Topic D from start to finish.

    bash cs/d/run.sh                 # everything (completed runs are skipped)
    bash cs/d/run.sh d1 d2           # selected steps
    bash cs/d/run.sh --fresh         # delete results/d/*.jsonl first

Steps: audit, d1, d2, d3, d4, d5, d6, d7, summary.
Only containers / volumes / networks named dic-d-* are created and removed.
"""
import datetime
import glob
import os
import sys
import time

from ..common import dockerutil as D
from ..common import memcheck
from ..common.record import RESULTS, Results, update_env
from . import (aggregate, audit, config as C, d1_hermitage, d2_contention, d3_defenses, d4_failover,
               d5_sync_cost, d6_stale, d7_promote, products, repl)

STEPS = ['audit', 'd1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7', 'summary']


def log(msg):
    print(f'[{datetime.datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def mem_check(label):
    s = memcheck.ensure(label)
    return s


def _cpu_model():
    try:
        with open('/proc/cpuinfo') as f:
            for line in f:
                if line.startswith('model name'):
                    return line.split(':', 1)[1].strip()
    except OSError:
        pass
    return None


def record_env(versions, mem_peak):
    images = {img: D.ensure_image(img) for img in C.IMAGES.values()}
    update_env('host', {**D.host_info(), 'cpu_model': _cpu_model(), 'memory_at_start': memcheck.snapshot()})
    update_env('images', images)
    update_env('topic_d', {'run_date': datetime.date.today().isoformat(), 'python': sys.version.split()[0]})
    if versions:
        update_env('topic_d_server_versions', versions)
    if mem_peak:
        update_env('topic_d_container_memory_seen', mem_peak)


def single_node_steps(steps, versions, mem_peak):
    """D-1 to D-3: one product at a time (never two DB products at once)."""
    for product in ('pg', 'mysql', 'crdb'):
        todo = [s for s in ('d1', 'd2', 'd3') if s in steps and not (s == 'd3' and product != 'pg')]
        if not todo:
            continue
        mem_check(f'start {product}')
        log(f'{product}: starting')
        versions[product] = products.start(product)
        try:
            if product == 'crdb':
                d1 = Results('d/d1.jsonl')
                if 'd1' in todo:
                    d1_hermitage.run(product, d1, versions[product], log=log)
                else:
                    d1_hermitage.crdb_settings(enable=True)   # D-2 needs RC and RR enabled
            elif 'd1' in todo:
                d1_hermitage.run(product, Results('d/d1.jsonl'), versions[product], log=log)
            if 'd2' in todo:
                mem_check(f'd2 {product}')
                d2_contention.run(product, Results('d/d2.jsonl'), versions[product], log=log)
            if 'd3' in todo:
                mem_check('d3')
                d3_defenses.run(Results('d/d3.jsonl'), versions[product], log=log)
            mem_peak.update(D.mem_usage(C.PREFIX))
        finally:
            products.stop(product)
        log(f'{product}: done, leftovers={D.leftovers(C.PREFIX)}')


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    steps = args or STEPS
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f'unknown steps {unknown}; choose from {STEPS}')
    if '--fresh' in argv:
        for f in glob.glob(os.path.join(RESULTS, 'd', '*.jsonl')):
            os.remove(f)
    t0 = time.monotonic()
    log(f'steps={steps} memory={mem_check("start")}')
    left = D.cleanup(C.PREFIX)          # leftovers of an interrupted run (dic-d-* only)
    versions, mem_peak = {}, {}
    try:
        for img in C.IMAGES.values():
            D.ensure_image(img)
        if 'audit' in steps:
            audit.run(log=log)
        single_node_steps(steps, versions, mem_peak)
        for name, mod in (('d4', d4_failover), ('d5', d5_sync_cost), ('d6', d6_stale), ('d7', d7_promote)):
            if name in steps:
                log(f'{name}: starting')
                mod.run(Results(f'd/{name}.jsonl'), mem_check, log=log)
                log(f'{name}: done')
    except memcheck.LowMemory as e:
        log(f'STOPPED: {e}')
        D.cleanup(C.PREFIX)
        raise SystemExit(3)
    finally:
        left = D.cleanup(C.PREFIX)
        log(f'cleanup: leftovers={left}')
    record_env(versions, mem_peak)
    if 'summary' in steps:
        aggregate.main()
        log('summary written')
    log(f'finished in {round((time.monotonic() - t0) / 60, 1)} min')


if __name__ == '__main__':
    main(sys.argv[1:])
