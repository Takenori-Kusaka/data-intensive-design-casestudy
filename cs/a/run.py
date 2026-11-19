"""Topic A from start to finish.

    bash cs/a/run.sh                   # everything (completed runs are skipped)
    bash cs/a/run.sh main wal          # selected steps
    bash cs/a/run.sh main --match=:be  # only runs whose id contains the text
    bash cs/a/run.sh --fresh           # delete results/a/*.jsonl first
    bash cs/a/run.sh --list            # print the planned run ids and exit

Steps: audit, main (Redpanda), wal, kafka (Apache Kafka cross-check), summary.
Only containers / volumes / networks named dic-a-* are created and removed.
"""
import datetime
import glob
import os
import sys
import time

from ..common import dockerutil as D
from ..common import memcheck
from ..common.record import RESULTS, Results, update_env
from . import aggregate, audit, cases, config as C, infra, memlog, wal

STEPS = ['audit', 'main', 'wal', 'kafka', 'summary']
FAULT_CONC = 4


def log(msg):
    print(f'[{datetime.datetime.now().strftime("%H:%M:%S")}] {msg}', flush=True)


def _spec(exp, broker, config, variant, case, conc, trial):
    return {'exp': exp, 'broker': broker, 'config': config, 'variant': variant, 'case': case, 'concurrency': conc,
            'trial': trial, 'seed': C.SEED0 + trial, 'n_ops': C.N_OPS, 'n_orders': C.N_ORDERS}


def plan_main():
    """(config, variant, case, concurrency, trials) on Redpanda."""
    p = []
    for cfg, var in (('n', ''), ('b', ''), ('be', ''), ('b', 'wm')):          # no fault
        for conc in (1, 4, 16):
            p.append((cfg, var, 'normal', conc, 5))
    p.append(('be', 'nokey', 'normal', 16, 5))
    for cfg, var in (('b', ''), ('b', 'wm')):                                # a long transaction mixed in
        p.append((cfg, var, 'longtxn', FAULT_CONC, 5))
    for case in ('writer_kill', 'broker_cut', 'broker_pause', 'broker_kill'):
        for cfg in ('n', 'b', 'be'):
            p.append((cfg, '', case, FAULT_CONC, 3))
    for cfg, var in (('n', ''), ('b', ''), ('b', 'noidem'), ('be', '')):
        p.append((cfg, var, 'consumer_kill', FAULT_CONC, 3))
    for cfg, var in (('n', ''), ('b', ''), ('be', ''), ('be', 'nokey')):
        p.append((cfg, var, 'consumer_late', FAULT_CONC, 3))
    for cfg, var in (('b', ''), ('b', 'noidem'), ('be', ''), ('be', 'noidem')):
        p.append((cfg, var, 'relay_kill', FAULT_CONC, 3))
    return [_spec('main', 'redpanda', cfg, var, case, conc, t)
            for cfg, var, case, conc, n in p for t in range(1, n + 1)]


def plan_kafka():
    """The main loss / duplicate cases again on Apache Kafka."""
    p = [('n', '', 'normal', 16, 5), ('n', '', 'writer_kill', FAULT_CONC, 3), ('n', '', 'broker_kill', FAULT_CONC, 3),
         ('b', '', 'relay_kill', FAULT_CONC, 3), ('b', 'noidem', 'consumer_kill', FAULT_CONC, 3),
         ('b', '', 'broker_kill', FAULT_CONC, 3), ('be', '', 'relay_kill', FAULT_CONC, 3),
         ('be', '', 'broker_kill', FAULT_CONC, 3)]
    return [_spec('kafka', 'kafka', cfg, var, case, conc, t) for cfg, var, case, conc, n in p for t in range(1, n + 1)]


def plan_wal():
    return [{'broker': 'redpanda', 'cond': cond, 'trial': t, 'seed': C.SEED0 + 500 + t}
            for cond in ('default', 'cap', 'idle') for t in (1, 2)]


def _cpu_model():
    try:
        with open('/proc/cpuinfo') as f:
            for line in f:
                if line.startswith('model name'):
                    return line.split(':', 1)[1].strip()
    except OSError:
        pass
    return None


def record_env(mem_peak, versions):
    import confluent_kafka
    import psycopg
    images = {img: D.ensure_image(img) for img in C.IMAGES.values()}
    update_env('images', images)
    update_env('host_topic_a', {**D.host_info(), 'cpu_model': _cpu_model(), 'memory_at_end': memcheck.snapshot()})
    update_env('topic_a', {'run_date': datetime.date.today().isoformat(), 'python': sys.version.split()[0],
                           'confluent_kafka': confluent_kafka.version()[0],
                           'librdkafka': confluent_kafka.libversion()[0], 'psycopg': psycopg.__version__})
    if versions:
        update_env('topic_a_server_versions', versions)
    if mem_peak:
        update_env('topic_a_container_memory_seen', mem_peak)


def _mib(s):
    try:
        return float(s.replace('MiB', '')) if s.endswith('MiB') else float(s.replace('GiB', '')) * 1024
    except ValueError:
        return 0.0


def main(argv):
    args = [a for a in argv if not a.startswith('--')]
    match = next((a.split('=', 1)[1] for a in argv if a.startswith('--match=')), None)
    steps = args or STEPS
    unknown = [s for s in steps if s not in STEPS]
    if unknown:
        raise SystemExit(f'unknown steps {unknown}; choose from {STEPS}')
    if '--list' in argv:
        for s in plan_main() + plan_kafka():
            print(cases.run_id_of(s))
        for s in plan_wal():
            print(f'wal:{s["broker"]}:{s["cond"]}:t{s["trial"]}')
        return
    if '--fresh' in argv:
        for f in glob.glob(os.path.join(RESULTS, 'a', '*.jsonl')):
            os.remove(f)
    t0 = time.monotonic()
    log(f'steps={steps} memory={memcheck.ensure("start")}')
    ml = memlog.MemLog().start()        # one line about every 60 s in results/a/memlog.jsonl
    D.cleanup(C.PREFIX)                 # leftovers of an interrupted run (dic-a-* only)
    mem_peak, versions = {}, {}

    def note(row):
        for k, v in (row.get('container_memory') or {}).items():
            if _mib(v) > _mib(mem_peak.get(k, '0MiB')):
                mem_peak[k] = v
        info = row.get('versions') or {}
        if info.get('pg_version'):
            versions['pg'] = info['pg_version']
        b = info.get('broker') or {}
        if b.get('product'):
            versions[b['product']] = b

    try:
        for img in C.IMAGES.values():
            D.ensure_image(img)
        if 'audit' in steps:
            audit.run(log=log)
        for step, specs in (('main', plan_main()), ('kafka', plan_kafka())):
            if step not in steps:
                continue
            res = Results(f'a/{step}.jsonl')
            todo = [s for s in specs if not res.has(cases.run_id_of(s)) and (not match or match in cases.run_id_of(s))]
            log(f'{step}: {len(todo)} of {len(specs)} runs to do')
            for i, s in enumerate(todo, 1):
                memcheck.ensure(cases.run_id_of(s))
                ml.label = cases.run_id_of(s)
                row = cases.run_one(s, log=log)
                res.append(row)
                note(row)
                m = row['metrics']
                log(f'{step} {i}/{len(todo)} {row["run_id"]}: acked={m["acked"]} '
                    f'missing={m["index"]["missing_committed"]} dup_effect={m["notify"]["dup_effect_extra"]} '
                    f'reorder={m["index"]["reorder_pairs"]} final_mismatch={m["final"]["mismatch"]} '
                    f'({row["seconds"]}s)')
        if 'wal' in steps:
            res = Results('a/wal.jsonl')
            todo = [s for s in plan_wal() if not res.has(f'wal:{s["broker"]}:{s["cond"]}:t{s["trial"]}')
                    and (not match or match in f'wal:{s["broker"]}:{s["cond"]}:t{s["trial"]}')]
            log(f'wal: {len(todo)} runs to do')
            for s in todo:
                memcheck.ensure(f'wal {s["cond"]}')
                ml.label = f'wal:{s["broker"]}:{s["cond"]}:t{s["trial"]}'
                row = wal.run_one(s, log=log)
                res.append(row)
                note(row)
                last = row['samples'][-1]
                log(f'{row["run_id"]}: retained={last.get("retained_by_slot_mb")}MB status={last.get("wal_status")} '
                    f'reason={last.get("invalidation_reason")} task={row["after_restart"]["connector_state"]["task"]} '
                    f'missing={row["after_restart"]["metrics"]["index"]["missing_committed"]} ({row["seconds"]}s)')
    except memcheck.LowMemory as e:
        log(f'STOPPED: {e}')
        infra.down()
        raise SystemExit(3)
    finally:
        infra.down()
        ml.label = 'end'
        ml.stop()
        log(f'cleanup: leftovers={D.cleanup(C.PREFIX)}')
    record_env(mem_peak, versions)
    if 'summary' in steps:
        aggregate.main()
        log('summary written')
    log(f'finished in {round((time.monotonic() - t0) / 60, 1)} min')


if __name__ == '__main__':
    main(sys.argv[1:])
