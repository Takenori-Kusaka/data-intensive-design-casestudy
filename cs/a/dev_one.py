"""Development helper: run one spec and print the row without recording it.

    python -m cs.a.dev_one <config>[-variant] <case> <concurrency> [broker] [n_ops]
"""
import json
import sys

from . import cases, config as C


def main(argv):
    cfg, _, variant = argv[0].partition('-')
    spec = {'exp': 'dev', 'broker': argv[3] if len(argv) > 3 else 'redpanda', 'config': cfg, 'variant': variant,
            'case': argv[1], 'concurrency': int(argv[2]), 'trial': 0, 'seed': C.SEED0,
            'n_ops': int(argv[4]) if len(argv) > 4 else C.N_OPS, 'n_orders': C.N_ORDERS}
    row = cases.run_one(spec)
    for k in ('versions', 'settings', 'container_memory'):
        row.pop(k, None)
    print(json.dumps(row, ensure_ascii=False, indent=1))


if __name__ == '__main__':
    main(sys.argv[1:])
