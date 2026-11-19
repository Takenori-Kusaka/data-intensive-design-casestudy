"""Run one planned run and print the row (nothing is written to results/).

    bash cs/b/py.sh cs.b.dev_one <text contained in the run id> [key path to print]
"""
import json
import sys

from . import run as R


def main(argv):
    pat = argv[0]
    for step, specs, mod in R.all_plans([pat.split(':')[0]]):
        for s in specs:
            if pat in mod.run_id_of(s):
                row = mod.run_one(s, log=R.log)
                for k in argv[1:]:
                    row = row[k] if not isinstance(row, list) else row[int(k)]
                print(json.dumps(row, ensure_ascii=False, indent=1, default=str)[:6000])
                return
    raise SystemExit(f'no run id contains {pat!r}')


if __name__ == '__main__':
    main(sys.argv[1:])
