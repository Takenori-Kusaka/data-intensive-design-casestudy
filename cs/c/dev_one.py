"""Dev check: run the planned runs whose id contains the given text and print the rows.
Nothing is written to results/.

    bash cs/c/py.sh cs.c.dev_one c1:S:pg:t1 [more ids] [--keys=ingest,size]
"""
import fnmatch
import importlib
import json
import sys

from .run import MODULES


def main(argv):
    keys = next((a.split('=', 1)[1].split(',') for a in argv if a.startswith('--keys=')), None)
    wants = [a for a in argv if not a.startswith('--')]
    for step, (mod, fn) in MODULES.items():
        if not any(w.startswith(step) for w in wants):
            continue
        m = importlib.import_module(f'.{mod}', __package__)
        for s in getattr(m, fn)():
            rid = m.run_id_of(s)
            if any(fnmatch.fnmatchcase(rid, w) for w in wants):
                row = m.run_one(s, log=print)
                show = {k: v for k, v in row.items() if not keys or k in keys or k in ('run_id', 'brief', 'seconds')}
                print(json.dumps(show, ensure_ascii=False)[:int(next((a.split('=')[1] for a in argv if a.startswith('--max=')), 6000))])


if __name__ == '__main__':
    main(sys.argv[1:])
