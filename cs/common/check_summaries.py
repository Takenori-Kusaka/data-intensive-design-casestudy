"""Checks on the generated documents (results/*/summary.md and results/RERUN.md):
no empty table cell, no ragged table row, no local path, no wall-clock timestamp.
Also lists the verdicts of the comparison tables. Exit code 1 when a check fails.

Usage: python -m cs.common.check_summaries
"""
import os
import re
import sys
from collections import Counter

from .record import RESULTS

FILES = [os.path.join(RESULTS, t, 'summary.md') for t in 'abcd'] + [os.path.join(RESULTS, 'RERUN.md')]
LOCAL_PATH = re.compile(r'(?<![A-Za-z])[A-Za-z]:[\\/]|/mnt/[a-z]/|/home/|/Users/|\\Users\\|~/')
STAMP = re.compile(r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}')


def cells(line):
    return [c.strip() for c in re.split(r'(?<!\\)\|', line.strip())[1:-1]]


def main():
    bad = 0
    for path in FILES:
        name = os.path.relpath(path, RESULTS).replace(os.sep, '/')
        lines = open(path, encoding='utf-8').read().split('\n')
        width, n_rows, n_cells, verdict_col = None, 0, 0, None
        verdicts = Counter()
        for i, line in enumerate(lines, 1):
            if LOCAL_PATH.search(line):
                print(f'{name}:{i}: local path: {LOCAL_PATH.search(line).group(0)}')
                bad += 1
            if STAMP.search(line):
                print(f'{name}:{i}: timestamp: {STAMP.search(line).group(0)}')
                bad += 1
            if not line.startswith('|'):
                width, verdict_col = None, None
                continue
            c = cells(line)
            if width is None:
                width = len(c)
                verdict_col = c.index('判定') if '判定' in c else None
                continue
            if set(''.join(c)) <= set('-: '):
                continue
            n_rows += 1
            n_cells += len(c)
            if len(c) != width:
                print(f'{name}:{i}: {len(c)} cells in a table of {width} columns')
                bad += 1
            if any(x == '' for x in c):
                print(f'{name}:{i}: empty cell')
                bad += 1
            if verdict_col is not None and len(c) == width:
                v = c[verdict_col]
                verdicts['実行によって変わった' if '実行によって変わった' in v else ('一致を含む' if '一致' in v else 'そのほか')] += 1
        print(f'{name}: {n_rows} table rows, {n_cells} cells'
              + (f'; verdicts: {dict(sorted(verdicts.items()))}' if verdicts else ''))
    print('OK' if not bad else f'{bad} problem(s)')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
