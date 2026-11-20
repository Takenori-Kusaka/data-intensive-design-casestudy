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


def check_readme():
    """The run counts in README.md must be the ones counted from the result files."""
    from . import compare_runs as R
    text = open(R.README, encoding='utf-8').read()
    if R.BEGIN not in text or R.END not in text:
        print('README.md: the block of run counts is missing')
        return 1
    block = text[text.index(R.BEGIN) + len(R.BEGIN):text.index(R.END)].strip('\n')
    if block != R.readme_block():
        print('README.md: the run counts differ from the result files (run cs/common/remake_summaries.sh)')
        return 1
    print('README.md: run counts match the result files')
    return 0


def check_titles():
    """Slide titles in the comparison tables must be titles of the guide's deck, word for word.
    The guide is looked for in $DIC_GUIDE or next to this repository; skipped when it is not there."""
    guide = os.environ.get('DIC_GUIDE') or os.path.join(os.path.dirname(os.path.dirname(RESULTS)), 'data-intensive-design-guide')
    deck = os.path.join(guide, 'deck')
    if not os.path.isdir(deck):
        print('slide titles: the guide repository is not available, not checked')
        return 0
    titles = set()
    for f in sorted(os.listdir(deck)):
        if re.fullmatch(r'p\d+b?\.js', f):
            titles.update(re.findall(r"L\.content\('((?:[^'\\]|\\.)*)'", open(os.path.join(deck, f), encoding='utf-8').read()))
    bad = n = 0
    for t in 'abcd':
        lines = open(os.path.join(RESULTS, t, 'summary.md'), encoding='utf-8').read().split('\n')
        i = next(k for k, line in enumerate(lines) if line.startswith('## ガイドの記述との照合'))
        for line in lines[i + 1:]:
            if line.startswith('## '):
                break
            if line.startswith('| ') and not line.startswith('| ---'):
                c = cells(line)
                if c[0] == 'スライドの題':
                    continue
                n += 1
                for part in c[0].split('／'):
                    if part not in titles:
                        print(f'{t}/summary.md: not a slide title of the guide: {part}')
                        bad += 1
    print(f'slide titles: {n} rows of the comparison tables checked against the guide deck')
    return bad


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
    bad += check_readme()
    bad += check_titles()
    print('OK' if not bad else f'{bad} problem(s)')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
