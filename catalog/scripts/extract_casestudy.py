# -*- coding: utf-8 -*-
"""Read results/<topic>/summary.md and write catalog/raw/reconcile.json: every row of the table under
「ガイドの記述との照合」 (slide title, the guide's statement, verdict, measurement), keyed <topic>-<nn>,
plus the headings of each summary (for link checks). Reads only.
  python catalog/scripts/extract_casestudy.py"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = os.path.dirname(HERE)
ROOT = os.path.dirname(CAT)
TOPICS = ['a', 'b', 'c', 'd']


def split_row(line):
    cells = re.split(r'(?<!\\)\|', line.strip())
    return [c.strip().replace('\\|', '|') for c in cells[1:-1]]


def main():
    out = {'rows': [], 'headings': {}}
    for t in TOPICS:
        path = os.path.join(ROOT, 'results', t, 'summary.md')
        lines = open(path, encoding='utf-8').read().split('\n')
        out['headings'][t] = [re.sub(r'^#+\s*', '', l).strip() for l in lines if l.startswith('#')]
        i = next(k for k, l in enumerate(lines) if l.startswith('## ガイドの記述との照合'))
        n = 0
        header = None
        for l in lines[i + 1:]:
            if l.startswith('## '):
                break
            if not l.startswith('|'):
                continue
            cells = split_row(l)
            if header is None:
                header = cells
                continue
            if set(''.join(cells)) <= set('-: '):
                continue
            n += 1
            row = dict(zip(header, cells))
            out['rows'].append({'key': '%s-%02d' % (t.upper(), n), 'topic': t.upper(), 'header': header, 'cells': cells,
                                'slide': cells[0], 'verdict': row.get('判定', ''), 'row': row})
        print(t, n, header)
    json.dump(out, open(os.path.join(CAT, 'raw', 'reconcile.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print('total', len(out['rows']))


if __name__ == '__main__':
    main()
