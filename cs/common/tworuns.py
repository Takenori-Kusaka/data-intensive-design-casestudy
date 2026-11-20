"""Put the first run next to the second one in a summary.

A summary section is rendered twice, once from each run's result files, and the two
renderings are merged block by block (a block is a paragraph, a bullet or a table):

- identical blocks are written once;
- in a table, a verdict cell that differs becomes "実行によって変わった" with both verdicts,
  a prose cell (the measured text) that differs is written for both runs, a count cell
  that differs (an occurrence count, or a column the topic names) is written as
  "1 回目 a・2 回目 b", and every other cell (timings, sizes) keeps the second run's value;
- a paragraph or bullet that differs is written for both runs.

Nothing here decides what a number means; it only refuses to show one run's value alone
where the other run gave a different count or a different verdict.
"""
import re

OCC = re.compile(r'\*\*\d+\*\*（\d+/\d+ 試行）')
VERDICT = re.compile(r'^判定$')
PROSE = re.compile(r'^(実測.*|読み取り|再び壊れたもの|入れた構成|外した構成|結果|補足)$')
CHANGED = '**実行によって変わった**'
LEGEND = ('2 回流した結果の書き方：この summary の数は、断りが無ければ 2 回目（`results/{t}/*.jsonl`）のもの。'
          '1 回目（`results/{t}/run1/*.jsonl`）と違った箇所は次のように並べた。'
          '表の中の、異常や失敗を数えた欄は「1 回目 a・2 回目 b」。判定と実測の文は「【1 回目】…【2 回目】…」で、'
          '判定が 2 回で違えば「実行によって変わった」と書く（「一致」は 2 回とも条件を満たした行だけ）。'
          '文や箇条書きも、数が違えば 2 回ぶんを並べた。時間と大きさだけの欄（p50、所要時間、MB など）は 2 回目の値で、'
          '2 回の比は `results/RERUN.md` にある。1 つしか書いていない数は、2 回とも同じ値だったか、時間・大きさの欄である。\n')


def blocks(parts):
    """Split rendered markdown (a list of strings) into blocks separated by blank lines."""
    text = '\n'.join(parts)
    return [b.strip('\n') for b in re.split(r'\n\s*\n', text) if b.strip()]


def _cells(line):
    return [c.strip() for c in re.split(r'(?<!\\)\|', line.strip())[1:-1]]


def _row(cells):
    return '| ' + ' | '.join(cells) + ' |'


def both(a, b):
    return f'【1 回目】{a} 【2 回目】{b}'


def _merge_table(b1, b2, counts, where):
    l1, l2 = b1.split('\n'), b2.split('\n')
    head = _cells(l2[0])
    if _cells(l1[0]) != head or len(l1) != len(l2):
        raise ValueError(f'tables of the two runs do not line up in {where}: {l2[0][:80]}')
    out = l2[:2]
    for r1, r2 in zip(l1[2:], l2[2:]):
        if r1 == r2:
            out.append(r2)
            continue
        c1, c2 = _cells(r1), _cells(r2)
        if len(c1) != len(c2) or len(c2) != len(head):
            raise ValueError(f'rows of the two runs do not line up in {where}: {r2[:80]}')
        row = []
        for h, a, b in zip(head, c1, c2):
            if a == b:
                row.append(b)
            elif VERDICT.search(h):
                row.append(f'{CHANGED}（1 回目：{a}／2 回目：{b}）')
            elif PROSE.search(h):
                row.append(both(a, b))
            elif OCC.search(a) or OCC.search(b) or (counts and counts.search(h)):
                row.append(f'1 回目 {a}・2 回目 {b}')
            else:
                row.append(b)
        out.append(_row(row))
    return '\n'.join(out)


def merge(parts1, parts2, counts=None, where=''):
    """Merge the renderings of run 1 and run 2 (lists of markdown strings) into one list."""
    b1, b2 = blocks(parts1), blocks(parts2)
    if len(b1) != len(b2):
        raise ValueError(f'the two runs render a different number of blocks in {where}: {len(b1)} and {len(b2)}')
    out = []
    for x, y in zip(b1, b2):
        if x == y:
            out.append(y)
        elif x.startswith('|') and y.startswith('|'):
            out.append(_merge_table(x, y, counts, where))
        elif x.startswith('#') or y.startswith('#'):
            raise ValueError(f'a heading differs between the two runs in {where}: {y[:80]}')
        elif y.startswith('- ') and x.startswith('- ') and '\n' not in x and '\n' not in y:
            out.append('- ' + both(x[2:], y[2:]))
        else:
            out.append(both(x, y))
    return [b + '\n' for b in out]
