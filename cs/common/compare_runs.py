"""Compare the two full runs of every topic and write results/RERUN.md.

Run 1 is kept under results/<topic>/run1/ (taken from the git history), run 2 is the
current results/<topic>/*.jsonl. Every number in RERUN.md is counted here from those
files; nothing is typed by hand. The per-topic definitions (which fields are timings,
which counts depend on a concurrent race, which counts are shown as "happened / did not
happen") live in cs/<topic>/rerun_def.py.

The helpers at the top (load, by_id) are also used by the per-topic aggregate scripts
to print the first run next to the second one.
"""
import importlib
import json
import os
import re
from statistics import median

from .record import RESULTS, read_jsonl

TOPICS = ['d', 'a', 'b', 'c']
OUT = os.path.join(RESULTS, 'RERUN.md')
RUN_JA = {1: '1 回目', 2: '2 回目'}

# Fields that are measurements of time or size in every topic (never compared as exact values).
COMMON_TIME = re.compile(
    r'(^|[._\]])(ms|seconds|t)$|_ms($|\.)|_s$|_mb$|(^|\.)(lag_ms|response_ms|latency|commit_latency|visible_after_ack|baseline)\.'
    r'|(^|\.)(duration_s|wall_s|seconds|writers_seconds|container_memory|windows_free_gb_before|brief|lsn)(\.|$)')
SKIP = re.compile(r'^recorded_at$')


def path_of(topic, name, run):
    """results/<topic>/<name>.jsonl for run 2, results/<topic>/run1/<name>.jsonl for run 1."""
    return os.path.join(RESULTS, topic, *([] if run == 2 else ['run1']), name + '.jsonl')


def load(topic, name, run):
    return read_jsonl(path_of(topic, name, run))


def by_id(rows):
    return {r['run_id']: r for r in rows}


def flat(v, p=''):
    """Leaf paths of a row. Lists of scalars stay one value; lists of containers are indexed."""
    if isinstance(v, dict):
        for k in sorted(v):
            yield from flat(v[k], f'{p}.{k}' if p else k)
    elif isinstance(v, list) and any(isinstance(x, (dict, list)) for x in v):
        for i, x in enumerate(v):
            yield from flat(x, f'{p}[{i}]')
    else:
        yield p, v


def cell(v):
    """A table cell: never empty, no raw pipe, bounded length."""
    if v is None:
        s = 'なし（null）'
    elif isinstance(v, bool):
        s = 'true' if v else 'false'
    elif isinstance(v, (list, dict)):
        s = json.dumps(v, ensure_ascii=False, sort_keys=True)
    else:
        s = str(v)
    s = s.replace('\n', ' ').replace('|', '\\|')
    if s == '':
        s = '（空の文字列）'
    return s if len(s) <= 160 else s[:157] + '…'


def table(header, rows):
    out = ['| ' + ' | '.join(header) + ' |', '| ' + ' | '.join('---' for _ in header) + ' |']
    for r in rows:
        out.append('| ' + ' | '.join(cell(c) for c in r) + ' |')
    return '\n'.join(out) + '\n'


def kind(defn, name, row, path):
    """'skip', 'time', 'racy' or 'det' for one leaf of one row."""
    if SKIP.search(path):
        return 'skip'
    k = defn.classify(name, row, path)
    if k:
        return k
    return 'time' if COMMON_TIME.search(path) else 'det'


def det_changes(defn, name, a, b):
    """[(run_id, path, v1, v2)] for the leaves classified 'det' whose value differs."""
    out = []
    for rid in sorted(set(a) & set(b)):
        x, y = dict(flat(a[rid])), dict(flat(b[rid]))
        for p in sorted(set(x) | set(y)):
            if kind(defn, name, b[rid] if p in y else a[rid], p) != 'det':
                continue
            if x.get(p, '（欄なし）') != y.get(p, '（欄なし）'):
                out.append((rid, p, x.get(p, '（欄なし）'), y.get(p, '（欄なし）')))
    return out


def occ_text(vals):
    """'sum（trials with at least one / trials）'."""
    return f'{sum(vals)}（{sum(1 for v in vals if v > 0)}/{len(vals)}）'


def occ_groups(defn, name, rows):
    """{(config, case, label): [value per trial]} for the counts listed in defn.OCC."""
    out = {}
    for r in sorted(rows, key=lambda r: r['run_id']):
        key = defn.group(name, r)
        if key is None:
            continue
        for f, label, get in defn.OCC:
            if f != name:
                continue
            v = get(r)
            if v is None:
                continue
            out.setdefault((key[0], key[1], label), []).append(v)
    return out


def ratio(xs):
    return f'{min(xs):.2f}／{median(xs):.2f}／{max(xs):.2f}'


def n_runs_of(defn, name, rows):
    """The number of runs in a file; rows that only record something (not a run) are counted apart."""
    is_run = getattr(defn, 'is_run', None)
    n = sum(1 for r in rows.values() if not is_run or is_run(name, r))
    return str(n) if n == len(rows) else f'{n}（ほかに記録の行 {len(rows) - n}。ファイルは {len(rows)} 行）'


def section_topic(t):
    defn = importlib.import_module(f'cs.{t}.rerun_def')
    files = defn.FILES + [('audit', '判定器の監査')]
    runs = {name: (by_id(load(t, name, 1)), by_id(load(t, name, 2))) for name, _ja in files}
    L = [f'## 題材 {t.upper()}：{defn.TITLE}\n']

    # ---- runs and audit
    rows = []
    for name, ja in files:
        a, b = runs[name]
        same = '同じ' if set(a) == set(b) else f'違う（1 回目だけ {len(set(a) - set(b))}、2 回目だけ {len(set(b) - set(a))}）'
        if name == 'audit':
            rows.append([ja, f'{name}.jsonl', f'{len(a)}（合格 {sum(1 for r in a.values() if defn.audit_ok(r))}）',
                         f'{len(b)}（合格 {sum(1 for r in b.values() if defn.audit_ok(r))}）', same])
        else:
            rows.append([ja, f'{name}.jsonl', n_runs_of(defn, name, a), n_runs_of(defn, name, b), same])
    L.append(table(['実験', '結果のファイル', '1 回目の実行の数', '2 回目の実行の数', '実行 ID の集合'], rows))

    # ---- deterministic fields
    L.append('### 決定的な欄が変わった実行\n')
    L.append(defn.DET_NOTE + '\n')
    rows, n_cmp, n_runs = [], 0, set()
    for name, _ja in files:
        a, b = runs[name]
        for rid in set(a) & set(b):
            n_cmp += sum(1 for p, _v in flat(b[rid]) if kind(defn, name, b[rid], p) == 'det')
        for rid, p, x, y in det_changes(defn, name, a, b):
            rows.append([rid, p, x, y])
            n_runs.add(rid)
    is_run = getattr(defn, 'is_run', None)
    n_all = sum(1 for name, (a, b) in runs.items() for rid in set(a) & set(b) if not is_run or is_run(name, b[rid]))
    n_rec = sum(len(set(a) & set(b)) for a, b in runs.values()) - n_all
    L.append(f'比べた実行：{n_all} 個（監査を含む。' + (f'ほかに、実行ではない記録の行 {n_rec} 個も比べた。' if n_rec else '')
             + f'決定的な欄は 2 回目の行で数えて延べ {n_cmp} 個）。'
             f'決定的な欄が 1 つでも変わった実行：{len(n_runs)} 個（変わった欄は延べ {len(rows)} 個）。\n')
    if rows:
        L.append(table(['実行 ID', '欄', '1 回目', '2 回目'], rows))

    # ---- happened / did not happen
    L.append('### 「起きた／起きなかった」が変わったケース\n')
    L.append('構成 × ケース（試行をまとめた単位）ごとに、下の「数えたもの」の合計が 0 か 1 以上かを 2 回で比べた。'
             '書き方は「合計（1 件以上起きた試行／試行）」。数えたもの：'
             + '、'.join(dict.fromkeys(f'{label}（{f}.jsonl）' for f, label, _g in defn.OCC)) + '。\n')
    flips, moved, n_groups = [], [], 0
    for name, ja in defn.FILES:
        g1, g2 = occ_groups(defn, name, runs[name][0].values()), occ_groups(defn, name, runs[name][1].values())
        for key in g2:
            if key not in g1:
                continue
            n_groups += 1
            v1, v2 = g1[key], g2[key]
            if (sum(v1) == 0) != (sum(v2) == 0):
                flips.append([f'{name}.jsonl', key[0], key[1], key[2], occ_text(v1), occ_text(v2)])
            elif sum(1 for v in v1 if v > 0) != sum(1 for v in v2 if v > 0):
                moved.append([f'{name}.jsonl', key[0], key[1], key[2], occ_text(v1), occ_text(v2)])
    L.append(f'比べた組（構成 × ケース × 数えたもの）：{n_groups} 個。0 か 1 以上かが変わった組：{len(flips)} 個。\n')
    if flips:
        L.append(table(['結果のファイル', '構成', 'ケース', '数えたもの', '1 回目', '2 回目'], flips))
    L.append(f'2 回とも 1 件以上起きたが、起きた試行の数が変わった組：{len(moved)} 個。\n')
    if moved:
        L.append(table(['結果のファイル', '構成', 'ケース', '数えたもの', '1 回目', '2 回目'], moved))

    # ---- spread of counts and timings
    L.append('### 件数と時間の揺れの幅\n')
    L.append('同じ実行 ID どうしで「2 回目 ÷ 1 回目」の比を取り、その最小／中央／最大を書いた。'
             'どちらかが 0 か、欄が無い実行は比を取れないので、数だけを右の列に書いた。\n')
    rows = []
    for name, label, get, pred in defn.RATIO:
        a, b = runs[name]
        xs, skipped = [], 0
        for rid in sorted(set(a) & set(b)):
            if pred and not pred(b[rid]):
                continue
            x, y = get(a[rid]), get(b[rid])
            if x is None or y is None or x <= 0 or y <= 0:
                skipped += 1
                continue
            xs.append(y / x)
        rows.append([f'{name}.jsonl', label, len(xs), ratio(xs) if xs else '比を取れた実行なし', skipped])
    L.append(table(['結果のファイル', '欄', '比を取った実行の数', '2 回目 ÷ 1 回目（最小／中央／最大）', '比を取れなかった実行の数'], rows))
    return L, len(n_runs), len(flips)


README = os.path.join(os.path.dirname(RESULTS), 'README.md')
BEGIN, END = '<!-- runs:begin (cs/common/compare_runs.py が書く。手で直さない) -->', '<!-- runs:end -->'


def readme_block():
    """The table of run counts in README.md, counted from the result files of both runs."""
    rows, tot = [], [0, 0, 0, 0]
    for t in sorted(TOPICS):
        defn = importlib.import_module(f'cs.{t}.rerun_def')
        is_run = getattr(defn, 'is_run', None)
        n, rec, same = [0, 0], [0, 0], True
        for name, _ja in defn.FILES:
            for i, run in enumerate((1, 2)):
                rs = load(t, name, run)
                k = sum(1 for r in rs if not is_run or is_run(name, r))
                n[i] += k
                rec[i] += len(rs) - k
            same = same and {r['run_id'] for r in load(t, name, 1)} == {r['run_id'] for r in load(t, name, 2)}
        au = load(t, 'audit', 2)
        rows.append([f'{t.upper()}　{defn.TITLE}', '、'.join(f'{name}.jsonl' for name, _ja in defn.FILES),
                     n[0], n[1], '同じ' if same else '違う', rec[1], f'{sum(1 for r in au if defn.audit_ok(r))}/{len(au)}'])
        tot = [tot[0] + n[0], tot[1] + n[1], tot[2] + rec[1], tot[3] + len(au)]
    rows.append(['計', '4 題材', tot[0], tot[1], '同じ' if all(r[4] == '同じ' for r in rows) else '違う題材がある', tot[2], f'{tot[3]} ケース'])
    return table(['題材', '結果のファイル（results/<題材>/）', '実行の数（1 回目）', '実行の数（2 回目）', '実行 ID の集合',
                  '実行ではない記録の行（2 回目）', '判定器の監査（2 回目、合格／ケース）'], rows).rstrip('\n')


def update_readme():
    """Rewrite the block between the markers in README.md."""
    with open(README, encoding='utf-8') as f:
        text = f.read()
    i, j = text.index(BEGIN), text.index(END)
    text = text[:i] + BEGIN + '\n' + readme_block() + '\n' + text[j:]
    with open(README, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)


def main():
    L = ['# 2 回の実行の比較\n',
         'このファイルは `cs/common/compare_runs.py` が、`results/<題材>/run1/*.jsonl`（1 回目。git の履歴から取り出したもの）と '
         '`results/<題材>/*.jsonl`（2 回目。同じコードで最初から流し直したもの）から作る。表の数は結果のファイルから数えたもので、'
         '手では書いていない。題材ごとの欄の分け方は `cs/<題材>/rerun_def.py` にある。\n',
         '欄は 3 つに分けた。**時間・大きさ**（所要時間、遅れ、メモリ、LSN、ファイルの大きさなど。流すたびに変わる測定値）、'
         '**並行の競合や時間に頼る数**（どの試行でも同じになるとは決めていない数。題材ごとに下で範囲を書く）、'
         'それ以外の**決定的な欄**（構成、種、入力の規模、件数を合図にした障害の結果、検査の可否、判定、監査）。'
         '記録した時刻（recorded_at）は比べていない。\n']
    total_det, total_flip = [], []
    body = []
    for t in TOPICS:
        sec, n_det, n_flip = section_topic(t)
        body += sec
        total_det.append(f'{t.upper()} {n_det}')
        total_flip.append(f'{t.upper()} {n_flip}')
    L.append('## 要点\n')
    L.append(f'- 決定的な欄が変わった実行の数：{"、".join(total_det)}\n'
             f'- 「起きた／起きなかった」が変わった組（構成 × ケース × 数えたもの）の数：{"、".join(total_flip)}\n'
             '- 並行の競合や時間に頼る数は、試行の数が少ない（多くは 3〜5 回）。0 と 1 以上が 2 回で入れ替わった組は、'
             'どちらの回の結果も「この条件で起きうる／起きないこともある」と読む。各題材の summary.md は、こうした行に 2 回の値を並べている\n')
    L += body
    text = '\n'.join(L).rstrip() + '\n'
    with open(OUT, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)
    update_readme()
    return OUT


if __name__ == '__main__':
    print(main())
