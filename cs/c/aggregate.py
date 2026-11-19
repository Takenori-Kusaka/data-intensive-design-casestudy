"""Build results/c/summary.md from results/c/*.jsonl. Every number in the tables is counted
from the result files; nothing is typed by hand. The verdicts of the comparison with the
guide are derived from the same numbers (agg_sections.py holds the per-experiment tables)."""
import json
import os

from ..common.record import RESULTS, read_jsonl, update_env
from . import agg_sections as A, config as C
from .agg_sections import NA, table

OUT = os.path.join(RESULTS, 'c', 'summary.md')


class Data:
    def __init__(self):
        for k in ('c1', 'c2', 'c3', 'c4', 'c5', 'c6', 'audit'):
            setattr(self, k, read_jsonl(os.path.join(RESULTS, 'c', f'{k}.jsonl')))
        self.perf = [r for r in self.c2 if r['part'] == 'perf']
        self.drift = [r for r in self.c2 if r['part'] == 'drift']
        self.compress = [r for r in self.c3 if r['part'] == 'compress']
        self.retain = [r for r in self.c3 if r['part'] == 'retain']
        self.runs = self.c1 + self.c2 + self.c3 + self.c4 + self.c5 + self.c6


def _env():
    p = os.path.join(RESULTS, 'env.json')
    return json.load(open(p, encoding='utf-8')) if os.path.exists(p) else {}


def _mib(s):
    try:
        return float(s.replace('MiB', '')) if s.endswith('MiB') else float(s.replace('GiB', '')) * 1024
    except ValueError:
        return 0.0


def overview(d):
    out = ['## 実行の概要\n']
    names = [('c1', 'C-1 行指向と列指向'), ('c2', 'C-2 取り込み時スキーマと読み出し時スキーマ'), ('c3', 'C-3 保持と圧縮'),
             ('c4', 'C-4 冪等な再実行'), ('c5', 'C-5 索引の費用'), ('c6', 'C-6 Collector の送信で失う場面')]
    rows = [[ja, f'{k}.jsonl', len(getattr(d, k))] for k, ja in names]
    rows.append(['判定器の監査', 'audit.jsonl', f'{len(d.audit)}（合格 {sum(1 for r in d.audit if r["ok"])}）'])
    out.append(table(['実験', '結果のファイル', '実行の数（行）'], rows))
    frees = [r['windows_free_gb_before'] for r in d.runs if r.get('windows_free_gb_before') is not None]
    out.append('1 回の実行ごとに、`dic-c-` のコンテナとボリュームを消して作り直した。同時に動かしたサーバーのコンテナは 1 つ'
               '（C-6 だけ Collector 2 つと Redpanda）。乱数の種は 20261120（C-4 は＋試行番号）。'
               '数の書き方：「中央値（最小〜最大）」は試行の間の幅。問い合わせの時間は、1 回の実行の中で暖機 1 回のあと '
               f'{C.REPS} 回繰り返した中央値を、試行ごとに 1 つ取ったもの。\n')
    env = _env()
    out.append('### 製品の版とイメージ\n')
    rows = [[img, '、'.join((env.get('images', {}).get(img) or {}).get('repo_digests', [])) or NA,
             f'{(env.get("images", {}).get(img) or {}).get("size_mb", NA)} MB'] for img in C.IMAGES.values()]
    out.append(table(['イメージ（タグ）', 'ダイジェスト', '大きさ'], rows))
    sv = env.get('topic_c_server_versions') or {}
    tc = {k: v for k, v in (env.get('topic_c') or {}).items() if k not in ('limits',)}
    out.append('サーバーとエンジンが返した版：' + ('、'.join(f'{k} {v}' for k, v in sorted(sv.items())) or NA)
               + '。クライアントと実行日（UTC）：' + ('、'.join(f'{k} {v}' for k, v in sorted(tc.items())) or NA)
               + '。DuckDB は Python のパッケージ（プロセスの中で動く）。\n')
    peak = {}
    for r in d.runs:
        for k, v in (r.get('container_memory') or {}).items():
            if _mib(v) > _mib(peak.get(k, '0MiB')):
                peak[k] = v
    if peak:
        update_env('topic_c_container_memory_seen', peak)
        out.append('コンテナのメモリ（各実行の終わりに docker stats で見た値の最大）：'
                   + '、'.join(f'{k} {v}' for k, v in sorted(peak.items())) + '。'
                   + (f'実行の直前に測った Windows の空きメモリの最小は {min(frees)} GB（{len(frees)} 回の測定）。\n' if frees else '\n'))
    out.append('### 設定（既定のままか、変えたか）\n')
    pg = next((r['settings'] for r in d.c1 if r['product'] == 'pg'), {})
    ch = next((r['settings'] for r in d.c1 if r['product'] == 'ch'), {})
    dk = next((r['settings'] for r in d.c1 if r['product'] == 'duck'), {})
    rows = [
        ['PostgreSQL', f'コンテナのメモリ {C.PG_MEMORY}、shared_buffers={C.PG_SHARED_BUFFERS}、max_connections=50',
         '、'.join(f'{k}={v}' for k, v in sorted(pg.items()) if k != 'shared_buffers') or NA],
        ['ClickHouse', f'コンテナのメモリ {C.CH_MEMORY}、max_server_memory_usage={C.CH_MAX_SERVER_MEMORY}、'
                       'mark_cache_size=128MiB、uncompressed_cache_size=0、ログの水準 warning',
         '、'.join(f'{k}={v}' for k, v in sorted(ch.items())) or NA],
        ['DuckDB', f'memory_limit={C.DUCK_MEMORY}', '、'.join(f'{k}={v}' for k, v in sorted(dk.items()) if k != 'memory_limit') or NA],
    ]
    out.append(table(['製品', '変えた設定', '既定のままの設定（製品が表示した値）'], rows))
    out.append('実験ごとに変えた設定（ClickHouse の async_insert、mutations_sync、input_format_allow_errors_num、'
               'non_replicated_deduplication_window、Collector の再試行の時間など）は、各節の表に書いた。'
               '索引・並び順・コーデックは実験の変数で、製品を速くするための調整はしていない。\n')
    return out


def audit_section(d):
    out = ['## 判定器の監査\n',
           '誤りを仕込んだデータと、誤りの無いデータを判定器に流し、仕込んだ数だけを数えることを確かめた（`cs/c/audit.py`、結果は audit.jsonl）。'
           'end_to_end は、生成器の CSV を DuckDB に読み、1 か所だけ誤りを仕込んでから Parquet に書き、6 本の問い合わせの照合が、'
           '影響を受ける問い合わせだけを不一致と数えることを確かめたもの。\n']
    by = {}
    for r in d.audit:
        by.setdefault(r['judge'], []).append(r)
    rows = [[j, len(rs), sum(1 for r in rs if r['ok']), '、'.join(r['case'] for r in rs)] for j, rs in by.items()]
    out.append(table(['判定器', 'ケースの数', '合格', 'ケース'], rows))
    n_checks = (sum(len(r['queries']) * (r['reps'] + 1) for r in d.c1)
                + sum(len(r['queries']) * (r['reps'] + 1) for r in d.perf)
                + sum(len(r['queries']) * (r['reps'] + 1) for r in d.compress)
                + sum(len(r['reads']) * (r['reps'] + 1) for r in d.c5))
    mism = sum(r['mismatch_total'] for r in d.c1 + d.perf + d.compress + d.c5)
    out.append(f'時間を比べた問い合わせは、暖機も含めて毎回、結果を生成器の正解と突き合わせた：{n_checks} 回の照合で不一致は {mism} 件'
               f'（C-1・C-2 の性能・C-3 の圧縮・C-5）。3 つの製品の結果が同じ正解に一致したうえで、時間を比べている。\n')
    return out


def main():
    d = Data()
    out = ['# 題材 C：共通基盤の費用と性能（結果）\n',
           'このファイルは `cs/c/aggregate.py` が `results/c/*.jsonl` から作る。表の数は結果のファイルから数えたもので、手では書いていない。'
           '性能の数字は、同じ機械・同じデータの中での相対比較で、ほかの環境には一般化できない。製品どうしの優劣は断定せず、'
           'どの問い合わせの形で、どちらに差が出たかを書く。\n',
           '問い：ログ基盤・分析基盤で、どの持ち方が何を払うか。題材は、架空の宅配注文サービスの注文イベントのログ（1 行が 1 イベント、23 列）。'
           '構成は **N**（素朴な作り：行指向の表に、製品の既定のまま型つきで取り込み、INSERT で流し直し、DELETE で古い行を消す）、'
           '**B**（共通の基準：再実行しても同じ結果になる取り込み）、**BE**（基準＋題材 C の重点：E-C1 列指向と圧縮、'
           'E-C2 取り込み時に型を決める列と後から読む列を分けて検査する、E-C3 保持期間をパーティションの単位で落とす）。\n']
    out += overview(d)
    out += A.comparison(d)
    out += A.c1_section(d)
    out += A.c2_section(d)
    out += A.c3_section(d)
    out += A.c4_section(d)
    out += A.c5_section(d)
    out += A.c6_section(d)
    out += A.ablation(d)
    out += A.normal_ops(d)
    out += audit_section(d)
    out += A.findings(d)
    out += A.closing(d)
    text = '\n'.join(out).rstrip() + '\n'
    with open(OUT, 'w', encoding='utf-8', newline='\n') as f:
        f.write(text)
    return OUT


if __name__ == '__main__':
    print(main())
