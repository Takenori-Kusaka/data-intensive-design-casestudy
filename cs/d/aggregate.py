"""Build results/d/summary.md from the JSONL files. Every number in the tables is
counted here from the result files; nothing is typed in by hand."""
import os
import re
import statistics
from collections import defaultdict

from ..common import tworuns
from ..common.record import RESULTS, read_jsonl
from . import config as C
from .d1_hermitage import TEST_IDS, TEST_JA
from .d3_defenses import INV_JA, INV_STRATEGIES, WS_JA, WS_STRATEGIES

OUT = os.path.join(RESULTS, 'd', 'summary.md')
# rr_naive retries on 40001, but snapshot isolation does not raise it for write skew: say what the strategy is
WS_JA = dict(WS_JA, rr_naive='Repeatable Read（スナップショット分離。40001 が返れば再試行する作り）')
P = {'pg': 'PostgreSQL', 'mysql': 'MySQL', 'crdb': 'CockroachDB'}
LV = C.LEVEL_JA
FIVE = [('G1a', 'ダーティリード'), ('P4', 'ロストアップデート'), ('GS', 'リードスキュー'),
        ('G2I', 'ライトスキュー'), ('PMP', 'ファントム')]

# Hermitage README table (as of 2026-11): True = prevented, False = occurs, 'RO' = prevented only read-only
H_COLS = ['G0', 'G1a', 'G1b', 'G1c', 'OTV', 'PMP', 'P4', 'GS', 'G2I', 'G2']
_T, _F = True, False
HERMITAGE = {
    ('pg', 'rc'): [_T, _T, _T, _T, _T, _F, _F, _F, _F, _F],
    ('pg', 'rr'): [_T, _T, _T, _T, _T, _T, _T, _T, _F, _F],
    ('pg', 'ser'): [_T] * 10,
    ('mysql', 'ru'): [_T, _F, _F, _F, _F, _F, _F, _F, _F, _F],
    ('mysql', 'rc'): [_T, _T, _T, _T, _T, _F, _F, _F, _F, _F],
    ('mysql', 'rr'): [_T, _T, _T, _T, _T, 'RO', _F, 'RO', _F, _F],
    ('mysql', 'ser'): [_T] * 10,
    ('crdb', 'rc'): [_T, _T, _T, _T, _T, _F, _F, _F, _F, _F],
    ('crdb', 'rr'): [_T, _T, _T, _T, _T, _T, _T, _T, _F, _F],
    ('crdb', 'ser'): [_T] * 10,
}
RO_WRITE_VARIANT = {'PMP': 'PMPW2', 'GS': 'GSW'}


# Columns whose value is a count that depends on a concurrent race or on the instant of a kill:
# where the two runs differ, both values are written (see cs/common/tworuns.py).
COUNTS = re.compile('ロストアップデート|売り越し|失われた応答済み|^試行ごと$|失った試行|起きた試行|違反したエリア|正常な要求の失敗'
                    '|応答なしで残った行|エラーで終わった要求|在庫が負|引き当て／正しい件数|通った申請|失敗した要求|読めなかった|応答済みに対する割合|再試行|エラーコード|^応答済み（合計）$')
# Columns of the ablation table: prose built from counts, written for both runs where they differ.
PROSE = re.compile('D-4）$|^在庫の引き当て|^配達員の休憩|D-7）$|^費用・正常な操作')


def load(name, run=2):
    return read_jsonl(os.path.join(RESULTS, 'd', *([] if run == 2 else ['run1']), name + '.jsonl'))


def med(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 2) if xs else '未検証'


def table(header, rows):
    if not rows:
        return '（結果のファイルに該当の行が無い。未検証）\n'
    out = ['| ' + ' | '.join(header) + ' |', '| ' + ' | '.join(['---'] * len(header)) + ' |']
    for r in rows:
        out.append('| ' + ' | '.join(str(x) if x not in (None, '') else '該当なし' for x in r) + ' |')
    return '\n'.join(out) + '\n'


def mech_ja(m):
    parts = []
    for p in m.split('+'):
        if p == 'wait':
            parts.append('待ち')
        elif p.startswith('error:'):
            codes = p[6:].replace('mysql:1213', '1213（デッドロック）').replace('mysql:1205', '1205（ロック待ちの時間切れ）')
            parts.append('エラー ' + codes)
    return '＋'.join(parts)


class D1:
    def __init__(self, rows):
        self.cells = defaultdict(list)
        self.actual = defaultdict(set)
        self.probe = {}
        for r in rows:
            if r['exp'] == 'd1_probe':
                self.probe[r['product']] = r
            else:
                self.cells[(r['product'], r['level'], r['test'])].append(r)
                self.actual[(r['product'], r['level'])].update(r['actual_level'])

    def verdicts(self, p, lv, t):
        return [r['verdict'] for r in self.cells.get((p, lv, t), [])]

    def state(self, p, lv, t):
        """'anomaly' / 'prevented' when all trials agree, 'mixed', or None (no data)."""
        v = set(self.verdicts(p, lv, t))
        if not v:
            return None
        return v.pop() if len(v) == 1 else 'mixed'

    def mechs(self, p, lv, t):
        return sorted({r['mechanism'] for r in self.cells.get((p, lv, t), [])})

    def cell(self, p, lv, t, short=False):
        rows = self.cells.get((p, lv, t), [])
        if not rows:
            return '未検証'
        st = self.state(p, lv, t)
        n = len(rows)
        if st == 'mixed':
            a = sum(1 for r in rows if r['verdict'] == 'anomaly')
            return f'試行で割れた（起きた {a}/{n}）'
        if st == 'inconclusive':
            return '判定できず'
        m = '／'.join(mech_ja(x) for x in self.mechs(p, lv, t) if mech_ja(x))
        if st == 'anomaly':
            if short:
                return '×'
            return '**起きた**' + (f'（{m}の後）' if m else '')
        if short:
            return '○' + ('' if not m else ('w' if '待ち' in m else '') + ('e' if 'エラー' in m else ''))
        return '防いだ（' + (m if m else '待ち・エラーなし') + '）'

    def levels(self, p):
        return [lv for lv in C.PRODUCT_LEVELS[p] if any(k[0] == p and k[1] == lv for k in self.cells)]


def sec_d1(d1):
    L = []
    L.append('## D-1 分離レベルごとの異常\n')
    L.append('2〜3 個のセッションで文の実行順を固定し、各セッションが読んだ行と、最後の表の中身で判定した。'
             '「待ち」は、文を出してから一定時間（PostgreSQL・MySQL 0.6 秒、CockroachDB 1.2 秒）応答が無かったことを指す。'
             '手順の考え方は Hermitage（Martin Kleppmann、CC BY 4.0）による。P2・P3・P3W・MAVW・PMPW2 はこのケーススタディで足した手順。\n')
    L.append('### サーバーが表示した水準\n')
    rows = []
    for p in P:
        pr = d1.probe.get(p)
        if not pr:
            continue
        lv = pr['levels_default']
        rows.append([P[p], pr['version'][:60], lv.get('default')] + [lv.get(k, '未検証') for k in ('ru', 'rc', 'rr', 'ser')]
                    + ['既定の設定のまま'])
        if 'levels_enabled' in pr:
            le = pr['levels_enabled']
            rows.append([P[p], pr['version'][:60], le.get('default')] + [le.get(k, '未検証') for k in ('ru', 'rc', 'rr', 'ser')]
                        + ['クラスタ設定を有効にした後（下の注）'])
    L.append(table(['製品', '版', '既定の水準', 'READ UNCOMMITTED を指定', 'READ COMMITTED を指定', 'REPEATABLE READ を指定',
                    'SERIALIZABLE を指定', '条件'], rows))
    pr = d1.probe.get('crdb')
    if pr:
        L.append(f'注（CockroachDB）：既定のクラスタ設定は {pr["settings_default"]}。'
                 f'有効にした後は {pr["settings_enabled"]}。以下の CockroachDB の結果は、有効にした後のもの。\n')
    L.append('### ガイドが名前を挙げた 5 つの異常\n')
    rows = []
    for p in P:
        for lv in d1.levels(p):
            act = '／'.join(sorted(d1.actual[(p, lv)]))
            n = len(d1.cells.get((p, lv, 'P4'), []))
            rows.append([P[p], LV[lv], act] + [d1.cell(p, lv, t) for t, _ in FIVE] + [n])
    L.append(table(['製品', '指定した水準', 'サーバーの表示'] + [f'{ja}（{t}）' for t, ja in FIVE] + ['試行（各セル）'], rows))
    L.append('### すべての手順\n')
    L.append('○＝防いだ、×＝起きた。w＝待ちがあった、e＝エラーが返った。添え字なしの ○ は、待ちもエラーも無く防いだ（版の読み分け）。\n')
    rows = []
    for p in P:
        for lv in d1.levels(p):
            rows.append([P[p], LV[lv]] + [d1.cell(p, lv, t, short=True) for t in TEST_IDS])
    L.append(table(['製品', '水準'] + TEST_IDS, rows))
    L.append('手順の名前：' + '、'.join(f'{t}＝{TEST_JA[t]}' for t in TEST_IDS) + '。\n')
    L.append('### 防ぎ方（エラーコード）の内訳\n')
    rows = []
    for p in P:
        for lv in d1.levels(p):
            for t in TEST_IDS:
                ms = [m for m in d1.mechs(p, lv, t) if 'error' in m]
                if ms and d1.state(p, lv, t) == 'prevented':
                    rows.append([P[p], LV[lv], t, '／'.join(mech_ja(m) for m in ms)])
    L.append(table(['製品', '水準', '手順', '防ぎ方'], rows))
    # determinism
    mixed = [(p, lv, t) for (p, lv, t) in d1.cells if d1.state(p, lv, t) in ('mixed', 'inconclusive')]
    total = sum(len(v) for v in d1.cells.values())
    L.append(f'実行の数：{total} 回（{len(d1.cells)} セル）。試行のあいだで判定が割れた・判定できなかったセル：{len(mixed)} 個'
             + ('（' + '、'.join(f'{P[p]} {LV[lv]} {t}' for p, lv, t in sorted(mixed)) + '）' if mixed else '') + '。\n')
    # Hermitage comparison
    L.append('### Hermitage の表との照合\n')
    rows, diff = [], 0
    for (p, lv), exp in HERMITAGE.items():
        for t, e in zip(H_COLS, exp):
            st = d1.state(p, lv, t)
            if st is None:
                continue
            if e == 'RO':
                w = RO_WRITE_VARIANT[t]
                ok = st == 'prevented' and d1.state(p, lv, w) == 'anomaly'
                got = f'読み取りだけ（{t}）：{d1.cell(p, lv, t, True)}、書き込みを伴う（{w}）：{d1.cell(p, lv, w, True)}'
                want = '読み取りだけなら防ぐ（R/O）'
            else:
                ok = (st == 'prevented') == e
                got = d1.cell(p, lv, t, True)
                want = '防ぐ' if e else '防がない'
            if not ok:
                diff += 1
                rows.append([P[p], LV[lv], t, want, got])
    n_cmp = sum(1 for (p, lv), exp in HERMITAGE.items() for t in H_COLS if d1.state(p, lv, t) is not None)
    L.append(f'Hermitage の README の表（PostgreSQL・MySQL/InnoDB・CockroachDB の行、{n_cmp} セル）と比べた。食い違ったセル：{diff} 個。\n')
    if rows:
        L.append(table(['製品', '水準', '手順', 'Hermitage', '実測'], rows))
    return '\n'.join(L)


def _sumk(rows, k):
    return sum(r.get(k, 0) for r in rows)


def _codes(rows):
    c = defaultdict(int)
    for r in rows:
        for k, v in r.get('error_codes', {}).items():
            c[k.replace('mysql:', 'MySQL ')] += v
    return '、'.join(f'{k}×{v}' for k, v in sorted(c.items())) or 'なし'


def sec_d2(rows):
    L = ['## D-2 並行の負荷での発生率\n']
    if rows:
        r0 = rows[0]
        L.append(f'在庫の引き当て（読む → クライアントで 1 を引く → 書き戻す）を 1 回 {r0["n_ops"]} 件、{r0["k_items"]} 品目に散らして実行した。'
                 '在庫は十分にあり、応答済みの引き当て 1 件につき在庫がちょうど 1 減るはずである。'
                 'ロストアップデートの件数＝最終の在庫数 −（初期の在庫数 − 応答済みの件数）。直列化の失敗とデッドロックは、トランザクション全体を最大 50 回まで再試行した。'
                 '種は試行ごとに固定（' + '、'.join(str(s) for s in sorted({r["seed"] for r in rows})) + '）。\n')
    g = defaultdict(list)
    for r in rows:
        g[(r['product'], r['level'], r['concurrency'])].append(r)
    out = []
    for p in P:
        for lv in ('ru', 'rc', 'rr', 'ser'):
            for c in sorted({k[2] for k in g}):
                rs = g.get((p, lv, c))
                if not rs:
                    continue
                served = _sumk(rs, 'served')
                lost = _sumk(rs, 'lost_updates')
                out.append([P[p], LV[lv], c, len(rs), served, lost, f'{sum(1 for r in rs if r["lost_updates"] > 0)}/{len(rs)}',
                            f'{round(100 * lost / served, 1)}%' if served else '0%', _sumk(rs, 'retries'), _sumk(rs, 'failed'),
                            _codes(rs), med([r['duration_s'] for r in rs]), med([r['latency']['p50_ms'] for r in rs]),
                            med([r['latency']['p95_ms'] for r in rs])])
    L.append(table(['製品', '水準', '並行度', '試行', '応答済み（合計）', 'ロストアップデート（合計）', '起きた試行', '応答済みに対する割合',
                    '再試行（合計）', '失敗した要求', 'エラーコード×回数', '所要時間（秒、中央値）', 'p50（ms、中央値）', 'p95（ms、中央値）'], out))
    L.append(f'実行の数：{len(rows)} 回。CockroachDB はポートの中継（Toxiproxy、遅延なし）を通るので、所要時間はほかの製品と直接は比べられない。\n')
    return '\n'.join(L)


def sec_d3(rows):
    L = ['## D-3 防ぎ方の費用（PostgreSQL）\n']
    inv = [r for r in rows if r['workload'] == 'inv']
    ws = [r for r in rows if r['workload'] == 'ws']
    if inv:
        r0 = inv[0]
        L.append(f'### 在庫の引き当て（品切れあり）\n\n1 回 {r0["n_ops"]} 件の要求。各品目の在庫より要求が多く、'
                 f'正しくは {r0["expected_served"]} 件が引き当てられ、残りは品切れで断られる。'
                 '「正常な要求の失敗」＝引き当てられるはずだったのに引き当てられなかった件数。「売り越し」＝在庫を超えて引き当てた件数。'
                 '「エラーコード×回数」は、DB が返したエラーの SQLSTATE と回数。23514 は CHECK 制約の違反で、'
                 '「アトミックな更新＋CHECK 制約」では、品切れの要求がこのエラーで断られた回数である（正しい拒否で、正常な要求の失敗ではない）。'
                 '40001 は直列化の失敗、app:version はバージョン列の不一致（アプリケーションが数えたもの）。\n')
    g = defaultdict(list)
    for r in inv:
        g[(r['strategy'], r['concurrency'])].append(r)
    out = []
    for s in INV_STRATEGIES:
        for c in sorted({k[1] for k in g}):
            rs = g.get((s, c))
            if not rs:
                continue
            out.append([INV_JA[s], c, len(rs), _sumk(rs, 'lost_updates'), _sumk(rs, 'oversold'), _sumk(rs, 'negative_items'),
                        _sumk(rs, 'false_rejects'), f'{_sumk(rs, "served")}/{_sumk(rs, "expected_served")}', _sumk(rs, 'retries'),
                        _codes(rs), med([r['latency']['p50_ms'] for r in rs]), med([r['latency']['p95_ms'] for r in rs]),
                        med([r['duration_s'] for r in rs])])
    L.append(table(['手段', '並行度', '試行', 'ロストアップデート（合計）', '売り越し（合計）', '在庫が負の品目', '正常な要求の失敗（合計）',
                    '引き当て／正しい件数', '再試行（合計）', 'エラーコード×回数', 'p50（ms、中央値）', 'p95（ms、中央値）', '所要時間（秒、中央値）'], out))
    if ws:
        r0 = ws[0]
        L.append(f'### ライトスキュー（配達員の休憩）\n\n{r0["areas"]} エリア、各エリアに稼働中が 2 人。2 人が同時に休憩を申請する（1 回 {r0["n_ops"]} 件）。'
                 '決まりは「稼働中が 1 人以上」。正しくは各エリアで 1 件だけ通る。「違反」＝稼働中が 0 人になったエリアの数。'
                 '「正常な要求の失敗」＝1 件も通らなかったエリアの数。\n')
    g = defaultdict(list)
    for r in ws:
        g[(r['strategy'], r['concurrency'])].append(r)
    out = []
    for s in WS_STRATEGIES:
        for c in sorted({k[1] for k in g}):
            rs = g.get((s, c))
            if not rs:
                continue
            out.append([WS_JA[s], c, len(rs), _sumk(rs, 'violations'), f'{sum(1 for r in rs if r["violations"] > 0)}/{len(rs)}',
                        _sumk(rs, 'ws_false_rejects'), _sumk(rs, 'ws_served'), _sumk(rs, 'retries'), _sumk(rs, 'failed'), _codes(rs),
                        med([r['latency']['p50_ms'] for r in rs]), med([r['latency']['p95_ms'] for r in rs])])
    L.append(table(['手段', '並行度', '試行', '違反したエリア（合計）', '起きた試行', '正常な要求の失敗（合計）', '通った申請（合計）',
                    '再試行（合計）', 'エラーで終わった要求', 'エラーコード×回数', 'p50（ms、中央値）', 'p95（ms、中央値）'], out))
    L.append(f'実行の数：在庫 {len(inv)} 回、ライトスキュー {len(ws)} 回。\n')
    return '\n'.join(L)


def _names(n):
    return f"'{n}'" if n else "''（空）"


def sec_d4(rows):
    L = ['## D-4 同期と非同期の複製：昇格で失われた応答済みコミット\n']
    L.append('PostgreSQL 18 のプライマリ＋スタンバイ 1 台（ストリーミングレプリケーション）。レプリケーションの経路は Toxiproxy を通し、'
             '件数を合図に切った。コミットは 1 行の INSERT で、エラーなく返ったものを「応答済み」とした。'
             '昇格は `pg_promote()`。失われた件数＝応答済みなのに、昇格後のスタンバイ（または再起動後のプライマリ）に無い行の数。'
             '「応答なしで残った行」＝クライアントに成功の応答が返らなかった（強制終了で接続が切れた）のに、昇格後のスタンバイ（または再起動後のプライマリ）にある行の数。'
             '応答が無いことは、書かれていないことを意味しない。\n')
    desc = {
        'cut': '経路を切ってから強制終了（書き手 1。200 件の応答とスタンバイへの反映を確かめた後に経路を切り、さらに 200 件の応答か 5 秒の無応答まで書いて、プライマリを SIGKILL、スタンバイを昇格）',
        'kill': '経路を切らずに強制終了（書き手 4。応答済みが 1000 件に達した時点でプライマリを SIGKILL、スタンバイを昇格）',
        'crash_restart': '昇格しない（書き手 4。応答済みが 1000 件に達した時点でプライマリを SIGKILL し、同じプライマリを起動し直して読む）',
    }
    for mode in ('cut', 'kill', 'crash_restart'):
        rs_m = [r for r in rows if r['mode'] == mode]
        L.append(f'### {desc[mode]}\n')
        g = defaultdict(list)
        for r in rs_m:
            g[(r['synchronous_standby_names'], r['synchronous_commit'])].append(r)
        out = []
        for names in ('', 'dic_s1'):
            for sc in ('off', 'local', 'remote_write', 'on', 'remote_apply'):
                rs = g.get((names, sc))
                if not rs:
                    continue
                rs = sorted(rs, key=lambda r: r['trial'])
                row = [_names(names), sc, len(rs), _sumk(rs, 'acked'), _sumk(rs, 'lost_acked'),
                       '、'.join(str(r['lost_acked']) for r in rs), f'{sum(1 for r in rs if r["lost_acked"] > 0)}/{len(rs)}',
                       _sumk(rs, 'unacked_present')]
                if mode == 'cut':
                    row += ['、'.join(str(r['acked_after_cut']) for r in rs),
                            f'{sum(1 for r in rs if r["stalled"])}/{len(rs)}']
                out.append(row)
        head = ['synchronous_standby_names', 'synchronous_commit', '試行', '応答済み（合計）', '失われた応答済み（合計）',
                '試行ごと', '失った試行', '応答なしで残った行（合計）']
        if mode == 'cut':
            head += ['経路を切った後に応答された件数（試行ごと）', '書き込みが止まった試行（5 秒の観測）']
        L.append(table(head, out))
        if mode == 'kill':
            hit = [f"{_names(n)}・{sc}：{'、'.join(str(r['lost_acked']) for r in sorted(g[(n, sc)], key=lambda r: r['trial']))}"
                   for n in ('', 'dic_s1') for sc in ('local', 'remote_write', 'on', 'remote_apply')
                   if g.get((n, sc)) and _sumk(g[(n, sc)], 'lost_acked') > 0]
            L.append('synchronous_commit が off 以外の行で、失われた応答済みが 1 件以上あったもの（試行ごとの件数）：'
                     + ('；'.join(hit) if hit else 'なし') + '。'
                     'この表の件数は、強制終了の瞬間にスタンバイへ届いていなかった分で、試行によって 0 にも 1 件以上にもなる。'
                     '同期スタンバイを名指ししていない行の 0 件は「この試行では届いていた」という意味で、失わない設定だという意味ではない。\n')
    L.append(f'実行の数：{len(rows)} 回。「経路を切ってから」の失われた件数は、切った後に応答された件数で決まる（この実験では上限 200 件）。'
             '割合ではなく、設定がその窓の書き込みを守るかどうかを見る数である。\n')
    return '\n'.join(L)


def sec_d5(rows):
    L = ['## D-5 損失ゼロの設定の費用\n']
    lat = [r for r in rows if r['exp'] == 'd5_latency']
    st = [r for r in rows if r['exp'] == 'd5_stall']
    L.append('### コミットの遅さ\n')
    if lat:
        L.append(f'1 行の INSERT を直列に {lat[0]["latency"]["n"]} 回。経路の遅延は Toxiproxy の latency で、片道ごとに足した値。'
                 '同じ 1 台の上なので、遅延 0 の行はネットワークの往復がほぼ無い条件。\n')
    g = defaultdict(list)
    for r in lat:
        g[(r['synchronous_standby_names'], r['path_delay_ms_each_way'], r['synchronous_commit'])].append(r)
    out = []
    for names in ('', 'dic_s1'):
        for d in sorted({k[1] for k in g}):
            for sc in ('off', 'local', 'remote_write', 'on', 'remote_apply'):
                rs = g.get((names, d, sc))
                if rs:
                    out.append([_names(names), sc, d, len(rs), med([r['latency']['p50_ms'] for r in rs]),
                                med([r['latency']['p95_ms'] for r in rs])])
    L.append(table(['synchronous_standby_names', 'synchronous_commit', '経路の遅延（ms、片道）', '試行', 'p50（ms、中央値）', 'p95（ms、中央値）'], out))
    L.append('### 同期スタンバイを止めたとき\n')
    L.append("synchronous_commit = on、同期スタンバイを名指しした構成で、50 件の書き込みの後にスタンバイを `docker stop` で止め、"
             '書き込みを 1 件出して、決めた時間だけ観測した。クライアントからは打ち切らない。\n')
    kinds = {'standby_back': 'スタンバイを起動し直す', 'reload_empty': "synchronous_standby_names を空にして再読み込み",
             'cancel': '待っているバックエンドを pg_cancel_backend()', 'primary_restart': '待っている最中にプライマリを再起動',
             'any1of2': "'ANY 1 (dic_s1, dic_s2)' でスタンバイを 2 台とも止め、1 台を起動し直す"}
    out, out2 = [], []
    for k, ja in kinds.items():
        rs = sorted([r for r in st if r['kind'] == k], key=lambda r: r['trial'])
        if not rs:
            continue
        dw = [r['during_wait'] for r in rs]
        we = sorted({'/'.join(map(str, w['wait_event'][0])) if w['wait_event'] else 'なし' for w in dw})
        out.append([ja, len(rs), rs[0]['window_s'], f'{sum(1 for r in rs if r["first_write_returned_in_window"])}/{len(rs)}',
                    '、'.join(str(r['first_write_waited_s']) for r in rs), '、'.join(we),
                    f'{sum(1 for w in dw if w["read_only_query"].get("returned"))}/{len(rs)}',
                    f'{sum(1 for w in dw if w["rolled_back_txn"].get("returned"))}/{len(rs)}',
                    f'{sum(1 for w in dw if w["second_write_returned_in_3s"])}/{len(rs)}',
                    f'{sum(1 for w in dw if w["same_key_write_returned_in_3s"])}/{len(rs)}',
                    '、'.join(str(w["row_visible_to_others"].get("value")) for w in dw)])
        af = [r['after_fix'] for r in rs]
        out2.append([ja, f'{sum(1 for a in af if a["first_write_returned"])}/{len(rs)}',
                     '、'.join(str(a['returned_after_fix_s']) for a in af),
                     '／'.join(sorted({str(a['first_write_outcome'])[:70] for a in af})),
                     '／'.join(sorted({n[:110] for a in af for n in a['first_write_notices']})) or 'なし',
                     '／'.join(sorted({str(a['rows_1001_1002_on_primary']) for a in af})),
                     '／'.join(sorted({str(a.get('new_write_returned_in_5s', '測っていない')) for a in af}))])
    L.append(table(['抜け出し方（この表は抜け出す前の観測）', '試行', '観測の時間（秒）', '時間内に書き込みが返った試行', '待った時間（秒、試行ごと）',
                    '待ちの種類（pg_stat_activity）', '読み取りが返った', 'ロールバックが返った', '別の書き込みが 3 秒で返った',
                    '同じキーへの書き込みが 3 秒で返った', '待っている行がほかのセッションから見えた数'], out))
    L.append(table(['抜け出し方', '待っていた書き込みが返った', '操作から返るまで（秒、試行ごと）', 'クライアントに返った結果', 'サーバーの通知',
                    'プライマリにある行（1001＝待っていた行、1002＝2 件目）', 'その後の新しい書き込みが 5 秒で返った'], out2))
    a = [r for r in st if r['kind'] == 'any1of2' and 'one_standby_down' in r]
    if a:
        L.append("'ANY 1 (dic_s1, dic_s2)' で 1 台だけ止めた間の書き込み：" + '、'.join(
            f'試行 {r["trial"]}＝{r["one_standby_down"]["acked"]} 件すべて応答（p50 {r["one_standby_down"]["p50_ms"]} ms、止める前の p50 {r["baseline"]["p50_ms"]} ms）'
            for r in sorted(a, key=lambda r: r['trial'])) + '。\n')
    n_one, n_any = sum(1 for r in st if r['kind'] != 'any1of2'), sum(1 for r in st if r['kind'] == 'any1of2')
    L.append(f'実行の数：遅さ {len(lat)} 行（{len({r["trial"] for r in lat})} 試行）、停止 {len(st)} 回。'
             f'停止 {len(st)} 回の内訳は、同期スタンバイを 1 台だけ名指しした構成が {n_one} 回'
             f'（抜け出し方 {len({r["kind"] for r in st if r["kind"] != "any1of2"})} 通り × {len({r["trial"] for r in st})} 試行。照合の表の「{n_one} 試行」はこれ）と、'
             f"'ANY 1 (dic_s1, dic_s2)' の構成が {n_any} 回。"
             '「時間内に返らなかった」は、その観測の時間の間は返らなかったという意味で、永久に待つことを確かめたものではない。\n')
    return '\n'.join(L)


def sec_d6(rows):
    L = ['## D-6 古い読み取り\n']
    if rows:
        L.append(f'プライマリに 1 行を書き、応答の直後（次の文）にスタンバイで同じ行を探した。1 回 {rows[0]["n_writes"]} 件。'
                 '遅れは、スタンバイの `recovery_min_apply_delay` で作った。「読めなかった」＝直後の読み取りで行が無かった。\n')
    mj = {'async': "非同期（on、名前は空）", 'sync_on': "同期（on、スタンバイを名指し）", 'remote_apply': 'remote_apply（スタンバイを名指し）'}
    g = defaultdict(list)
    for r in rows:
        g[(r['mode'], r['apply_delay_ms'])].append(r)
    out = []
    for m in mj:
        for d in sorted({k[1] for k in g}):
            rs = g.get((m, d))
            if not rs:
                continue
            n, s = _sumk(rs, 'n'), _sumk(rs, 'stale')
            out.append([mj[m], d, len(rs), n, s, f'{round(100 * s / n, 1)}%', '、'.join(str(r['stale']) for r in sorted(rs, key=lambda r: r['trial'])),
                        med([r['commit_latency']['p50_ms'] for r in rs]), med([r['commit_latency']['p95_ms'] for r in rs]),
                        med([r['visible_after_ack']['p50_ms'] for r in rs]), med([r['visible_after_ack']['p95_ms'] for r in rs]),
                        _sumk(rs, 'not_visible_within_cap')])
    L.append(table(['構成', '適用の遅れ（ms）', '試行', '書き込み（合計）', '読めなかった（合計）', '割合', '試行ごと', 'コミットの p50（ms、中央値）',
                    'コミットの p95（ms、中央値）', '応答から読めるまで p50（ms）', '応答から読めるまで p95（ms）', '5 秒待っても読めなかった'], out))
    L.append(f'実行の数：{len(rows)} 回。\n')
    return '\n'.join(L)


def sec_d7(rows):
    L = ['## D-7 昇格の手順：遮断の有無\n']
    if rows:
        L.append(f'書き手 A がプライマリに {rows[0]["n_base"]} 件を書いた後、DB どうしの経路を切り（クライアントは両方に届く）、スタンバイを昇格した。'
                 f'その後、書き手 A は旧プライマリへ {rows[0]["n_after"]} 件、書き手 B は新プライマリへ {rows[0]["n_after"]} 件を書こうとする。'
                 '「遮断あり」は、昇格の前に旧プライマリを `docker stop` で止めた。旧プライマリの中身は、測定の後に起動し直して読んだ。\n')
    g = defaultdict(list)
    for r in rows:
        g[(r['replication'], r['fenced'])].append(r)
    rj = {'async': "非同期（名前は空）", 'sync': "同期（on、スタンバイを名指し）"}
    out = []
    for rp in rj:
        for f in (False, True):
            rs = sorted(g.get((rp, f), []), key=lambda r: r['trial'])
            if not rs:
                continue
            out.append([rj[rp], '遮断あり' if f else '遮断なし', len(rs),
                        '、'.join(str(r['old_acked_after_promotion']) for r in rs),
                        f'{sum(1 for r in rs if r["old_writer_stalled"])}/{len(rs)}',
                        '、'.join(str(r['new_acked']) for r in rs),
                        '、'.join(str(r['after_old_restart']['only_old']) for r in rs),
                        '、'.join(str(r['after_old_restart']['only_new']) for r in rs),
                        '、'.join(str(r['after_old_restart']['id_collisions']) for r in rs),
                        '／'.join(sorted({(r['old_writer_error'] or 'なし')[:60] for r in rs}))])
    L.append(table(['複製', '手順', '試行', '昇格後に旧プライマリが応答した書き込み（試行ごと）', '旧プライマリへの書き込みが止まった試行（5 秒の観測）',
                    '新プライマリが応答した書き込み', '旧プライマリにだけある行', '新プライマリにだけある行', '同じ ID で中身が違う行', '旧プライマリへの書き手のエラー'], out))
    L.append(f'実行の数：{len(rows)} 回。「昇格後に旧プライマリが応答した書き込み」が、両方に書けてしまった件数（スプリットブレイン）である。'
             'この件数は、書き手 A が試みた件数で決まる（上限 100 件）。\n')
    return '\n'.join(L)


# ---------------------------------------------------------------- ablation
def sec_ablation(d3, d4, d5, d7):
    L = ['## 除去実験：基準と重点を 1 つずつ外す\n']
    L.append('構成 BE＝共通の基準 B5（同期の複製：synchronous_commit = on と同期スタンバイの名指し）＋重点 E-D1（必要な箇所を Serializable にして再試行）'
             '＋E-D2（明示的なロック／バージョン列）＋E-D3（昇格の前に旧プライマリを遮断）。数は上の各実験の結果のファイルから数えた。\n')

    def d4sum(sc, names, mode):
        rs = [r for r in d4 if r['mode'] == mode and r['synchronous_commit'] == sc and r['synchronous_standby_names'] == names]
        return f'{_sumk(rs, "lost_acked")} 件／{len(rs)} 試行' if rs else '未検証'

    def d3sum(wl, s, key, conc=None):
        rs = [r for r in d3 if r['workload'] == wl and r['strategy'] == s and (conc is None or r['concurrency'] == conc)]
        unit = '回' if key == 'retries' else '件'
        return f'{_sumk(rs, key)} {unit}／{len(rs)} 試行' if rs else '未検証'

    def d7stall(rp, f):
        rs = [r for r in d7 if r['replication'] == rp and r['fenced'] == f]
        return f'旧プライマリへの書き込みが 5 秒の観測の間止まった試行 {sum(1 for r in rs if r["old_writer_stalled"])}/{len(rs)}' if rs else '未検証'

    def d4stall(sc, names):
        rs = [r for r in d4 if r['mode'] == 'cut' and r['synchronous_commit'] == sc and r['synchronous_standby_names'] == names]
        return (f'経路を切った後に書き込みが止まった試行 {sum(1 for r in rs if r["stalled"])}/{len(rs)}、'
                f'切った後に応答された書き込み {_sumk(rs, "acked_after_cut")} 件') if rs else '未検証'

    def d7sum(rp, f):
        rs = [r for r in d7 if r['replication'] == rp and r['fenced'] == f]
        return f'{_sumk(rs, "old_acked_after_promotion")} 件／{len(rs)} 試行' if rs else '未検証'

    def lat(sc, names, d):
        rs = [r for r in d5 if r['exp'] == 'd5_latency' and r['synchronous_commit'] == sc
              and r['synchronous_standby_names'] == names and r['path_delay_ms_each_way'] == d]
        return f'p50 {med([r["latency"]["p50_ms"] for r in rs])} ms・p95 {med([r["latency"]["p95_ms"] for r in rs])} ms' if rs else '未検証'

    st = [r for r in d5 if r['exp'] == 'd5_stall' and r['kind'] == 'standby_back']
    stall = (f'{sum(1 for r in st if not r["first_write_returned_in_window"])}/{len(st)} 試行で {st[0]["window_s"]} 秒の観測の間は返らなかった'
             if st else '未検証')
    rows = [
        ['BE（すべて入れる）',
         f'経路を切ってから強制終了：{d4sum("on", "dic_s1", "cut")}。切らずに強制終了：{d4sum("on", "dic_s1", "kill")}',
         f'ロストアップデート {d3sum("inv", "for_update", "lost_updates")}、売り越し {d3sum("inv", "for_update", "oversold")}（SELECT FOR UPDATE）。'
         f'バージョン列：{d3sum("inv", "version", "lost_updates")}',
         f'違反 {d3sum("ws", "ser_retry", "violations")}（Serializable＋再試行）',
         f'{d7sum("sync", True)}',
         f'コミットの遅さ：{lat("on", "dic_s1", 0)}（経路の遅延 0）、{lat("on", "dic_s1", 5)}（片道 5 ms）。'
         f'同期スタンバイを止めると {stall}。再試行：在庫 {d3sum("inv", "ser_retry", "retries")}、休憩 {d3sum("ws", "ser_retry", "retries")}。'
         f'正常な要求の失敗：{d3sum("ws", "ser_retry", "ws_false_rejects")}'],
        ['BE − B5（同期の複製を外す：名前を空に）',
         f'経路を切ってから強制終了：{d4sum("on", "", "cut")}。切らずに強制終了：{d4sum("on", "", "kill")}',
         '変わらない（複製の設定は 1 台の中の異常に関係しない。未検証ではなく対象外）', '変わらない（同上）', f'{d7sum("async", True)}（遮断あり）',
         f'コミットの遅さ：{lat("on", "", 0)}。{d4stall("on", "")}（BE は {d4stall("on", "dic_s1")}）'],
        ['BE − E-D1 の再試行（Serializable のまま再試行しない）', '変わらない（対象外）',
         f'在庫を Serializable で守る場合：ロストアップデート {d3sum("inv", "ser_noretry", "lost_updates")}、正常な要求の失敗 {d3sum("inv", "ser_noretry", "false_rejects")}',
         f'違反 {d3sum("ws", "ser_noretry", "violations")}、正常な要求の失敗（1 件も通らなかったエリア）{d3sum("ws", "ser_noretry", "ws_false_rejects")}、'
         f'エラーで終わった要求 {d3sum("ws", "ser_noretry", "failed")}', '変わらない（対象外）',
         f'再試行：休憩 {d3sum("ws", "ser_noretry", "retries")}。失敗はそのまま呼び出し側に返る'],
        ['BE − E-D1（Serializable を外し、Read Committed のまま）', '変わらない（対象外）', '変わらない（在庫は E-D2 で守る）',
         f'違反 {d3sum("ws", "rc_naive", "violations")}（Read Committed）、{d3sum("ws", "rr_naive", "violations")}（Repeatable Read）',
         '変わらない（対象外）', f'再試行：{d3sum("ws", "rc_naive", "retries")}'],
        ['BE − E-D2（明示的なロック・バージョン列を外し、読んで書き戻す）', '変わらない（対象外）',
         f'ロストアップデート {d3sum("inv", "naive_rc", "lost_updates")}、売り越し {d3sum("inv", "naive_rc", "oversold")}。'
         f'CHECK 制約だけ足した場合：ロストアップデート {d3sum("inv", "check_only", "lost_updates")}、売り越し {d3sum("inv", "check_only", "oversold")}',
         '変わらない（休憩は E-D1 で守る）', '変わらない（対象外）',
         f'再試行：{d3sum("inv", "naive_rc", "retries")}。正常な要求の失敗：{d3sum("inv", "naive_rc", "false_rejects")}'],
        ['BE − E-D3（遮断を外す）', '変わらない（対象外）', '変わらない（対象外）', '変わらない（対象外）',
         f'同期のまま：{d7sum("sync", False)}（{d7stall("sync", False)}）。非同期で遮断も外す：{d7sum("async", False)}（{d7stall("async", False)}）',
         '昇格の手順が 1 つ減る'],
    ]
    L.append(table(['構成', '失われた応答済みコミット（D-4）', '在庫の引き当て（D-3、並行度 1・4・16 の合計）', '配達員の休憩（D-3、並行度 1・4・16 の合計）',
                    '昇格後に旧プライマリが応答した書き込み（D-7）', '費用・正常な操作への影響'], rows))
    return '\n'.join(L)


# ---------------------------------------------------------------- guide comparison
def sec_guide(d1, d2, d3, d4, d5, d6, d7):
    L = ['## ガイドの記述との照合\n\n「ガイドの記述」の欄は、実測の結果を反映する前のガイド（コミット ad5dd6b）の文で、何と照合したかの記録である。「スライドの題」の欄は、いまのガイドの題に合わせてある。\n']
    L.append('ガイド `data-intensive-design-guide` の Part 8（deck/p08.js）と Part 9（deck/p09.js）の記述を、上の結果と 1 つずつ比べた。'
             '「一致」「食い違い」は結果のファイルから機械的に判定した。「未確認だった点の実測」は、ガイドが未確認・未検証と書いていた点。\n')
    rows = []

    def add(slide, claim, ok, measured, kind=None):
        if ok is None:
            verdict = '未検証（結果なし）'
        else:
            verdict = kind or ('一致' if ok else '**食い違い**')
        rows.append([slide, claim, verdict, measured])

    def st(p, lv, t):
        return d1.state(p, lv, t)

    def cells(p, lv, ts):
        return '、'.join(f'{t}＝{d1.cell(p, lv, t)}' for t in ts)

    def have(p, lv, ts):
        return all(st(p, lv, t) is not None for t in ts)

    S1 = '分離レベルは「どの異常を許すか」の約束'
    three = ['GS', 'P4', 'G2I']
    for lv, exp, text in (('rc', ['anomaly'] * 3, 'Read Committed：リードスキュー・ロストアップデート・ライトスキューを防がない'),
                          ('rr', ['prevented', 'prevented', 'anomaly'], 'Repeatable Read：リードスキューとロストアップデートを防ぎ、ライトスキューを防がない'),
                          ('ser', ['prevented'] * 3, 'Serializable：3 つとも防ぐ')):
        add(S1, f'PostgreSQL {text}（Hermitage の結果の表）',
            [st('pg', lv, t) for t in three] == exp if have('pg', lv, three) else None, cells('pg', lv, three))
    ok = (all(st('pg', lv, 'G1a') == 'prevented' for lv in ('rc', 'rr', 'ser'))
          if all(have('pg', lv, ['G1a']) for lv in ('rc', 'rr', 'ser')) else None)
    add(S1, 'PostgreSQL の 3 水準ともダーティリードは防ぐ', ok, '、'.join(f'{LV[lv]}＝{d1.cell("pg", lv, "G1a")}' for lv in ('rc', 'rr', 'ser')))

    S2 = '同じ名前の分離レベルが製品で別のものを指す'

    def default(p):
        pr = d1.probe.get(p)
        return pr['levels_default']['default'] if pr else None
    for p, want in (('pg', 'read committed'), ('mysql', 'repeatable read'), ('crdb', 'serializable')):
        add(S2, f'{P[p]} の既定は {want}', default(p) == want if default(p) else None, f'サーバーの表示：{default(p)}')
    add(S2, 'PostgreSQL の Serializable は、直列化の異常を検査して失敗を返す',
        (st('pg', 'ser', 'G2I') == 'prevented' and any('40001' in m for m in d1.mechs('pg', 'ser', 'G2I'))) if have('pg', 'ser', ['G2I']) else None,
        cells('pg', 'ser', ['G2I', 'G2', 'G2E']))
    add(S2, 'MySQL 8.4（InnoDB）の REPEATABLE READ はロストアップデートを防がない（題材 D で測る）',
        st('mysql', 'rr', 'P4') == 'anomaly' if have('mysql', 'rr', ['P4']) else None,
        cells('mysql', 'rr', ['P4']) + '。D-2 の件数は下の行', '未確認だった点の実測（一致）' if st('mysql', 'rr', 'P4') == 'anomaly' else None)
    add(S2, 'MySQL の SERIALIZABLE は、通常の SELECT を SELECT ... FOR SHARE に変える',
        (st('mysql', 'ser', 'P4') == 'prevented' and any('wait' in m for m in d1.mechs('mysql', 'ser', 'G1a'))) if have('mysql', 'ser', ['P4', 'G1a']) else None,
        cells('mysql', 'ser', ['G1a', 'P2', 'P4', 'G2I'])
        + ('。G1a は読み取りが未コミットの書き手を待ち、P2 は書き込みが読み手を待った'
           if any('wait' in m for m in d1.mechs('mysql', 'ser', 'G1a')) and any('wait' in m for m in d1.mechs('mysql', 'ser', 'P2')) else ''))
    pr = d1.probe.get('crdb')
    if pr:
        rr_def, rr_en = pr['levels_default'].get('rr'), pr['levels_enabled'].get('rr')
        add(S2, 'CockroachDB：文書（v26.4 表示）は水準を 2 つと書く。Hermitage には repeatable read の行がある（食い違い。理由は未確認）',
            True, f'既定の設定（{pr["settings_default"]}）では、REPEATABLE READ を指定してもサーバーの表示は {rr_def}。'
            f'クラスタ設定を有効にすると表示は {rr_en} になり、{cells("crdb", "rr", ["P4", "GS", "G2I"])}。'
            'Hermitage の手順書は、試験の前にこの種の設定を有効にしている（版は 23.2.0-beta.1 と記載）', '未確認だった点の実測')
    else:
        add(S2, 'CockroachDB の Repeatable Read', None, '')
    add(S2, 'CockroachDB の SERIALIZABLE：競合は 40001',
        any('40001' in m for m in d1.mechs('crdb', 'ser', 'G2I')) if have('crdb', 'ser', ['G2I']) else None, cells('crdb', 'ser', ['P4', 'G2I', 'G2']),
        '未検証だった点の実測（一致）' if any('40001' in m for m in d1.mechs('crdb', 'ser', 'G2I')) else None)

    S3 = 'DB を入れ替えると、既定の正しさの水準が変わる'
    add(S3, 'MySQL → PostgreSQL：同じ名前の Repeatable Read を指定すると、競合した更新は失敗として返る',
        (st('pg', 'rr', 'P4') == 'prevented' and any('40001' in m for m in d1.mechs('pg', 'rr', 'P4')) and st('mysql', 'rr', 'P4') == 'anomaly')
        if have('pg', 'rr', ['P4']) and have('mysql', 'rr', ['P4']) else None,
        f'PostgreSQL：{d1.cell("pg", "rr", "P4")}。MySQL：{d1.cell("mysql", "rr", "P4")}')

    def d2sum(p, lv, key):
        rs = [r for r in d2 if r['product'] == p and r['level'] == lv]
        return (_sumk(rs, key), _sumk(rs, 'served'), len(rs)) if rs else None
    x = d2sum('crdb', 'ser', 'retries')
    add(S3, 'PostgreSQL → CockroachDB：40001 を受けたときの、クライアント側の再試行が要る',
        x[0] > 0 if x else None, f'D-2 の CockroachDB Serializable：再試行 {x[0]} 回（応答済み {x[1]} 件、{x[2]} 試行の合計）' if x else '',
        '未検証だった点の実測（一致）' if x and x[0] > 0 else None)

    S4 = 'MySQL の既定はロストアップデートを防がない'
    x = d2sum('mysql', 'rr', 'lost_updates')
    xp = d2sum('pg', 'rr', 'lost_updates')
    add(S4, '既定の REPEATABLE READ で読んで書き戻すと、更新が黙って消えうる（Jepsen の対象は 8.0.34。8.4 系でも同じかは未確認）',
        x[0] > 0 if x else None,
        (f'MySQL 8.4.13 の D-2：ロストアップデート {x[0]} 件（応答済み {x[1]} 件、{x[2]} 試行の合計。並行度 1 を含む）。エラーは返らなかった。'
         + (f'PostgreSQL の Repeatable Read：{xp[0]} 件' if xp else '')) if x else '',
        '未確認だった点の実測（一致）' if x and x[0] > 0 else None)
    mv = ['GS', 'GSD', 'PMP', 'P3', 'MAVW', 'GSW', 'P3W', 'PMPW2']
    add(S4, '実際の水準の名前は食い違う（Hermitage：monotonic atomic view。Jepsen：それも満たさない）。ロストアップデート以外は題材 D で測る',
        True if have('mysql', 'rr', mv) else None,
        '読み取りだけのトランザクションでは：' + cells('mysql', 'rr', ['GS', 'GSD', 'PMP', 'P3'])
        + '。自分の書き込みを挟むと：' + cells('mysql', 'rr', ['MAVW', 'GSW', 'P3W', 'PMPW2'])
        + '。MAVW は、1 回の SELECT の結果に、別のトランザクションの更新の前の値と後の値が混ざった', '未確認だった点の実測')
    add(S4, 'PostgreSQL の Repeatable Read は、同じ場面でエラーを返して片方を失敗させる',
        any('40001' in m for m in d1.mechs('pg', 'rr', 'P4')) if have('pg', 'rr', ['P4']) else None, cells('pg', 'rr', ['P4', 'MAVW', 'GSW']))

    S5 = 'ロストアップデートの防ぎ方は 5 つ、費用が違う'

    def inv(s, key):
        rs = [r for r in d3 if r['workload'] == 'inv' and r['strategy'] == s]
        return _sumk(rs, key) if rs else None

    def invtxt(s):
        return (f'ロストアップデート {inv(s, "lost_updates")} 件、売り越し {inv(s, "oversold")} 件、正常な要求の失敗 {inv(s, "false_rejects")} 件、'
                f'再試行 {inv(s, "retries")} 回') if inv(s, 'lost_updates') is not None else ''
    for s, claim in (('atomic', 'アトミックな更新'), ('for_update', '明示的なロック（SELECT ... FOR UPDATE）'), ('version', 'バージョン列（条件付き書き込み）')):
        v = inv(s, 'lost_updates')
        add(S5, f'{claim}でロストアップデートを防ぐ', (v == 0 and inv(s, 'oversold') == 0) if v is not None else None, invtxt(s))
    v, w = inv('rr_retry', 'lost_updates'), inv('ser_retry', 'lost_updates')
    add(S5, '分離レベルを上げる：PostgreSQL は Repeatable Read と Serializable で自動検出。失敗（40001）が返るので再試行が要る',
        (v == 0 and w == 0 and inv('rr_retry', 'retries') > 0) if v is not None and w is not None else None,
        f'Repeatable Read＋再試行：{invtxt("rr_retry")}。Serializable＋再試行：{invtxt("ser_retry")}。再試行なしの Serializable：{invtxt("ser_noretry")}')
    v = inv('check_only', 'lost_updates')
    add(S5, '制約（CHECK）：更新が消えること自体は検出しない', v > 0 if v is not None else None,
        f'素朴＋CHECK：{invtxt("check_only")}、在庫が負の品目 {inv("check_only", "negative_items")}。'
        f'制約なしの素朴：{invtxt("naive_rc")}。アトミックな更新＋CHECK：{invtxt("atomic_check")}')

    S6 = 'Serializable の実装は 3 つ、費用の出方が違う'
    add(S6, '2 相ロックの例：MySQL（InnoDB）。読み手と書き手が互いを待たせる。デッドロック',
        (any('wait' in m for m in d1.mechs('mysql', 'ser', 'G2I')) and any('1213' in m for m in d1.mechs('mysql', 'ser', 'G2I')))
        if have('mysql', 'ser', ['G2I']) else None, cells('mysql', 'ser', ['P2', 'P4', 'G2I']))
    add(S6, 'SSI の例：PostgreSQL、CockroachDB。待たせずに進め、危ない並びを見つけたら失敗させる（40001）',
        (any('40001' in m for m in d1.mechs('pg', 'ser', 'G2I')) and any('40001' in m for m in d1.mechs('crdb', 'ser', 'G2I')))
        if have('pg', 'ser', ['G2I']) and have('crdb', 'ser', ['G2I']) else None,
        f'PostgreSQL：{cells("pg", "ser", ["G1a", "G2I"])}。CockroachDB：{cells("crdb", "ser", ["G1a", "G2I"])}'
        + ('。CockroachDB の Serializable では、未コミットの書き込みに当たった読み取り（G1a）に待ちがあった'
           if any('wait' in m for m in d1.mechs('crdb', 'ser', 'G1a')) else ''))

    S7 = '必要な箇所だけ強くし、受け入れる異常は理由を残す'

    def wsv(s, key='violations'):
        rs = [r for r in d3 if r['workload'] == 'ws' and r['strategy'] == s]
        return _sumk(rs, key) if rs else None
    if wsv('ser_retry') is not None:
        add(S7, '配達員の休憩（稼働中が 1 人以上）：Serializable＋再試行、または前提にした行のロックで守る',
            wsv('ser_retry') == 0 and wsv('lock_premise') == 0 and wsv('rc_naive') > 0,
            f'違反したエリア：Read Committed {wsv("rc_naive")}、Repeatable Read {wsv("rr_naive")}、Serializable＋再試行 {wsv("ser_retry")}'
            f'（再試行 {wsv("ser_retry", "retries")} 回）、前提の行のロック {wsv("lock_premise")}（各 15 試行の合計）')
    else:
        add(S7, '配達員の休憩', None, '')
    v = inv('atomic_check', 'lost_updates')
    add(S7, '在庫の引当（在庫は 0 以上）：アトミックな更新＋CHECK 制約で守る',
        (v == 0 and inv('atomic_check', 'oversold') == 0 and inv('atomic_check', 'false_rejects') == 0) if v is not None else None, invtxt('atomic_check'))

    # ---- Part 8
    def cut(sc, names):
        rs = [r for r in d4 if r['mode'] == 'cut' and r['synchronous_commit'] == sc and r['synchronous_standby_names'] == names]
        return (_sumk(rs, 'lost_acked'), _sumk(rs, 'acked_after_cut'), len(rs)) if rs else None

    T1 = 'synchronous_commit = on でも既定は非同期'
    x = cut('on', '')
    add(T1, "synchronous_standby_names が空（既定）なら、on でも手元のフラッシュしか待たない", x[0] > 0 if x else None,
        f"経路を切った後も {x[1]} 件が応答され、昇格後に {x[0]} 件が無かった（{x[2]} 試行の合計）" if x else '')
    xs = {sc: cut(sc, '') for sc in ('local', 'remote_write', 'remote_apply')}
    add(T1, '名前が空のとき、remote_apply・remote_write・local は on と同じ手元の同期になる',
        all(v and v[0] == v[1] and v[0] > 0 for v in xs.values()) if all(xs.values()) else None,
        '、'.join(f'{sc}：切った後の応答 {v[1]} 件、失われた {v[0]} 件' for sc, v in xs.items() if v))
    xs = {sc: cut(sc, 'dic_s1') for sc in ('off', 'local', 'remote_write', 'on', 'remote_apply')}
    if all(xs.values()):
        add(T1, '同期スタンバイがあるとき：remote_write・on・remote_apply はスタンバイを待ち、local と off は待たない（表 19.1 をもとにした表）',
            all(xs[s][0] == 0 for s in ('remote_write', 'on', 'remote_apply')) and all(xs[s][0] > 0 for s in ('off', 'local')),
            '、'.join(f'{sc}：切った後の応答 {v[1]} 件、失われた {v[0]} 件' for sc, v in xs.items()))
    else:
        add(T1, '同期スタンバイがあるときの各値', None, '')
    cr = {sc: [r for r in d4 if r['mode'] == 'crash_restart' and r['synchronous_commit'] == sc] for sc in ('off', 'on')}
    if cr['off'] and cr['on']:
        add(T1, 'off でも DB は壊れないが、直近のコミットを失いうる',
            _sumk(cr['off'], 'lost_acked') > 0 and _sumk(cr['on'], 'lost_acked') == 0,
            f"プライマリを強制終了して同じプライマリを起動し直した。off：応答済み {_sumk(cr['off'], 'acked')} 件のうち {_sumk(cr['off'], 'lost_acked')} 件が無い"
            f"（試行ごと：{'、'.join(str(r['lost_acked']) for r in sorted(cr['off'], key=lambda r: r['trial']))}）。"
            f"on：{_sumk(cr['on'], 'lost_acked')} 件。起動し直した後は読み書きできた（壊れていないことの検査はこれだけ）")
    else:
        add(T1, 'off は直近のコミットを失いうる', None, '')

    T2 = '損失ゼロの設定は、別の止まり方を持ち込む'
    st_ = [r for r in d5 if r['exp'] == 'd5_stall']
    one = [r for r in st_ if r['kind'] != 'any1of2']
    if one:
        sb = [r for r in one if r['kind'] == 'standby_back']
        add(T2, "同期・1 台だけ名指し：s1 が落ちると、コミットが完了しないまま待つ（題材 D で実測する：コミットが返らない時間）",
            all(not r['first_write_returned_in_window'] for r in one),
            f'{len(one)} 試行すべてで、観測の時間（{"・".join(str(w) for w in sorted({r["window_s"] for r in one}))} 秒）の間は返らなかった。'
            + (f'60 秒の観測は {len(sb)} 試行。スタンバイを起動し直すと {"、".join(str(r["after_fix"]["returned_after_fix_s"]) for r in sb)} 秒で返った' if sb else ''),
            '未確認だった点の実測（一致）' if all(not r['first_write_returned_in_window'] for r in one) else None)
        dw = [r['during_wait'] for r in one]
        add(T2, '読み取り専用のトランザクションとロールバックは待たない',
            all(w['read_only_query'].get('returned') and w['rolled_back_txn'].get('returned') for w in dw),
            f'待ちの間に、読み取りは {sum(1 for w in dw if w["read_only_query"].get("returned"))}/{len(dw)} 試行、'
            f'ロールバックは {sum(1 for w in dw if w["rolled_back_txn"].get("returned"))}/{len(dw)} 試行で返った')
        add(T2, '待つ間もトランザクションのロックは持ったまま',
            all(not w['same_key_write_returned_in_3s'] for w in dw),
            f'待っている行と同じキーへの INSERT は {sum(1 for w in dw if not w["same_key_write_returned_in_3s"])}/{len(dw)} 試行で 3 秒の間は返らなかった。'
            f'待っている行がほかのセッションから見えた数：{"、".join(sorted({str(w["row_visible_to_others"].get("value")) for w in dw}))}')
        pr_ = [r for r in one if r['kind'] == 'primary_restart']
        if pr_:
            add(T2, '待っている最中にプライマリが再起動すると、そのトランザクションはコミット済みになる。「応答が無い＝書かれていない」とは言えない',
                all(1001 in (r['after_fix']['rows_1001_1002_on_primary'] or []) and r['after_fix']['first_write_outcome'] != 'ok' for r in pr_),
                f'{len(pr_)} 試行。クライアントに返った結果：{"／".join(sorted({str(r["after_fix"]["first_write_outcome"])[:80] for r in pr_}))}。'
                f'再起動後のプライマリにある行：{"／".join(sorted({str(r["after_fix"]["rows_1001_1002_on_primary"]) for r in pr_}))}')
        cn = [r for r in one if r['kind'] == 'cancel']
        if cn:
            add(T2, '（ガイドに記述なし）待っているコミットをキャンセルするとどうなるか', True,
                f'{len(cn)} 試行。クライアントに返った結果：{"／".join(sorted({str(r["after_fix"]["first_write_outcome"])[:60] for r in cn}))}。'
                f'通知：{"／".join(sorted({n[:120] for r in cn for n in r["after_fix"]["first_write_notices"]})) or "なし"}。'
                f'プライマリにある行：{"／".join(sorted({str(r["after_fix"]["rows_1001_1002_on_primary"]) for r in cn}))}（スタンバイは止まったまま）',
                'ガイドに無い点の実測')
    else:
        add(T2, '同期スタンバイが落ちるとコミットが待つ', None, '')
    an = [r for r in st_ if r['kind'] == 'any1of2']
    if an:
        add(T2, "同期・'ANY 1 (s1, s2)'：残りの 1 台の確認で続く。2 台とも落ちると待つ",
            all(r['one_standby_down']['acked'] == 50 and not r['first_write_returned_in_window'] for r in an),
            f'{len(an)} 試行。1 台を止めた後の書き込み：{"、".join(str(r["one_standby_down"]["acked"]) for r in an)} 件が応答。'
            f'2 台とも止めると {an[0]["window_s"]} 秒の観測の間は返らなかった試行：{sum(1 for r in an if not r["first_write_returned_in_window"])}/{len(an)}')
    else:
        add(T2, "'ANY 1 (s1, s2)'", None, '')
    kl = {n: [r for r in d4 if r['mode'] == 'kill' and r['synchronous_commit'] == 'on' and r['synchronous_standby_names'] == n] for n in ('', 'dic_s1')}
    if kl[''] and kl['dic_s1']:
        add(T2, '非同期（既定）：昇格すると、届いていないコミットを失う（題材 D で実測する：失われた応答済みコミットの件数）',
            _sumk(kl['dic_s1'], 'lost_acked') == 0,
            f"負荷をかけたままプライマリを強制終了して昇格（経路は切らない）。非同期（on、名前は空）：応答済み {_sumk(kl[''], 'acked')} 件のうち "
            f"{_sumk(kl[''], 'lost_acked')} 件を失った（試行ごと：{'、'.join(str(r['lost_acked']) for r in sorted(kl[''], key=lambda r: r['trial']))}）。"
            f"同期（on、名指し）：{_sumk(kl['dic_s1'], 'lost_acked')} 件。"
            + '名前が空のほかの値（手元の同期）：' + '、'.join(
                f"{sc} {_sumk(rs_, 'lost_acked')} 件（失った試行 {sum(1 for r in rs_ if r['lost_acked'] > 0)}/{len(rs_)}）"
                for sc in ('local', 'remote_write', 'remote_apply')
                for rs_ in [[r for r in d4 if r['mode'] == 'kill' and r['synchronous_commit'] == sc and r['synchronous_standby_names'] == '']] if rs_)
            + '。強制終了の瞬間に届いていなかった分だけを失うので、0 件の試行と 1 件以上の試行がある。経路が切れていた場合は上の行', '未確認だった点の実測')
    else:
        add(T2, '非同期で昇格したとき、失われた応答済みコミットの件数', None, '')

    T3 = '複製の遅れは、読み取りに 3 つの異常を起こす'

    def d6g(mode, delay):
        rs = [r for r in d6 if r['mode'] == mode and r['apply_delay_ms'] == delay]
        return rs or None
    a0, a2, r0, r2, s2 = d6g('async', 0), d6g('async', 200), d6g('remote_apply', 0), d6g('remote_apply', 200), d6g('sync_on', 200)
    if a0 and a2:
        add(T3, '自分の書き込みが読めない（題材 D で実測する：書き込みの直後にスタンバイを読み、古い値が返った割合）', True,
            f'非同期：遅れなしで {_sumk(a0, "stale")}/{_sumk(a0, "n")} 件、適用の遅れ 200 ms で {_sumk(a2, "stale")}/{_sumk(a2, "n")} 件が読めなかった。'
            + (f'同期（on）でも適用の遅れ 200 ms では {_sumk(s2, "stale")}/{_sumk(s2, "n")} 件' if s2 else ''), '未確認だった点の実測')
    else:
        add(T3, '書き込みの直後にスタンバイを読み、古い値が返った割合', None, '')
    if r0 and r2 and a2:
        add(T3, 'remote_apply は、同期スタンバイでの適用までコミットが待つ。そのスタンバイでは直後に読める',
            _sumk(r0, 'stale') == 0 and _sumk(r2, 'stale') == 0,
            f'remote_apply：読めなかった {_sumk(r0, "stale")}/{_sumk(r0, "n")} 件（遅れなし）、{_sumk(r2, "stale")}/{_sumk(r2, "n")} 件（適用の遅れ 200 ms）')

        def l5(sc, d):
            rs = [r for r in d5 if r['exp'] == 'd5_latency' and r['synchronous_commit'] == sc
                  and r['synchronous_standby_names'] == 'dic_s1' and r['path_delay_ms_each_way'] == d]
            return med([r['latency']['p50_ms'] for r in rs]) if rs else None
        ra0, on0, ra5, on5 = l5('remote_apply', 0), l5('on', 0), l5('remote_apply', 5), l5('on', 5)
        s2p = med([r['commit_latency']['p50_ms'] for r in s2]) if s2 else None
        ra2p = med([r['commit_latency']['p50_ms'] for r in r2])
        if None not in (ra0, on0, ra5, on5, s2p):
            bigger_idle = ra0 >= 1.5 * on0
            add(T3, 'remote_apply のコミットの遅れは、ほかの値よりずっと大きい（文書の記述）', bigger_idle,
                f'スタンバイの適用が遅れていない条件では差が出なかった：コミットの p50 は remote_apply {ra0} ms・on {on0} ms（経路の遅延 0）、'
                f'remote_apply {ra5} ms・on {on5} ms（片道 5 ms）。適用を 200 ms 遅らせた条件では remote_apply {ra2p} ms・on {s2p} ms で、'
                '適用の遅れの分だけ増えた',
                None if bigger_idle else '**条件による**（適用が遅れない条件では差が出ず、適用が遅れる条件では大きい）')
    else:
        add(T3, 'remote_apply での変化', None, '')

    T4 = 'フェイルオーバーは検知・昇格・遮断を誰が決めるか'

    def d7g(rp, f):
        return [r for r in d7 if r['replication'] == rp and r['fenced'] == f] or None
    au, af, su = d7g('async', False), d7g('async', True), d7g('sync', False)
    if au and af:
        add(T4, '遮断（フェンシング）の取り決めが無いと、スプリットブレイン：両方がリーダーだと思い、データを失う',
            _sumk(au, 'old_acked_after_promotion') > 0 and _sumk(af, 'old_acked_after_promotion') == 0,
            f'遮断なし（非同期）：昇格後も旧プライマリが {_sumk(au, "old_acked_after_promotion")} 件に応答（{len(au)} 試行の合計）。'
            f'同じ ID で中身が違う行 {sum(r["after_old_restart"]["id_collisions"] for r in au)} 件。'
            f'遮断あり：{_sumk(af, "old_acked_after_promotion")} 件。'
            + (f'遮断なしでも同期の設定なら {_sumk(su, "old_acked_after_promotion")} 件（旧プライマリのコミットは応答されずに止まった。'
               f'ただし再起動後の旧プライマリにだけある行が {sum(r["after_old_restart"]["only_old"] for r in su)} 件）' if su else ''),
            '未検証だった点の実測（一致）' if _sumk(au, 'old_acked_after_promotion') > 0 and _sumk(af, 'old_acked_after_promotion') == 0 else None)
    else:
        add(T4, 'スプリットブレイン', None, '')

    L.append(table(['スライドの題', 'ガイドの記述', '判定', '実測'], rows))
    n_diff = sum(1 for r in rows if '食い違い' in r[2])
    n_cond = sum(1 for r in rows if '条件による' in r[2])
    L.append(f'照合した記述：{len(rows)} 個。食い違い：{n_diff} 個。条件によって一致しなかったもの：{n_cond} 個。結果が無く判定できなかったもの：{sum(1 for r in rows if "未検証（結果なし）" in r[2])} 個。\n')
    return '\n'.join(L), rows


def sections(run):
    """The sections whose numbers come from the result files, rendered from one run."""
    d1rows, d2, d3, d4, d5, d6, d7 = (load(n, run) for n in ('d1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7'))
    d1 = D1(d1rows)
    guide_md, _rows = sec_guide(d1, d2, d3, d4, d5, d6, d7)
    return [guide_md, sec_d1(d1), sec_d2(d2), sec_d3(d3), sec_d4(d4), sec_d5(d5), sec_d6(d6), sec_d7(d7),
            sec_ablation(d3, d4, d5, d7)]


def main():
    d1rows, d2, d3, d4, d5, d6, d7 = (load(n) for n in ('d1', 'd2', 'd3', 'd4', 'd5', 'd6', 'd7'))
    audit = load('audit')
    env = {}
    import json
    envp = os.path.join(RESULTS, 'env.json')
    if os.path.exists(envp):
        with open(envp, encoding='utf-8') as f:
            env = json.load(f)
    L = ['# 題材 D：整合性の異常の再現（結果）\n']
    L.append('このファイルは `cs/d/aggregate.py` が `results/d/*.jsonl`（2 回目の実行）と `results/d/run1/*.jsonl`（1 回目の実行）から作る。'
             '表の数は結果のファイルから数えたもので、手では書いていない。'
             '性能の数字は同じ環境の中での相対比較で、ほかの環境には一般化できない。\n')
    L.append('問い：どの設定が、どの異常を許し、何を失うか。同じ名前の設定で、製品ごとに結果が違うか。'
             '構成は **N**（素朴な作り：分離レベルは製品の既定のまま、読んで書き戻す、synchronous_standby_names は空、遮断せずに昇格）、'
             '**B**（共通の基準 B5：応答済みの書き込みを失わない複製の設定。synchronous_commit = on と同期スタンバイの名指し）、'
             '**BE**（基準＋題材 D の重点：E-D1 必要な箇所だけ Serializable にして再試行、E-D2 明示的なロック／バージョン列、'
             'E-D3 昇格の前に旧プライマリを遮断）。D-1〜D-7 は、この要素を 1 つずつ変えた実験で、N・B・BE を通しで流した実行ではない'
             '（要素を外したときの対応は「除去実験」の表）。\n')
    L.append(tworuns.LEGEND.format(t='d'))
    L.append('## 実行の概要\n')
    n_probe = sum(1 for r in d1rows if r['exp'] != 'd1')
    rows = [['D-1 分離レベルごとの異常', 'd1.jsonl',
             f"{sum(1 for r in d1rows if r['exp'] == 'd1')}（ほかに、サーバーが表示した水準を記録した行が {n_probe}。ファイルは {len(d1rows)} 行）"],
            ['D-2 並行の負荷での発生率', 'd2.jsonl', len(d2)], ['D-3 防ぎ方の費用', 'd3.jsonl', len(d3)],
            ['D-4 同期と非同期の複製', 'd4.jsonl', len(d4)],
            ['D-5 損失ゼロの設定の費用', 'd5.jsonl', len(d5)], ['D-6 古い読み取り', 'd6.jsonl', len(d6)],
            ['D-7 昇格の手順', 'd7.jsonl', len(d7)],
            ['判定器の監査', 'audit.jsonl', f'{len(audit)}（合格 {sum(1 for a in audit if a["pass"])}）']]
    L.append(table(['実験', '結果のファイル', '実行の数（行）'], rows))
    L.append('実行の数は 2 回目のもの。1 回目も同じ実行 ID の集合で、監査の合格数も同じだった（`results/RERUN.md`）。\n')
    imgs = env.get('images', {})
    vers = env.get('topic_d_server_versions', {})
    rows = [[img, '、'.join(i.get('repo_digests', [])) if i else '未取得', f'{i.get("size_mb")} MB' if i else '未取得'] for img, i in imgs.items()
            if img in C.IMAGES.values()]
    L.append('### 製品の版とイメージ\n')
    L.append(table(['イメージ（タグ）', 'ダイジェスト', '大きさ'], rows))
    if vers:
        L.append('サーバーが返した版：' + '；'.join(f'{P.get(k, k)}＝{v}' for k, v in vers.items()) + '。\n')
    h = env.get('host', {})
    if h:
        L.append(f'環境：{h.get("cpu_model")}（{h.get("ncpu")} スレッド）、WSL2 のメモリ {h.get("wsl_mem_total_gb")} GB、{h.get("os")}、'
                 f'kernel {h.get("kernel")}、Docker Engine {h.get("docker_server")}。'
                 'D-1〜D-3 のコンテナのメモリ（docker stats で見た値）：'
                 + ('、'.join(f'{k} {v}' for k, v in sorted(env.get('topic_d_container_memory_seen', {}).items())) or '未取得')
                 + '。D-4〜D-7（PostgreSQL 2〜3 台と Toxiproxy）のメモリは記録していない。\n')
    L += tworuns.merge(sections(1), sections(2), COUNTS, 'topic D', PROSE)
    L.append('## 判定器の監査\n')
    g = defaultdict(list)
    for a in audit:
        g[a['judge']].append(a)
    L.append('わざと誤りを仕込んだデータと、誤りの無いデータを判定器に流し、仕込んだとおりに数えるかを確かめた（`cs/d/audit.py`、結果は `audit.jsonl`）。\n')
    L.append(table(['判定器', 'ケース', '合格'], [[k, len(v), sum(1 for a in v if a['pass'])] for k, v in sorted(g.items())]))
    L.append(NOTES)
    with open(OUT, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(L).rstrip() + '\n')
    return OUT


NOTES = """## 測り方の注記と限界

- **手順の出どころ**：D-1 の手順の考え方は Hermitage（https://github.com/ept/hermitage 、Martin Kleppmann、CC BY 4.0）による。文の並びはこのリポジトリで書き直し、P2・P3・P3W・MAVW・PMPW2 を足した。Hermitage と同じく、試験の集まりであって証明ではない。「防いだ」は、この手順では起きなかったという意味である。
- **D-1 の判定**：各セッションが読んだ行と、最後の表の中身で判定した（メッセージの文言は使っていない）。エラーが返ったセッションは、アプリケーションがするようにロールバックし、残りの文を出さない。
- **CockroachDB**：v26.4.0 の `start-single-node` はループバック以外の待ち受けを受け付けなかったので、同じネットワーク名前空間に置いた Toxiproxy（遅延なし）でポートを中継した。単一ノードなので、ノードをまたぐ挙動は測っていない。READ COMMITTED と REPEATABLE READ はクラスタ設定を有効にして測った。
- **D-2・D-3**：並行の競合に頼るので、種・試行の数・件数を組で読む。発生率は、この負荷（少数の品目に集中、クライアントは Python のスレッド、DB は同じ 1 台の上）での値で、ほかの負荷には当てはまらない。再試行の上限は 50 回。
- **D-4**：「経路を切ってから強制終了」で失われる件数は、切った後に応答された件数そのものである（割合ではない）。「切らずに強制終了」は、強制終了の瞬間の遅れに左右される。SIGKILL の合図は応答済み 1000 件だが、合図から停止までの間にも応答が進むので、応答済みの合計は試行ごとに違う。
- **D-5**：「返らなかった」は観測した時間の中での話で、永久に待つことは確かめていない。経路の遅延は Toxiproxy の latency（片道ごと）で、実際のネットワークの揺らぎは再現していない。
- **D-6**：「直後」は、コミットの応答を受けた次の文という意味である。クライアントとスタンバイの往復の時間（1 ms 未満）ぶん、実際の直後より遅い。
- **D-7**：遮断は `docker stop`（旧プライマリの停止）で行った。ネットワークの遮断やストレージの遮断は測っていない。旧プライマリを新プライマリへ戻す手順（pg_rewind など）は測っていない。
- **経路の遮断の方法**：Toxiproxy（プロキシの無効化）。クライアントの経路には触れていない。

## 測れなかったこと（未検証）

- クラウドのマネージド製品（Aurora、Spanner、Aurora DSQL など）の分離レベルと複製。ガイドの該当の表は公式文書によるままである
- Oracle Database、SQL Server の分離レベル（題材 D の対象外）
- リージョンをまたぐ障害、時計のずれに由来する異常（1 台の上では時計が共有される）
- 複数ノードの CockroachDB、MySQL の複製（準同期・グループレプリケーション）
- スタンバイの OS が落ちる場合（remote_write と on の差）。この実験の昇格は、スタンバイのプロセスも OS も落ちていない条件である
- 障害の自動検知と自動の昇格（Patroni など）。昇格は手動（`pg_promote()`）
- Jepsen のような、履歴全体からの異常の検査。ここで測ったのは、決めた手順と、在庫・休憩の 2 つの負荷だけである
"""


if __name__ == '__main__':
    print(main())
