"""Topic D: how the fields of results/d/*.jsonl are treated when the two runs are compared
(used by cs/common/compare_runs.py)."""
import re

TITLE = '整合性の異常の再現'
FILES = [('d1', 'D-1 分離レベルごとの異常'), ('d2', 'D-2 並行の負荷での発生率'), ('d3', 'D-3 防ぎ方の費用'),
         ('d4', 'D-4 同期と非同期の複製'), ('d5', 'D-5 損失ゼロの設定の費用'), ('d6', 'D-6 古い読み取り'),
         ('d7', 'D-7 昇格の手順')]
DET_NOTE = ('並行の競合や時間に頼る数として外した欄：D-1 の、文が返った順と待ちの印とエラーの文言（steps の seq・queued・msg。'
            '判定・読んだ行・最後の表の中身・防ぎ方は決定的な欄として比べた）。D-2・D-3 の並行度 4・16 の実行の、件数の欄すべて'
            '（並行度 1 の実行は全部の件数を決定的な欄として比べた）。D-4 の「経路を切らずに強制終了」「昇格しない」の、'
            '応答済み・残った行・失われた件数・応答なしで残った行（強制終了の瞬間に左右される。「経路を切ってから強制終了」は全部を比べた）。'
            'D-5・D-6・D-7 は、時間のほかは全部を決定的な欄として比べた。')

P = {'pg': 'PostgreSQL', 'mysql': 'MySQL', 'crdb': 'CockroachDB'}
CONFIG = re.compile(r'^(run_id|exp|product|level|concurrency|trial|seed|n_ops|ops|k_items|areas|expected_served|strategy|workload|version)$')
TIME = re.compile(r'^steps\[\d+\]\.ms$|^after_fix\.returned_after_fix_s$|^during_wait\.\w+\.ms$|^one_standby_down\.(max|p50|p95)_ms$'
                  r'|^first_write_waited_s$|^stall_observed_s$')


def audit_ok(r):
    return bool(r['pass'])


def classify(name, row, path):
    if TIME.search(path):
        return 'time'
    if name == 'd1' and re.search(r'^steps\[\d+\]\.(seq|queued|msg)$', path):
        return 'racy'
    if name in ('d2', 'd3') and row.get('concurrency', 1) > 1 and not CONFIG.search(path):
        return 'time' if path.startswith(('latency.', 'duration_s', 'wall_s')) else 'racy'
    if name == 'd4' and row.get('mode') in ('kill', 'crash_restart') and re.search(
            r'^(acked|present|lost_acked|unacked_present)$|^events', path):
        return 'racy'
    if name == 'd4' and path.startswith('events'):
        return 'time'
    if path.startswith('events'):
        return 'time'
    return None


def _names(r):
    return f"synchronous_standby_names='{r['synchronous_standby_names']}'、synchronous_commit={r['synchronous_commit']}"


def group(name, r):
    if name == 'd2':
        return f"{P[r['product']]} {r['level']}", f"並行度 {r['concurrency']}"
    if name == 'd3':
        return f"{r['workload']}・{r['strategy']}", f"並行度 {r['concurrency']}"
    if name == 'd4':
        return _names(r), {'cut': '経路を切ってから強制終了', 'kill': '経路を切らずに強制終了', 'crash_restart': '昇格しない'}[r['mode']]
    if name == 'd6':
        return r['mode'], f"適用の遅れ {r['apply_delay_ms']} ms"
    if name == 'd7':
        return r['replication'], '遮断あり' if r['fenced'] else '遮断なし'
    return None


OCC = [
    ('d2', 'ロストアップデート', lambda r: r['lost_updates']),
    ('d2', '失敗した要求', lambda r: r['failed']),
    ('d3', 'ロストアップデート', lambda r: r.get('lost_updates')),
    ('d3', '売り越し', lambda r: r.get('oversold')),
    ('d3', '正常な要求の失敗（在庫）', lambda r: r.get('false_rejects')),
    ('d3', '違反したエリア', lambda r: r.get('violations')),
    ('d3', '正常な要求の失敗（休憩）', lambda r: r.get('ws_false_rejects')),
    ('d3', 'エラーで終わった要求', lambda r: r.get('failed')),
    ('d4', '失われた応答済み', lambda r: r['lost_acked']),
    ('d4', '応答なしで残った行', lambda r: r['unacked_present']),
    ('d6', '読めなかった', lambda r: r['stale']),
    ('d7', '昇格後に旧プライマリが応答した書き込み', lambda r: r['old_acked_after_promotion']),
    ('d7', '同じ ID で中身が違う行', lambda r: r['after_old_restart']['id_collisions']),
]

_conc = lambda r: r.get('concurrency', 1) > 1
RATIO = [
    ('d2', 'ロストアップデートの件数（並行度 4・16）', lambda r: r['lost_updates'], _conc),
    ('d2', '再試行の回数（並行度 4・16）', lambda r: r['retries'], _conc),
    ('d2', '応答時間の p50', lambda r: r['latency']['p50_ms'], None),
    ('d3', '再試行の回数（並行度 4・16）', lambda r: r['retries'], _conc),
    ('d3', '応答時間の p50', lambda r: r['latency']['p50_ms'], None),
    ('d4', '応答済みの件数（経路を切らずに強制終了・昇格しない）', lambda r: r['acked'], lambda r: r['mode'] != 'cut'),
    ('d4', '失われた応答済みの件数（同上）', lambda r: r['lost_acked'], lambda r: r['mode'] != 'cut'),
    ('d5', 'コミットの p50', lambda r: (r.get('latency') or {}).get('p50_ms'), lambda r: r['exp'] == 'd5_latency'),
    ('d6', 'コミットの p50', lambda r: r['commit_latency']['p50_ms'], None),
    ('d6', '応答から読めるまでの p50', lambda r: r['visible_after_ack']['p50_ms'], None),
]
