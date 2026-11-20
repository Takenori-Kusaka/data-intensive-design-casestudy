"""Topic A: how the fields of results/a/*.jsonl are treated when the two runs are compared
(used by cs/common/compare_runs.py)."""
import re

TITLE = '基盤をまたぐデータ統合'
FILES = [('main', '構成 × ケース（Redpanda）'), ('wal', 'CDC の落とし穴（WAL）'), ('kafka', 'Apache Kafka での照合')]
# Additional experiments that were run once: no first run, so they are not compared; README.md lists them apart.
ONCE = [('poll', '追加実験 A-P1：CDC の読み取り間隔')]
DET_NOTE = ('並行の競合や時間に頼る数として外した欄：並行度 4・16 の実行（main・kafka）と、CDC の落とし穴（wal。書き手は並行度 4）の、'
            '判定器が数えた件数すべて（metrics、照合が見つけた数、失敗の内訳、効果の回数、落ち着いた理由）。'
            '強制終了や切断は件数を合図に入れるが、その瞬間に処理中だった要求の数は並行の進み方で変わるので、ここに入れた。'
            '並行度 1 の実行は、件数を全部、決定的な欄として比べた。'
            '構成、種、設定、製品の版、コネクタの状態、スロットの wal_status・無効化の理由・無効になったかどうかは、どの実行でも決定的な欄として比べた。'
            'Kafka Connect のログの抜粋は、壁時計の時刻を含むので時間の欄にした。')

CFG = {('n', ''): 'N', ('b', ''): 'B', ('be', ''): 'BE', ('b', 'wm'): 'B の変種（連番より大きい行だけ読む）',
       ('b', 'noidem'): 'B − 冪等な受け手', ('be', 'nokey'): 'BE − キーごとの順序', ('be', 'noidem'): 'BE − 冪等な受け手'}
RACY = re.compile(r'(^|\.)metrics\.|^index_update_count_sum$|^(lag_ms|response_ms)\.n$|^failures_by_reason|(^|\.)reconcile\.'
                  r'|(^|\.)settled(_p1)?\.reason$|(^|\.)missing_by_phase\.|would_detect_by_reconcile|^restarts')
FIXED = re.compile(r'(^|\.)metrics\.(attempted|expected|not_attempted)$')
TIME = re.compile(r'^events|log_excerpt|seconds_observed|seconds_since_stop|(^|\.)settled(_p1)?\.seconds$|\.lsn$')


def audit_ok(r):
    return bool(r['ok'])


def classify(name, row, path):
    if name == 'audit':
        return None
    if TIME.search(path):
        return 'time'
    conc = row['ops']['concurrency'] if name == 'wal' else row['concurrency']
    if conc > 1 and RACY.search(path) and not FIXED.search(path):
        return 'racy'
    return None


def group(name, r):
    if name == 'wal':
        return f"BE・{r['cond']}", 'CDC の落とし穴'
    return (f"{CFG[(r['config'], r['variant'])]}（{'Apache Kafka' if r['broker'] == 'kafka' else 'Redpanda'}）",
            f"{r['case']}・並行度 {r['concurrency']}")


def _m(path):
    def get(r):
        cur = r['metrics']
        for k in path.split('.'):
            cur = cur[k]
        return cur
    return get


def _w(part, path):
    def get(r):
        cur = r.get(part)
        if not cur:
            return None
        cur = cur['metrics']
        for k in path.split('.'):
            cur = cur[k]
        return cur
    return get


_COUNTS = [('失敗した要求', 'request_failures'), ('正にあって索引に届かない', 'index.missing_committed'),
           ('正にあって通知に届かない', 'notify.missing_committed'), ('応答済みなのに索引に効果なし', 'index.missing_acked'),
           ('応答済みなのにブローカーに無い', 'log.missing_acked'), ('ブローカーのログ内の重複', 'log.dup_extra'),
           ('届いた重複（索引）', 'index.dup_delivered_extra'), ('届いた重複（通知）', 'notify.dup_delivered_extra'),
           ('状態に 2 回効いた（索引）', 'index.dup_effect_extra'), ('状態に 2 回効いた（通知）', 'notify.dup_effect_extra'),
           ('順序の入れ替わり', 'index.reorder_pairs'), ('古いイベントの再適用', 'index.regress_pairs'),
           ('最終状態の食い違い', 'final.mismatch')]
OCC = ([(f, ja, _m(p)) for f in ('main', 'kafka') for ja, p in _COUNTS]
       + [(f, '照合が見つけた数', lambda r: r['reconcile']['detected']) for f in ('main', 'kafka')]
       + [('wal', 'Debezium を戻した後：正にあって索引に届かない', _w('after_restart', 'index.missing_committed')),
          ('wal', 'Debezium を戻した後：最終状態の食い違い', _w('after_restart', 'final.mismatch')),
          ('wal', '復旧の後：正にあって索引に届かない', _w('recovery', 'index.missing_committed')),
          ('wal', '復旧の後：古いイベントの再適用', _w('recovery', 'index.regress_pairs')),
          ('wal', '復旧の後：最終状態の食い違い', _w('recovery', 'final.mismatch'))])

_conc = lambda r: r['concurrency'] > 1
RATIO = [
    ('main', '遅れの p50', lambda r: r['lag_ms']['p50_ms'], None),
    ('main', '応答時間の p50', lambda r: r['response_ms']['p50_ms'], None),
    ('main', '届いた重複（索引）の回数', _m('index.dup_delivered_extra'), _conc),
    ('main', '順序の入れ替わりの組', _m('index.reorder_pairs'), _conc),
    ('main', '最終状態の食い違いの注文', _m('final.mismatch'), _conc),
    ('kafka', '遅れの p50', lambda r: r['lag_ms']['p50_ms'], None),
    ('kafka', '届いた重複（索引）の回数', _m('index.dup_delivered_extra'), _conc),
    ('wal', 'スロットが保持した WAL の最大（MB）', lambda r: max((s.get('retained_by_slot_mb') or 0) for s in r['samples']), None),
    ('wal', '復旧の後の最終状態の食い違い', _w('recovery', 'final.mismatch'), None),
]
