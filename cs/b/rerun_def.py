"""Topic B: how the fields of results/b/*.jsonl are treated when the two runs are compared
(used by cs/common/compare_runs.py)."""
import re

TITLE = '置換と更新の演習'
FILES = [('b1', 'B-1 イベントのスキーマ変更'), ('b2', 'B-2 古いデータの読み直し'), ('b3', 'B-3 DB のスキーマ変更'),
         ('b4', 'B-4 導出ストアの入れ替え'), ('b5', 'B-5 作り直しの前提条件'), ('b6', 'B-6 記録のシステムの入れ替え'),
         ('b7', 'B-7 マテリアライズドビュー')]
DET_NOTE = ('B-1・B-2 は、時間のほかは全部を決定的な欄として比べた（登録の可否、受け手ごとのエラー・誤った値・欠けた値の件数）。'
            '並行の競合や時間に頼る数として外した欄：B-3 の、負荷の件数（成功・失敗・誤った読み取り、段階ごとの内訳）、ロック待ちの標本、'
            '別の接続から見えたロック、移行の最終状態の件数（DDL が取ったロック、表の書き換えの有無、DDL のエラー、DDL の試行回数は決定的な欄として比べた）。'
            'B-4 の、応答済みのイベント数、読み取りの件数、照合の中身、最終状態の件数（結果＝切り替えた／照合で止めた、は決定的な欄として比べた）。'
            'B-5 の、ログの先頭・末尾・残ったレコード数とその時系列（保持は時間で効く。作り直しの最終状態、削除の印が消えたか、警報は決定的な欄として比べた）。'
            'B-6 の、負荷の件数、二重書き込みの失敗、埋め戻しの前後の差、最終状態の件数、切り替え前のシーケンスと主キー'
            '（新のシーケンスの値は決定的な欄として比べた）。B-7 の、読み取りの件数とロック待ちの標本（ビューの食い違いの数は決定的な欄として比べた）。')

TIME = re.compile(r'^store_bytes\.|^slow_lag_age_ms$')
RACY = {
    'b3': re.compile(r'^(baseline_read|baseline_write|read|write|read_by_phase|write_by_phase|lock_waits)\.|^locks_sampled|^final\.'),
    'b4': re.compile(r'^events_acked$|^final\.|^reads\.|^reconciliation\.'),
    'b5': re.compile(r'^(log|log_before|log_after)\.|^series'),
    'b6': re.compile(r'^(acked_on_new_only|dual_new_failed|dual_new_missed|subscription_apply_errors)$|^(final|other_side)\.'
                     r'|^(read|write)_\w+\.|^steps\.(diff_|max_id_before_switch|sequence_before_switch\.pg$)'),
    'b7': re.compile(r'^(baseline_read|read|lock_waits)\.|^locks_sampled'),
}


def audit_ok(r):
    return bool(r['ok'])


def classify(name, row, path):
    if TIME.search(path):
        return 'time'
    rx = RACY.get(name)
    if rx and rx.search(path):
        return 'racy'
    return None


def group(name, r):
    if name == 'b3':
        return {'ops': '操作ごと', 'queue': '長いトランザクションの後ろ', 'mig': '列を分ける移行'}[r['exp']], r['case']
    if name == 'b4':
        return r['config'], r['variant']
    if name == 'b5':
        return f"{r['broker']}・{r['case']}", r['variant']
    if name == 'b6':
        return r['config'], r['scenario']
    if name == 'b7':
        return r['product'], r['case']
    return None


def _p(*path):
    def get(r):
        cur = r
        for k in path:
            if not isinstance(cur, dict) or k not in cur or cur[k] is None:
                return None
            cur = cur[k]
        return int(cur) if isinstance(cur, bool) else cur
    return get


def _b4reads(key):
    return lambda r: sum(v.get(key, 0) for v in r['reads']['by_store'].values())


def _b4serving(key):
    return lambda r: r['final'][r['serving']].get(key)


OCC = [
    ('b3', '失敗した書き込み', _p('write', 'failed')), ('b3', '失敗した読み取り', _p('read', 'failed')),
    ('b3', '誤った読み取り', _p('read', 'wrong')), ('b3', '最終状態：値の食い違い', _p('final', 'value_mismatch')),
    ('b3', '最終状態：応答済みの書き込みの欠落', _p('final', 'lost_acked_write')), ('b3', '新列が NULL の行', _p('final', 'null_new_columns')),
    ('b4', '切り替えなかった（照合で止めた）', lambda r: int(r['outcome'] == 'aborted_by_reconciliation')),
    ('b4', '読み取り：見つからない', _b4reads('not_found')), ('b4', '読み取り：前より古い版', _b4reads('regress')),
    ('b4', '使っているストアの状態の食い違い', _b4serving('state_mismatch')), ('b4', '使っているストアの適用回数の食い違い', _b4serving('count_mismatch')),
    ('b4', '新のストアの状態の食い違い', _p('final', 'new', 'state_mismatch')), ('b4', '新のストアの適用回数の食い違い', _p('final', 'new', 'count_mismatch')),
    ('b5', '無い注文', _p('final', 'missing')), ('b5', '状態の食い違い', _p('final', 'state_mismatch')),
    ('b5', '適用回数の食い違い', _p('final', 'count_mismatch')), ('b5', '残った：止めて再開した受け手', _p('final', 'slow', 'deleted_left')),
    ('b5', '残った：止めなかった受け手', _p('final', 'fast', 'deleted_left')), ('b5', '残った：先頭から作り直し', _p('final', 'fresh', 'deleted_left')),
    ('b5', '削除の印がログから消えた', _p('tombstones_gone')), ('b5', '監視の警報', _p('alarm')),
    ('b5', '導出に残った削除済みの行', _p('final', 'deleted_left')),
    ('b6', '失敗した書き込み', _p('write_all', 'failed')), ('b6', '古い・無い読み取り', _p('read_all', 'wrong')),
    ('b6', '読みを切り替えた段階の古い・無い読み取り', _p('read_by_phase', '4_read_new', 'wrong')),
    ('b6', '最終：無い行', _p('final', 'missing')), ('b6', '最終：応答済みの書き込みの欠落', _p('final', 'lost_acked_write')),
    ('b6', '最終：値の食い違い', _p('final', 'value_mismatch')), ('b6', '埋め戻しの後：新が欠ける・古い行', _p('steps', 'diff_after_backfill')),
    ('b6', '購読の適用エラー', _p('subscription_apply_errors')),
    ('b7', '失敗した読み取り', _p('read', 'failed')),
]

RATIO = [
    ('b3', 'DDL の時間（操作ごと）', _p('ddl_ms'), lambda r: r['exp'] == 'ops'),
    ('b3', '書き込みが止まった時間', _p('write', 'max_gap_ms'), None),
    ('b3', '成功した書き込みの件数', _p('write', 'ok'), None),
    ('b3', '移行の所要時間', _p('migration_s'), lambda r: r['exp'] == 'mig'),
    ('b4', '作り直しの時間', _p('rebuild_s'), None),
    ('b4', '読み取り：前より古い版の件数', _b4reads('regress'), None),
    ('b5', '作り直しの時間', _p('rebuild_s'), None),
    ('b5', 'ログの先頭のオフセット（保持を縮めた実行）', _p('log', 'low'), lambda r: r['case'] == 'retention'),
    ('b6', '切り替えで書き込みが止まった時間', _p('write_switch', 'max_gap_ms'), None),
    ('b6', '失敗した書き込みの件数', _p('write_all', 'failed'), None),
    ('b6', '埋め戻しの後：新が欠ける・古い行', _p('steps', 'diff_after_backfill'), None),
    ('b7', 'REFRESH の時間', _p('refresh_ms'), None),
]
