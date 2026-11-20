"""Topic C: how the fields of results/c/*.jsonl are treated when the two runs are compared
(used by cs/common/compare_runs.py)."""
import re
from statistics import median

TITLE = '共通基盤の費用と性能'
FILES = [('c1', 'C-1 行指向と列指向'), ('c2', 'C-2 取り込み時スキーマと読み出し時スキーマ'), ('c3', 'C-3 保持と圧縮'),
         ('c4', 'C-4 冪等な再実行'), ('c5', 'C-5 索引の費用'), ('c6', 'C-6 Collector の送信で失う場面')]
DET_NOTE = ('時間・大きさとして外した欄：問い合わせ・取り込み・挿入の時間、容量とファイルの大きさ（バイト数、大きい列の内訳）、空きメモリ。'
            '並行の競合や時間に頼る数として外した欄：C-1 の、6 秒の間に終わった検索・挿入・集計の回数、'
            'ClickHouse の 1 行ずつの書き込みの直後に読めた行数とパーツの数。C-6 の、受け口が受けた要求の数（再試行の回数で変わる）。'
            'それ以外は決定的な欄として比べた：C-2 の揺れの結果、C-4 の重複・欠落の件数、問い合わせの照合の不一致、'
            '**C-6 の判定器の件数（受け付けた・断った・届いた・届かなかった・重複）と、失った要求の番号**。'
            'C-6 の障害は要求の件数を合図に入れているので、件数は決定的になるはずの欄として扱い、変わったものを全部下に出した。'
            '実際には、あふれ・再試行の時間切れ・強制終了・ブローカーの停止の件数が変わった。合図は件数でも、その後の結果は、'
            'キューが空く速さ、再試行の時刻、強制終了の瞬間に Collector と Kafka が持っていた分に頼るためで、決定的ではない欄だと分かった。')

SIZE = re.compile(r'bytes|^top_columns|(^|\.)size\.|^scan\.|^events')
RACY = {
    'c1': re.compile(r'^mixed\.\d\.(insert|lookup)\.n$|^mixed\.\d\.analytic_queries_done$|^rowwrite\.single_(nowait|sync)_(rows_at_return|parts)$'),
    'c6': re.compile(r'^sink_requests\.'),
}
DESIGN = {'memory': 'メモリのキュー', 'file': 'ディスクのキュー', 'kafka': 'Kafka を挟む（受け側は既定）', 'kafka_after': 'Kafka を挟む（処理の後で印）'}


def audit_ok(r):
    return bool(r['ok'])


def classify(name, row, path):
    if SIZE.search(path):
        return 'time'
    rx = RACY.get(name)
    if rx and rx.search(path):
        return 'racy'
    return None


def group(name, r):
    if name == 'c1':
        return r['product'], f"規模 {r['scale']}"
    if name == 'c4':
        return f"{r['product']}・{r['strategy']}", r['scenario']
    if name == 'c5':
        return r['product'], r['variant']
    if name == 'c6':
        return DESIGN.get(r['design'], r['design']), r['scenario']
    if name == 'c3':
        return r['product'], f"{r['part']}・{r.get('method') or r.get('order')}・{r.get('codec') or ''}".rstrip('・')
    return None


def _j(k):
    return lambda r: r['judge'][k]


OCC = [
    ('c1', '問い合わせの照合の不一致', lambda r: r['mismatch_total']),
    ('c1', '集計と並べた検索・挿入の失敗', lambda r: sum(v['failed'] for v in r['mixed'].values()) if r.get('mixed') else None),
    ('c3', '問い合わせの照合の不一致', lambda r: r.get('mismatch_total')),
    ('c4', '流し直した後の重複', lambda r: r['after_rerun']['duplicates']),
    ('c4', '流し直した後の誤り', lambda r: r['after_rerun']['wrong']),
    ('c5', '問い合わせの照合の不一致', lambda r: r['mismatch_total']),
    ('c6', '受け付けたのに届かなかったログ', _j('lost_acked')),
    ('c6', '重複して届いたログ', _j('duplicates')),
    ('c6', '断ったログ', _j('rejected')),
]


def _q(q, key='queries'):
    return lambda r: median(r[key][q]['ms']) if q in r.get(key, {}) else None


def _mixed_insert(r):
    return r['mixed']['0']['insert']['p50_ms'] if r.get('mixed') else None


RATIO = [
    ('c1', '店舗ごとの集計の時間', _q('agg_store'), None),
    ('c1', 'キーで 1 行の検索の時間', _q('point_pk'), None),
    ('c1', '集計なしのときの挿入の p50（PostgreSQL）', _mixed_insert, lambda r: r['product'] == 'pg'),
    ('c1', '容量', lambda r: r['size']['total_bytes'], None),
    ('c2', '店舗ごとの集計の時間（性能の実行）', _q('agg_store'), lambda r: r['part'] == 'perf'),
    ('c3', '書き込みの時間（圧縮の実行）', lambda r: r.get('write_ms'), lambda r: r['part'] == 'compress'),
    ('c3', '落とす操作の時間の合計', lambda r: r.get('total_ms'), lambda r: r['part'] == 'retain'),
    ('c5', '取り込みの時間', lambda r: r['load_ms'], None),
    ('c5', '挿入の時間', lambda r: r['insert_ms'], None),
    ('c5', '顧客 1 人の件数と金額の時間', _q('filter_sum', 'reads'), None),
    ('c5', '顧客 1 人の行（全列）の時間', _q('filter_rows', 'reads'), None),
    ('c5', 'データの大きさ', lambda r: r['data_bytes'], None),
    ('c6', '正常時に届くまでの遅れの p50', lambda r: (r.get('delay_normal_ms') or {}).get('p50_ms'), lambda r: r['scenario'] == 'normal'),
    ('c6', '受け付けたのに届かなかったログの件数', _j('lost_acked'), None),
]
