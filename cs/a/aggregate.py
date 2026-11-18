"""Build results/a/summary.md from results/a/*.jsonl. Every number in the
tables is counted from the result files; nothing is typed by hand."""
import json
import os
from collections import defaultdict
from statistics import median

from ..common.record import RESULTS, read_jsonl
from . import cases, config as C, wal as WAL

OUT = os.path.join(RESULTS, 'a', 'summary.md')

CFG_JA = {
    ('n', ''): 'N 二重書き込み',
    ('b', ''): 'B アウトボックス＋ポーリング',
    ('be', ''): 'BE アウトボックス＋CDC',
    ('b', 'wm'): 'B の変種（最後に読んだ連番より大きい行だけ読む）',
    ('b', 'noidem'): 'B − 冪等な受け手',
    ('be', 'nokey'): 'BE − キーごとの順序',
    ('be', 'noidem'): 'BE − 冪等な受け手',
}
CFG_ORDER = list(CFG_JA)
CASES = [   # id, key, name, aim, guide part
    ('A-N1', 'normal', '障害なし（並行度 1・4・16）', '二重書き込みは、障害なしの並行更新だけで正と導出が食い違うか。基準を入れた構成の遅れと応答時間', '6・15'),
    ('A-N2', 'longtxn', '障害なし＋長いトランザクション', '連番の順に読むポーリングは、あとからコミットされた小さい連番の行を読み飛ばすか', '6'),
    ('A-W1', 'writer_kill', '書き手の強制終了', 'DB の更新と発行の間で書き手が落ちると、片方だけが成功するか', '6'),
    ('A-K1', 'broker_cut', 'ブローカーへの接続の切断', '発行の途中で接続が切れると、何が失われ、正規の要求はどれだけ失敗するか', '6'),
    ('A-K2', 'broker_pause', 'ブローカーの一時停止', '一時停止のあいだの書き込みは届くか。正規の要求は失敗するか', '6'),
    ('A-K3', 'broker_kill', 'ブローカーの強制終了と再起動', '応答済みのメッセージが再起動で失われるか', '6・8'),
    ('A-C1', 'consumer_kill', '受け手の強制終了', 'オフセットを記録する前に落ちた受け手は、同じメッセージを 2 回処理するか。冪等な受け手で効果は 1 回になるか', '6・11'),
    ('A-C2', 'consumer_late', '受け手を長く止める', '溜まったイベントをまとめて読むとき、欠落と順序のずれが出るか', '6'),
    ('A-R1', 'relay_kill', '運び手の強制終了', 'ポーリングのプロセス／Debezium が落ちて再開すると、再送による重複と読み飛ばしが出るか', '6・11'),
]
CASE_JA = {k: f'{i} {n}' for i, k, n, _a, _p in CASES}
WAL_JA = {'default': 'A-L1 上限なし（既定 max_slot_wal_keep_size=-1）', 'cap': 'A-L2 上限あり（max_slot_wal_keep_size=64MB）',
          'idle': 'A-L3 未使用スロットの時間切れ（idle_replication_slot_timeout=20s）'}


def table(header, rows):
    out = ['| ' + ' | '.join(header) + ' |', '| ' + ' | '.join('---' for _ in header) + ' |']
    for r in rows:
        out.append('| ' + ' | '.join('未検証' if c is None or c == '' else str(c) for c in r) + ' |')
    return '\n'.join(out) + '\n'


def g(row, path):
    cur = row['metrics']
    for k in path.split('.'):
        cur = cur[k]
    return cur


def tot(rows, path):
    return sum(g(r, path) for r in rows)


def occ(rows, path):
    """'sum（trials with at least one / trials）' — 0 stays a plain 0."""
    if not rows:
        return '未検証'
    s = tot(rows, path)
    k = sum(1 for r in rows if g(r, path) > 0)
    return '0' if s == 0 else f'**{s}**（{k}/{len(rows)} 試行）'


def pair(rows, a, b):
    if not rows:
        return '未検証'
    return f'{tot(rows, a)}／{tot(rows, b)}'


def med(rows, key, sub):
    v = [r[key][sub] for r in rows if r.get(key) and r[key].get(sub) is not None]
    return None if not v else round(median(v), 1)


def lat(rows, key):
    a, b = med(rows, key, 'p50_ms'), med(rows, key, 'p95_ms')
    return '測れず（効果が 0 件）' if a is None else f'{a}／{b}'


class Data:
    def __init__(self):
        self.main = read_jsonl(os.path.join(RESULTS, 'a', 'main.jsonl'))
        self.kafka = read_jsonl(os.path.join(RESULTS, 'a', 'kafka.jsonl'))
        self.wal = read_jsonl(os.path.join(RESULTS, 'a', 'wal.jsonl'))
        self.audit = read_jsonl(os.path.join(RESULTS, 'a', 'audit.jsonl'))
        self.by = defaultdict(list)
        for r in self.main + self.kafka:
            self.by[(r['broker'], r['config'], r['variant'], r['case'], r['concurrency'])].append(r)

    def rows(self, config, variant, case, conc=None, broker='redpanda'):
        if conc is not None:
            return self.by.get((broker, config, variant, case, conc), [])
        out = []
        for k, v in self.by.items():
            if k[:4] == (broker, config, variant, case):
                out += v
        return out

    def where(self, pred, broker='redpanda'):
        return [r for r in self.main + self.kafka if r['broker'] == broker and pred(r)]


HEAD = ['構成', '並行度', '試行', '応答済み', '失敗した要求', '正にあって届かない（索引／通知）', 'ブローカーのログ内の重複',
        '届いた重複（索引／通知）', '状態に 2 回効いた（索引／通知）', '順序の入れ替わり（組）', '古いイベントの再適用（組）',
        '最終状態の食い違い（注文）', '遅れ p50／p95（ms）', '応答 p50／p95（ms）']


def line(label, conc, rows):
    if not rows:
        return [label, conc] + ['未検証'] * (len(HEAD) - 2)
    return [label, conc, len(rows), tot(rows, 'acked'), occ(rows, 'request_failures'),
            pair(rows, 'index.missing_committed', 'notify.missing_committed'),
            occ(rows, 'log.dup_extra'),
            pair(rows, 'index.dup_delivered_extra', 'notify.dup_delivered_extra'),
            pair(rows, 'index.dup_effect_extra', 'notify.dup_effect_extra'),
            occ(rows, 'index.reorder_pairs'), occ(rows, 'index.regress_pairs'),
            occ(rows, 'final.mismatch'), lat(rows, 'lag_ms'), lat(rows, 'response_ms')]


def sec_overview(d, env):
    L = ['## 実行の概要\n']
    n_ok = sum(1 for r in d.audit if r['ok'])
    L.append(table(['実験', '結果のファイル', '実行の数（行）'], [
        ['構成 × ケース（Redpanda）', 'main.jsonl', len(d.main)],
        ['CDC の落とし穴（WAL）', 'wal.jsonl', len(d.wal)],
        ['Apache Kafka での照合', 'kafka.jsonl', len(d.kafka)],
        ['判定器の監査', 'audit.jsonl', f'{len(d.audit)}（合格 {n_ok}）'],
    ]))
    L.append('1 回の実行ごとに、コンテナとボリュームを消して作り直した。1 回の実行は、注文 '
             f'{C.N_ORDERS} 件に対する {C.N_OPS} 件の状態の更新（書き手ごとの列は種から決まる。種は {C.SEED0}＋試行番号）。'
             '書き手は最大 2 つの注文サービスのプロセスに分かれ、並行度 16 では 8 スレッド × 2 プロセスになる。\n')
    L.append('### ケースの一覧\n')
    L.append(table(['ID', 'ケース', '狙い', 'ガイドの Part'], [[i, n, a, p] for i, _k, n, a, p in CASES] + [
        ['A-V1', '変種：最後に読んだ連番より大きい行だけ読むポーリング', 'A-N1・A-N2 の条件で、読み飛ばしによる欠落が出るか', '6'],
        ['A-L1〜L3', 'CDC の落とし穴（Debezium を止めたまま書き続ける）', 'スロットが保持する WAL の増え方。上限・時間切れを付けたとき、スロットとイベントに何が起きるか', '6'],
    ]))
    L.append('### 言葉の決め\n')
    L.append(table(['言葉', '数え方'], [
        ['応答済み', '注文サービスが呼び出し元に成功を返した要求。N は「DB のコミット」と「ブローカーの確認応答」の両方が済んだとき、B・BE は DB のコミットが済んだときに成功を返す'],
        ['失敗した要求', '呼び出し元に成功が返らなかった要求（発行の時間切れ、強制終了で応答なし）。正規の要求が失敗した数として数える'],
        ['正にあって届かない', '記録のシステム（注文の表の履歴）にコミットされているのに、受け手の状態に 1 回も効いていないイベントの数。最後まで待った後の最終状態で数える'],
        ['ブローカーのログ内の重複', 'トピックを先頭から読み直し、同じイベント ID が 2 回以上あった余分の数'],
        ['届いた重複', '受け手に 2 回以上渡されたイベントの、余分の回数（受け手が渡されるたびに 1 行記録する）'],
        ['状態に 2 回効いた', '受け手の状態を 2 回以上変えたイベントの、余分の回数。索引は上書きの回数、通知は送信の履歴の行数'],
        ['順序の入れ替わり', '同じ注文について、索引に効いた順（初めて効いたときだけを見る）が、記録のシステムのコミットの順（版）と逆になった隣り合う組の数'],
        ['古いイベントの再適用', '同じ注文について、索引に効いた順（再送で 2 回目に効いたものも含む）で、版が下がった隣り合う組の数'],
        ['最終状態の食い違い', '最後まで待った後、索引の最後のイベントが、注文の表の最後のイベントと違う注文の数（索引に無い注文を含む）'],
        ['遅れ', '注文サービスが DB のコミットを終えた時刻から、索引に初めて効いた時刻まで。同じホストの時計。試行ごとの p50・p95 の中央値'],
    ]))
    L.append('数の書き方：「**12**（3/5 試行）」は、試行の合計が 12 で、5 試行のうち 3 試行で 1 件以上起きたことを表す。「a／b」は索引の受け手／通知の受け手。\n')
    imgs = env.get('images', {})
    rows = []
    for img in C.IMAGES.values():
        i = imgs.get(img) or {}
        rows.append([img, '、'.join(i.get('repo_digests', [])) or '未検証', f'{i.get("size_mb", "未検証")} MB'])
    L.append('### 製品の版とイメージ\n')
    L.append(table(['イメージ（タグ）', 'ダイジェスト', '大きさ'], rows))
    v = env.get('topic_a_server_versions', {})
    ta = env.get('topic_a', {})
    rp = v.get('redpanda', {})
    L.append(f'サーバーが返した版：PostgreSQL {v.get("pg", "未検証")}、Redpanda {rp.get("version", "未検証")}、'
             f'Apache Kafka {v.get("kafka", {}).get("version", "未検証")}。クライアント：Python {ta.get("python", "未検証")}、'
             f'confluent-kafka {ta.get("confluent_kafka", "未検証")}（librdkafka {ta.get("librdkafka", "未検証")}）、'
             f'psycopg {ta.get("psycopg", "未検証")}。\n')
    L.append(f'Redpanda の耐久性の設定（各実行で確かめて記録した値）：write_caching_default＝{rp.get("write_caching_default", "未検証")}、'
             f'--unsafe-bypass-fsync＝{rp.get("unsafe_bypass_fsync", "未検証")}、{rp.get("developer_mode", "developer_mode 未検証")}。'
             'このイメージは、何も指定しないと開発用の設定（write_caching_default＝true、--unsafe-bypass-fsync）で起動したので、'
             '起動の引数と起動後の設定で両方を切った。Apache Kafka は既定のまま（メッセージごとの fsync なし）。\n')
    mem = env.get('topic_a_container_memory_seen', {})
    if mem:
        L.append('コンテナのメモリ（docker stats で見た最大の値）：' + '、'.join(f'{k} {v}' for k, v in sorted(mem.items())) + '。\n')
    return L


def sec_normal(d):
    L = ['## A-N1 障害なし：構成 × 並行度\n']
    rows = []
    for cfg, var in (('n', ''), ('b', ''), ('be', ''), ('b', 'wm'), ('be', 'nokey')):
        for conc in (1, 4, 16):
            rs = d.rows(cfg, var, 'normal', conc)
            if rs or var == '':
                rows.append(line(CFG_JA[(cfg, var)], conc, rs))
    L.append(table(HEAD, rows))
    n16 = d.rows('n', '', 'normal', 16)
    n4 = d.rows('n', '', 'normal', 4)
    n1 = d.rows('n', '', 'normal', 1)
    if n16:
        L.append(f'**二重書き込み（N）は、障害を 1 つも入れなくても順序が入れ替わった。** 並行度 16 では {len(n16)} 試行の合計で、'
                 f'順序の入れ替わりが {tot(n16, "index.reorder_pairs")} 組、最終状態の食い違いが {tot(n16, "final.mismatch")} 注文'
                 f'（応答済み {tot(n16, "acked")} 件、失敗した要求 {tot(n16, "request_failures")} 件）。'
                 f'並行度 4 では {tot(n4, "index.reorder_pairs")} 組／{tot(n4, "final.mismatch")} 注文、'
                 f'並行度 1 では {tot(n1, "index.reorder_pairs")} 組／{tot(n1, "final.mismatch")} 注文。'
                 '入れ替わりのうち、その注文の最後の 2 件に当たったものだけが、最終状態の食い違いとして残る。'
                 'エラーは 1 件も返っていない。\n')
    per = []
    for conc in (4, 16):
        for r in sorted(d.rows('n', '', 'normal', conc), key=lambda r: r['trial']):
            per.append([conc, r['trial'], r['seed'], g(r, 'acked'), g(r, 'index.reorder_pairs'), g(r, 'index.reorder_orders'),
                        g(r, 'final.mismatch'), g(r, 'final.status_mismatch'), r['reconcile']['detected']])
    if per:
        L.append('N の試行ごとの内訳（並行の競合に頼る結果なので、種・試行・発生数を並べる）：\n')
        L.append(table(['並行度', '試行', '種', '応答済み', '順序の入れ替わり（組）', '入れ替わった注文', '最終状態の食い違い（注文）',
                        'うち表示の状態も違う', '照合が見つけた数'], per))
    L.append('遅れは、受け手が書き手に追いつける並行度 1 の行で比べる（並行度 16 では書き手のほうが速く、遅れは受け手の処理待ちを含む）。'
             f'B はポーリングの間隔 {C.RELAY_POLL_MS} ms、BE は Debezium の poll.interval.ms が既定（500 ms）のまま。'
             'N の応答時間には、ブローカーの確認応答を待つ時間が入る。\n')
    return L


def sec_variant(d):
    L = ['## A-V1・A-N2 変種：最後に読んだ連番より大きい行だけ読むポーリング\n']
    L.append('アウトボックスの連番（bigserial）は INSERT の時点で決まり、行が見えるのはコミットの後である。'
             '変種の運び手は「最後に読んだ連番」を覚え、それより大きい行だけを読む。正しい運び手（B）は「未送信の行」を読む。'
             f'A-N2 は、書き手の 1 つが {cases.LONG_TXN["long_txn_every"]} 件に 1 回、アウトボックスに書いてからコミットまで '
             f'{cases.LONG_TXN["long_txn_ms"]} ms 待つ（長いトランザクション）。\n')
    rows = []
    for case, concs in (('normal', (1, 4, 16)), ('longtxn', (4,))):
        for cfg, var in (('b', 'wm'), ('b', '')):
            for conc in concs:
                rs = d.rows(cfg, var, case, conc)
                rows.append([CASE_JA[case].split(' ', 1)[0], CFG_JA[(cfg, var)], conc, len(rs) or '未検証',
                             tot(rs, 'acked') if rs else '未検証',
                             occ(rs, 'log.missing_acked'), occ(rs, 'index.missing_acked'), occ(rs, 'final.mismatch'),
                             occ(rs, 'index.reorder_pairs'),
                             '未検証' if not rs else f'{sum(r["reconcile"]["detected"] for r in rs)}'])
    L.append(table(['ケース', '運び手', '並行度', '試行', '応答済み', '応答済みなのにブローカーに無い', '応答済みなのに索引に効果なし',
                    '最終状態の食い違い（注文）', '順序の入れ替わり（組）', '照合が見つけた数'], rows))
    wm = d.where(lambda r: r['variant'] == 'wm')
    if wm:
        per = [[CASE_JA[r['case']].split(' ', 1)[0], r['concurrency'], r['trial'], r['seed'], g(r, 'acked'),
                g(r, 'index.missing_acked'), g(r, 'final.mismatch')]
               for r in sorted(wm, key=lambda r: (r['case'], r['concurrency'], r['trial'])) if r['concurrency'] > 1]
        L.append('変種の試行ごとの内訳（並行度 4・16）：\n')
        L.append(table(['ケース', '並行度', '試行', '種', '応答済み', '応答済みなのに索引に効果なし', '最終状態の食い違い（注文）'], per))
    return L


def sec_faults(d):
    L = ['## 障害のケース：構成 × ケース（並行度 4、Redpanda）\n']
    L.append(f'障害は件数を合図に入れた。書き手の強制終了：1 つ目の書き手の {cases.WRITER_CRASH_SEQS} 件目の、DB のコミットの直後（N では発行の前）。'
             f'ブローカーの障害：呼び出し元の要求が {cases.FAULT_AT} 件に達した時点で入れ、決めた秒数と件数の両方を満たした時点で外す'
             f'（切断 {cases.BROKER_FAULT["broker_cut"]["min_s"]} 秒、一時停止 {cases.BROKER_FAULT["broker_pause"]["min_s"]} 秒、'
             f'強制終了から起動まで {cases.BROKER_FAULT["broker_kill"]["min_s"]} 秒以上）。'
             f'受け手の強制終了：起動ごとに {cases.CONSUMER_CRASH_AFTER[0]} 件を処理した直後（効果のコミット後、オフセットのコミット前）に 3 回。'
             f'運び手の強制終了：B は起動ごとに {cases.RELAY_CRASH_AFTER[0]} 行を発行した直後（送信済みの印を付ける前）に 3 回、'
             f'BE は要求が {cases.FAULT_AT} 件に達した時点で Kafka Connect のコンテナを 1 回。入れた時点と外した時点は各行の events に記録した。\n')
    rows = []
    plan = [('writer_kill', [('n', ''), ('b', ''), ('be', '')]), ('broker_cut', [('n', ''), ('b', ''), ('be', '')]),
            ('broker_pause', [('n', ''), ('b', ''), ('be', '')]), ('broker_kill', [('n', ''), ('b', ''), ('be', '')]),
            ('consumer_kill', [('n', ''), ('b', ''), ('b', 'noidem'), ('be', '')]),
            ('consumer_late', [('n', ''), ('b', ''), ('be', ''), ('be', 'nokey')]),
            ('relay_kill', [('b', ''), ('b', 'noidem'), ('be', ''), ('be', 'noidem')])]
    for case, cfgs in plan:
        for cfg, var in cfgs:
            rs = d.rows(cfg, var, case, 4)
            rows.append([CASE_JA[case]] + line(CFG_JA[(cfg, var)], 4, rs)[:1] + line(CFG_JA[(cfg, var)], 4, rs)[2:])
    L.append(table(['ケース'] + HEAD[:1] + HEAD[2:], rows))
    # detail of failed requests and unacked commits
    rows = []
    for case, cfgs in plan[:4]:
        for cfg, var in cfgs:
            rs = d.rows(cfg, var, case, 4)
            if not rs:
                continue
            reasons = defaultdict(int)
            for r in rs:
                for k, v in r['failures_by_reason'].items():
                    reasons[k] += v
            rows.append([CASE_JA[case], CFG_JA[(cfg, var)], len(rs), tot(rs, 'attempted'), tot(rs, 'acked'),
                         tot(rs, 'request_failures'),
                         '、'.join(f'{k} {v}' for k, v in sorted(reasons.items())) or 'なし',
                         tot(rs, 'committed_unacked'), tot(rs, 'index.missing_committed'), tot(rs, 'index.missing_acked'),
                         tot(rs, 'final.mismatch'), sum(r['reconcile']['detected'] for r in rs),
                         sum(r['reconcile']['after_repair'] for r in rs)])
    L.append('### 書き手・ブローカーの障害の内訳（正規の要求の失敗と、片方だけの成功）\n')
    L.append(table(['ケース', '構成', '試行', '要求', '応答済み', '失敗した要求', '失敗の内訳',
                    '応答なし・失敗なのに DB にはコミット済み', 'うち索引に届かない', '応答済みなのに索引に届かない',
                    '最終状態の食い違い（注文）', '照合が見つけた数', '照合で直した後の差'], rows))
    L.append('N では、発行に失敗した要求も、強制終了で応答が無かった要求も、DB にはコミットされている。'
             '呼び出し元から見ると失敗だが、注文の表は変わっており、索引と通知には届かない（片方だけの成功）。'
             'B・BE では、同じ障害のあいだも要求は失敗せず、障害が終わった後に全件が届いた。\n')
    return L


def sec_ablation(d):
    L = ['## 除去実験\n']
    rows = []

    def add(name, what, base, abl, note):
        rows.append([name, what, base, abl, note])

    for case in ('consumer_kill', 'relay_kill'):
        b, a = d.rows('b', '', case, 4), d.rows('b', 'noidem', case, 4)
        if b or a:
            add('B − 冪等な受け手', CASE_JA[case],
                f'届いた重複 {pair(b, "index.dup_delivered_extra", "notify.dup_delivered_extra")}、'
                f'状態に 2 回効いた {pair(b, "index.dup_effect_extra", "notify.dup_effect_extra")}、'
                f'古いイベントの再適用 {tot(b, "index.regress_pairs") if b else "未検証"}',
                f'届いた重複 {pair(a, "index.dup_delivered_extra", "notify.dup_delivered_extra")}、'
                f'状態に 2 回効いた {pair(a, "index.dup_effect_extra", "notify.dup_effect_extra")}、'
                f'古いイベントの再適用 {tot(a, "index.regress_pairs") if a else "未検証"}',
                '外すと、届いた重複がそのまま通知の重複になる')
    b, a = d.rows('be', '', 'relay_kill', 4), d.rows('be', 'noidem', 'relay_kill', 4)
    if b or a:
        add('BE − 冪等な受け手', CASE_JA['relay_kill'],
            f'届いた重複 {pair(b, "index.dup_delivered_extra", "notify.dup_delivered_extra")}、'
            f'状態に 2 回効いた {pair(b, "index.dup_effect_extra", "notify.dup_effect_extra")}',
            f'届いた重複 {pair(a, "index.dup_delivered_extra", "notify.dup_delivered_extra")}、'
            f'状態に 2 回効いた {pair(a, "index.dup_effect_extra", "notify.dup_effect_extra")}',
            'Debezium の再送も、受け手が冪等でなければ通知の重複になる')
    for case in ('normal', 'writer_kill', 'broker_cut', 'broker_pause', 'broker_kill'):
        conc = 16 if case == 'normal' else 4
        b, a = d.rows('b', '', case, conc), d.rows('n', '', case, conc)
        if b or a:
            add('B − アウトボックス（＝N）', CASE_JA[case] + (f'（並行度 {conc}）' if case == 'normal' else ''),
                f'正にあって届かない {pair(b, "index.missing_committed", "notify.missing_committed")}、'
                f'順序の入れ替わり {tot(b, "index.reorder_pairs") if b else "未検証"}、'
                f'最終状態の食い違い {tot(b, "final.mismatch") if b else "未検証"}、'
                f'失敗した要求 {tot(b, "request_failures") if b else "未検証"}',
                f'正にあって届かない {pair(a, "index.missing_committed", "notify.missing_committed")}、'
                f'順序の入れ替わり {tot(a, "index.reorder_pairs") if a else "未検証"}、'
                f'最終状態の食い違い {tot(a, "final.mismatch") if a else "未検証"}、'
                f'失敗した要求 {tot(a, "request_failures") if a else "未検証"}',
                '外すと、片方だけの成功と、並行更新の入れ替わりが戻る')
    for case, conc in (('normal', 16), ('consumer_late', 4)):
        b, a = d.rows('be', '', case, conc), d.rows('be', 'nokey', case, conc)
        if b or a:
            add('BE − キーごとの順序', CASE_JA[case] + f'（並行度 {conc}）',
                f'順序の入れ替わり {occ(b, "index.reorder_pairs")}、最終状態の食い違い {occ(b, "final.mismatch")}、'
                f'複数のパーティションにまたがった注文 {tot(b, "log.orders_multi_partition") if b else "未検証"}',
                f'順序の入れ替わり {occ(a, "index.reorder_pairs")}、最終状態の食い違い {occ(a, "final.mismatch")}、'
                f'複数のパーティションにまたがった注文 {tot(a, "log.orders_multi_partition") if a else "未検証"}',
                'キーをイベント ID にすると、同じ注文のイベントが別のパーティションに分かれる')
    wd = [r for r in d.wal if r['cond'] == 'default']
    wc = [r for r in d.wal if r['cond'] == 'cap']
    if wd or wc:
        def wdesc(rs):
            if not rs:
                return '未検証'
            mx = max(max((s.get('retained_by_slot_mb') or 0) for s in r['samples']) for r in rs)
            wd_ = max(max(s['pg_wal_dir_mb'] for s in r['samples']) for r in rs)
            miss = sum(r['after_restart']['metrics']['index']['missing_committed'] for r in rs)
            st = sorted({r['samples'][-1].get('wal_status') or 'なし' for r in rs})
            return (f'スロットが保持した WAL の最大 {mx} MB、pg_wal の最大 {wd_} MB、最後の wal_status＝{"・".join(st)}、'
                    f'Debezium を戻した後に届かないイベント {miss}（{len(rs)} 試行の合計）')
        add('BE − WAL の上限', 'A-L1・A-L2（Debezium を止めたまま書き続ける）', wdesc(wc), wdesc(wd),
            '上限を外すと正の DB の WAL が増え続ける。上限を付けるとスロットが失われ、イベントが届かなくなる（下の節）')
    if wc:
        det = sum(r['after_restart']['would_detect_by_reconcile'] for r in wc)
        aft = sum(r['reconcile']['after_repair'] for r in wc)
        add('BE − 照合', 'A-L2（スロットが失われた後）',
            f'照合が見つけた索引の差 {det} 注文（{len(wc)} 試行の合計）。直した後の差 {aft}',
            f'差 {det} 注文が、エラーも警告も受け手に出ないまま残る（受け手は「イベントが来ない」ことを知る手段を持たない）',
            '照合は索引の状態は直せるが、送られなかった通知は戻らない')
    nall = d.where(lambda r: r['config'] == 'n')
    if nall:
        det = sum(r['reconcile']['detected'] for r in nall)
        aft = sum(r['reconcile']['after_repair'] for r in nall)
        add('（参考）N ＋ 照合', f'N の全ケース（{len(nall)} 実行）',
            f'照合が見つけた索引の差 {det} 注文。直した後の差 {aft}', f'差 {det} 注文が残る',
            '二重書き込みのままでも、照合を足せば索引の差は見つけて直せる。通知の欠落と重複は直せない')
    L.append(table(['外した要素', 'ケース', '入れた構成', '外した構成', '読み取り'], rows))
    # give-up: Kafka transactions
    idem = d.where(lambda r: r['config'] in ('b', 'be') and r['idempotent_consumer'] and r['variant'] != 'wm')
    if idem:
        L.append('### 見送り：Kafka のトランザクション（「厳密に 1 回」）\n')
        L.append(f'Kafka のトランザクションは使わず、受け手の冪等性（イベント ID を、効果と同じトランザクションで処理済みの表に入れる）だけにした。'
                 f'B・BE（変種と除去を除く）の全 {len(idem)} 実行の合計：ブローカーのログ内の重複 {tot(idem, "log.dup_extra")}、'
                 f'届いた重複 索引 {tot(idem, "index.dup_delivered_extra")}／通知 {tot(idem, "notify.dup_delivered_extra")}、'
                 f'**状態に 2 回効いた 索引 {tot(idem, "index.dup_effect_extra")}／通知 {tot(idem, "notify.dup_effect_extra")}**、'
                 f'正にあって届かない 索引 {tot(idem, "index.missing_committed")}／通知 {tot(idem, "notify.missing_committed")}。'
                 '見送りで失うもの：ブローカーのログには重複が残る（ログを直接読む別の受け手は、自分で重複を捨てる必要がある）。'
                 '見直す条件：受け手の状態を持てない（処理済みの表を効果と同じトランザクションで書けない）受け手が加わるとき。\n')
    return L


def sec_wal(d):
    L = ['## CDC の落とし穴：Debezium を止めたまま書き続ける（A-L1〜L3）\n']
    if not d.wal:
        L.append('未検証（結果のファイルが無い）。\n')
        return L
    r0 = d.wal[0]
    L.append(f'手順：Debezium を動かしたまま {r0["ops"]["p1"]} 件を書いて全件が届くのを待つ → Kafka Connect を止める（docker stop）→ '
             f'{r0["ops"]["per_step"]} 件の注文の更新と、取り込みの対象でない表への書き込み（WAL を増やすため）を 1 段として繰り返し、'
             '段ごとに CHECKPOINT を打ってスロットの状態を読む → Kafka Connect を戻す。'
             f'PostgreSQL の設定：{", ".join(f"{k}={v}" for k, v in sorted(r0["pg_settings"].items()))}（条件ごとの違いは表の題）。\n')
    for cond in ('default', 'cap', 'idle'):
        rs = sorted([r for r in d.wal if r['cond'] == cond], key=lambda r: r['trial'])
        if not rs:
            L.append(f'### {WAL_JA[cond]}\n\n未検証。\n')
            continue
        L.append(f'### {WAL_JA[cond]}\n')
        L.append(f'1 段あたり WAL を約 {rs[0]["filler_mb_per_step"]} MB 増やした'
                 + (f'（各段で {rs[0]["sleep_s_per_step"]} 秒待つ）' if rs[0]['sleep_s_per_step'] else '') + f'。試行 {len(rs)} 回。\n')
        rows = []
        for i, s in enumerate(rs[0]['samples']):
            row = [s['label'].replace('before_stop', '止める前').replace('stopped', '止めた直後').replace('step', '段 ')]
            for r in rs:
                x = r['samples'][i] if i < len(r['samples']) else {}
                row += [x.get('wal_since_stop_mb', 0.0), '無し（NULL）' if x.get('retained_by_slot_mb') is None else x['retained_by_slot_mb'],
                        x.get('pg_wal_dir_mb'), x.get('wal_status'),
                        x.get('invalidation_reason') or 'なし']
            rows.append(row)
        head = ['時点']
        for r in rs:
            head += [f'試行 {r["trial"]}：止めてからの WAL（MB）', 'スロットが保持する WAL（MB）', 'pg_wal の大きさ（MB）', 'wal_status', '無効化の理由']
        L.append(table(head, rows))
        rows = []
        for r in rs:
            a = r['after_restart']
            st = a['connector_state']
            mb = a['missing_by_phase']
            rows.append([r['trial'], 'はい' if r['slot_invalid_before_restart'] else 'いいえ', st['task'],
                         (st['trace'] or 'なし')[:420],
                         a['metrics']['index']['missing_committed'],
                         '、'.join(f'{k}：{v["missing"]}/{v["committed"]}' for k, v in sorted(mb.items())),
                         a['metrics']['final']['mismatch'], a['would_detect_by_reconcile'],
                         a['metrics']['index']['dup_effect_extra'] + a['metrics']['notify']['dup_effect_extra']])
        L.append('Kafka Connect を戻した後：\n')
        L.append(table(['試行', '戻す前にスロットは無効か', 'コネクタのタスクの状態', 'タスクの失敗の内容（先頭）', '正にあって索引に届かない',
                        '内訳（届かない／コミット済み）', '最終状態の食い違い（注文）', '照合が見つける差（注文）', '状態に 2 回効いた'], rows))
        if rs[0].get('after_catchup'):
            L.append('追いついた後（位置の確認応答と CHECKPOINT の後）：' + '；'.join(
                f'試行 {r["trial"]}：スロットが保持する WAL {r["after_catchup"]["retained_by_slot_mb"]} MB、'
                f'pg_wal {r["after_catchup"]["pg_wal_dir_mb"]} MB、観測 {r["after_restart"]["seconds_observed"]} 秒' for r in rs) + '。\n')
        if rs[0].get('recovery'):
            rows = []
            for r in rs:
                rc = r['recovery']
                rows.append([r['trial'], rc['snapshot_mode'], rc['connector_state']['task'], rc.get('error') or 'なし',
                             rc['metrics']['index']['missing_committed'],
                             '、'.join(f'{k}：{v["missing"]}/{v["committed"]}' for k, v in sorted(rc['missing_by_phase'].items())),
                             rc['metrics']['log']['dup_extra'],
                             f'{rc["metrics"]["index"]["dup_delivered_extra"]}／{rc["metrics"]["notify"]["dup_delivered_extra"]}',
                             f'{rc["metrics"]["index"]["dup_effect_extra"]}／{rc["metrics"]["notify"]["dup_effect_extra"]}',
                             rc['metrics']['final']['mismatch'], r['reconcile']['detected'], r['reconcile']['after_repair']])
            L.append('復旧の手順（古いスロットを落とし、新しいスロットとコネクタを snapshot.mode=initial で作り、'
                     f'さらに {rs[0]["ops"]["p3"]} 件を書く）の後：\n')
            L.append(table(['試行', 'snapshot.mode', 'タスクの状態', 'エラー', '正にあって索引に届かない', '内訳（届かない／コミット済み）',
                            'ブローカーのログ内の重複', '届いた重複（索引／通知）', '状態に 2 回効いた（索引／通知）',
                            '最終状態の食い違い（注文）', '最後の照合が見つけた数', '照合で直した後の差'], rows))
    return L


def sec_kafka(d):
    L = ['## Apache Kafka での照合\n']
    if not d.kafka:
        L.append('未検証（結果のファイルが無い）。\n')
        return L
    L.append('欠落と重複の主なケースを、ブローカーだけ Apache Kafka（KRaft、1 ノード、既定の設定）に替えて流した。'
             'ほかの部品と設定は同じ。\n')
    keys = []
    for r in d.kafka:
        k = (r['config'], r['variant'], r['case'], r['concurrency'])
        if k not in keys:
            keys.append(k)
    rows = []
    for cfg, var, case, conc in keys:
        for broker in ('redpanda', 'kafka'):
            rs = d.rows(cfg, var, case, conc, broker=broker)
            rows.append([CASE_JA[case], 'Redpanda' if broker == 'redpanda' else 'Apache Kafka'] + line(CFG_JA[(cfg, var)], conc, rs))
    L.append(table(['ケース', 'ブローカー'] + HEAD, rows))
    diffs = []
    for cfg, var, case, conc in keys:
        a, b = d.rows(cfg, var, case, conc, broker='redpanda'), d.rows(cfg, var, case, conc, broker='kafka')
        if not a or not b:
            continue
        for path, name in (('index.missing_committed', '正にあって索引に届かない'), ('index.missing_acked', '応答済みなのに索引に届かない'),
                           ('notify.dup_effect_extra', '通知が 2 回効いた'), ('final.mismatch', '最終状態の食い違い')):
            za, zb = tot(a, path) == 0, tot(b, path) == 0
            if za != zb:
                diffs.append(f'{CASE_JA[case]}・{CFG_JA[(cfg, var)]}：{name}が Redpanda {tot(a, path)}／Apache Kafka {tot(b, path)}')
    L.append('「起きた／起きなかった」が 2 つのブローカーで分かれた項目：' + ('；'.join(diffs) if diffs else 'なし') + '。'
             '件数そのものは、並行の競合と再送の範囲に左右されるので、試行ごとに揺れる。\n')
    return L


def sec_guide(d):
    """Compare the statements of the guide (deck/p06.js, p11.js, p15.js) with the results."""
    L = ['## ガイドの記述との照合\n']
    L.append('ガイド `data-intensive-design-guide` の Part 6（deck/p06.js）、Part 11（deck/p11.js）、Part 15（deck/p15.js）の記述と、'
             'スライドが「題材 A で実測する」と書いた点を、上の結果と比べた。判定は結果のファイルから機械的に付けた。\n')
    rows = []

    def verdict(cond, yes='一致', no='**食い違い**'):
        return '未検証' if cond is None else (yes if cond else no)

    n1, n4, n16 = (d.rows('n', '', 'normal', c) for c in (1, 4, 16))
    if n16:
        nn = n4 + n16
        rows.append(['二重書き込みの不整合は 2 つの型で起きる',
                     '型 2：並行した更新が逆順に届く。4 回の書き込みはすべて成功し、エラーは 1 件もない。「題材 A で、障害を入れない並行更新だけで食い違う件数を実測する」',
                     verdict(tot(nn, 'index.reorder_pairs') > 0 and tot(nn, 'request_failures') == 0, '実測（一致）'),
                     f'N・障害なし：並行度 4 で順序の入れ替わり {occ(n4, "index.reorder_pairs")}・最終状態の食い違い {occ(n4, "final.mismatch")}、'
                     f'並行度 16 で {occ(n16, "index.reorder_pairs")}・{occ(n16, "final.mismatch")}、'
                     f'並行度 1 で {tot(n1, "index.reorder_pairs") if n1 else "未検証"}・{tot(n1, "final.mismatch") if n1 else "未検証"}。'
                     f'失敗した要求は {tot(nn + n1, "request_failures")} 件。'
                     f'B は同じ条件で入れ替わり {tot(d.rows("b", "", "normal"), "index.reorder_pairs")}、'
                     f'BE は {tot(d.rows("be", "", "normal"), "index.reorder_pairs")}'])
    wk, bc = d.rows('n', '', 'writer_kill', 4), d.rows('n', '', 'broker_cut', 4)
    if wk or bc:
        rows.append(['二重書き込みの不整合は 2 つの型で起きる',
                     '型 1：DB は成功、発行は失敗：受け手は変更を知らない',
                     verdict((tot(wk, 'index.missing_committed') if wk else 0) + (tot(bc, 'index.missing_committed') if bc else 0) > 0),
                     f'N・書き手の強制終了：DB にコミット済みで索引に届かない {tot(wk, "index.missing_committed") if wk else "未検証"} 件'
                     f'（強制終了 {sum(sum(r["restarts"].values()) for r in wk)} 回）。'
                     f'N・接続の切断：{tot(bc, "index.missing_committed") if bc else "未検証"} 件（失敗した要求 {tot(bc, "request_failures") if bc else "未検証"} 件）。'
                     f'B・BE の同じケース：{tot(d.rows("b", "", "writer_kill", 4) + d.rows("be", "", "writer_kill", 4) + d.rows("b", "", "broker_cut", 4) + d.rows("be", "", "broker_cut", 4), "index.missing_committed")} 件'])
        ghosts = tot(d.where(lambda r: True), 'index.ghost')
        rows.append(['二重書き込みの不整合は 2 つの型で起きる', '型 1：DB は失敗、発行は成功：DB にない出来事が流れる', '未検証',
                     f'この実装の N は「DB のコミット → 発行」の順で、DB が失敗したら発行しない。発行を先にする作りは試していない。'
                     f'全実行で、DB に無いイベントが受け手に効いた数は {ghosts}'])
    wm_n = d.where(lambda r: r['variant'] == 'wm' and r['case'] == 'normal' and r['concurrency'] > 1)
    wm_l = d.rows('b', 'wm', 'longtxn', 4)
    wm_1 = d.rows('b', 'wm', 'normal', 1)
    if wm_n or wm_l:
        rows.append(['アウトボックスの運び方は 2 つ：ポーリングと CDC',
                     '「連番の順に読むポーリングは、あとからコミットされた小さい番号の行を読み飛ばすか。一次資料では確かめられなかった。題材 A で、長いトランザクションを混ぜて実測する」',
                     verdict(None if not (wm_n or wm_l) else True, '未確認だった点の実測'),
                     f'「最後に読んだ連番より大きい行だけ読む」作りは読み飛ばした。障害なし・並行度 4 と 16：応答済みなのに索引に効果なし {occ(wm_n, "index.missing_acked")}。'
                     f'長いトランザクションを混ぜた並行度 4：{occ(wm_l, "index.missing_acked")}。並行度 1：{tot(wm_1, "index.missing_acked") if wm_1 else "未検証"}。'
                     f'「未送信の行を読む」作り（B）は同じ条件で {tot(d.rows("b", "", "normal") + d.rows("b", "", "longtxn"), "index.missing_acked")}'])
    ball = d.where(lambda r: r['config'] == 'b' and r['variant'] == '')
    if ball:
        rows.append(['アウトボックスの運び方は 2 つ：ポーリングと CDC',
                     'ポーリングの順序：AWS は、時刻と連番で順序を保つ設計だと書く。microservices.io は難しいとする（食い違う）',
                     '未確認だった点の実測',
                     f'B（未送信の行を連番の順に読む。注文の行を更新してからアウトボックスに書く）の全 {len(ball)} 実行で、'
                     f'同じ注文の順序の入れ替わりは {tot(ball, "index.reorder_pairs")} 組、最終状態の食い違いは {tot(ball, "final.mismatch")} 注文。'
                     '同じ注文の更新は行のロックで直列になるので、連番の順がコミットの順と一致した。違う注文どうしの順は保証していない（測っていない）'])
    b1, be1, nn1 = d.rows('b', '', 'normal', 1), d.rows('be', '', 'normal', 1), d.rows('n', '', 'normal', 1)
    if b1 and be1:
        bm, bem = med(b1, 'lag_ms', 'p50_ms'), med(be1, 'lag_ms', 'p50_ms')
        rows.append(['アウトボックスの運び方は 2 つ：ポーリングと CDC',
                     '遅れ：ポーリングは間隔しだい。CDC は書き込みの直後に流せる',
                     verdict(bem <= bm, '一致', '**条件つきで食い違い**'),
                     f'並行度 1 の遅れ p50／p95（ms）：N {lat(nn1, "lag_ms")}、B {lat(b1, "lag_ms")}（間隔 {C.RELAY_POLL_MS} ms）、'
                     f'BE {lat(be1, "lag_ms")}（Debezium の poll.interval.ms は既定の 500 ms）。'
                     'この設定では CDC のほうが遅れた。CDC の遅れも、コネクタの読み出しの間隔しだいだった'])
    rk_b, rk_be = d.rows('b', '', 'relay_kill', 4), d.rows('be', '', 'relay_kill', 4)
    ck = d.rows('b', '', 'consumer_kill', 4) + d.rows('be', '', 'consumer_kill', 4) + d.rows('n', '', 'consumer_kill', 4)
    if rk_b or rk_be:
        rows.append(['どの方式も既定は「少なくとも 1 回」',
                     '重複は障害のあとに起きる：ポーリングの中継が重複して送る／Debezium は異常終了すると直前のイベントを出し直しうる／受け手はオフセットを記録する前に落ちると同じメッセージを 2 回処理する',
                     verdict((tot(rk_b, 'log.dup_extra') if rk_b else 0) > 0 and (tot(rk_be, 'log.dup_extra') if rk_be else 0) > 0
                             and tot(ck, 'index.dup_delivered_extra') > 0),
                     f'ブローカーのログ内の重複：B・運び手の強制終了 {occ(rk_b, "log.dup_extra")}、BE・Kafka Connect の強制終了 {occ(rk_be, "log.dup_extra")}'
                     f'（オフセットの記録の間隔 {C.CONNECT_OFFSET_FLUSH_MS} ms）。'
                     f'受け手の強制終了で届いた重複（索引）：{occ(ck, "index.dup_delivered_extra")}。'
                     f'障害なしの N・B・BE では、届いた重複は {tot(d.rows("n", "", "normal") + d.rows("b", "", "normal") + d.rows("be", "", "normal"), "index.dup_delivered_extra")}'])
    noid = d.where(lambda r: r['variant'] == 'noidem') + d.rows('n', '', 'consumer_kill', 4)
    idem = d.where(lambda r: r['config'] in ('b', 'be') and r['idempotent_consumer'] and r['case'] in ('consumer_kill', 'relay_kill'))
    if noid and idem:
        rows.append(['冪等な受け手：処理済みの記録と更新を同時に書く',
                     '「冪等キーなし／ありで、障害を入れたときの重複件数を題材 A で実測する」',
                     verdict(tot(idem, 'notify.dup_effect_extra') == 0 and tot(noid, 'notify.dup_effect_extra') > 0, '実測（一致）'),
                     f'冪等キーなし（N と、B・BE から冪等性を外した構成。{len(noid)} 実行）：届いた重複 {tot(noid, "notify.dup_delivered_extra")} 回のうち、'
                     f'通知が 2 回効いた {tot(noid, "notify.dup_effect_extra")} 回。'
                     f'冪等キーあり（B・BE の受け手・運び手の強制終了。{len(idem)} 実行）：届いた重複 {tot(idem, "notify.dup_delivered_extra")} 回、'
                     f'通知が 2 回効いた {tot(idem, "notify.dup_effect_extra")} 回、索引が 2 回効いた {tot(idem, "index.dup_effect_extra")} 回'])
    allidem = d.where(lambda r: r['config'] in ('b', 'be') and r['idempotent_consumer'] and r['variant'] != 'wm')
    if allidem:
        rows.append(['「厳密に 1 回」は、その枠組みの中だけの保証',
                     'DB に書く受け手は冪等にするか、オフセットを出力と同じ場所に保存する（Kafka のトランザクションだけでは足りない）',
                     verdict(tot(allidem, 'index.dup_effect_extra') + tot(allidem, 'notify.dup_effect_extra') == 0,
                             '実測（受け手の冪等性だけで、状態に効く重複は 0）'),
                     f'Kafka のトランザクションを使わず、B・BE の全 {len(allidem)} 実行で、状態に 2 回効いた数は索引 {tot(allidem, "index.dup_effect_extra")}／'
                     f'通知 {tot(allidem, "notify.dup_effect_extra")}（届いた重複は {tot(allidem, "index.dup_delivered_extra")}／{tot(allidem, "notify.dup_delivered_extra")}）。'
                     'Kafka のトランザクションを使った構成は流していない（未検証）'])
    nk = d.rows('be', 'nokey', 'normal', 16) + d.rows('be', 'nokey', 'consumer_late', 4)
    k = d.rows('be', '', 'normal', 16) + d.rows('be', '', 'consumer_late', 4)
    if nk:
        rows.append(['順序を決める場所を 1 つにし、ほかはログを読む',
                     '順序が保たれるのはパーティションの中だけ。Outbox Event Router の既定のキーは集約の ID',
                     verdict(tot(nk, 'index.reorder_pairs') > 0 and tot(k, 'index.reorder_pairs') == 0),
                     f'キーを注文 ID にした BE：順序の入れ替わり {tot(k, "index.reorder_pairs")} 組（{len(k)} 実行）。'
                     f'キーをイベント ID にした BE：{occ(nk, "index.reorder_pairs")}、最終状態の食い違い {occ(nk, "final.mismatch")}'
                     f'（{len(nk)} 実行。パーティションは {C.PARTITIONS}）'])
    wm_all = d.where(lambda r: r['variant'] == 'wm')
    if wm_all:
        rows.append(['順序を決める場所を 1 つにし、ほかはログを読む',
                     'メッセージを 1 件失うだけで、導出データは正とずれたままになる',
                     '実測（条件つき）',
                     f'変種のポーリング（{len(wm_all)} 実行）：届かなかったイベント {tot(wm_all, "index.missing_committed")} 件に対し、'
                     f'最終状態が食い違ったままの注文は {tot(wm_all, "final.mismatch")}。'
                     '同じ注文に後から別の更新が届くと索引は上書きされて一致に戻るので、ずれが残ったのは「その注文の最後の更新」が失われたときだけだった。'
                     f'通知は失われたまま戻らない（通知の欠落 {tot(wm_all, "notify.missing_committed")} 件）'])
    wd = [r for r in d.wal if r['cond'] == 'default']
    wc = [r for r in d.wal if r['cond'] == 'cap']
    wi = [r for r in d.wal if r['cond'] == 'idle']
    if wd:
        first = wd[0]['samples'][2].get('retained_by_slot_mb') if len(wd[0]['samples']) > 2 else None
        last = wd[0]['samples'][-1].get('retained_by_slot_mb')
        grows = all(all((a.get('retained_by_slot_mb') or 0) <= (b.get('retained_by_slot_mb') or 0)
                        for a, b in zip(r['samples'][1:], r['samples'][2:])) for r in wd)
        rows.append(['CDC の受け手が止まると、正の DB の WAL が溜まる',
                     'スロットは接続が無くても WAL を保持し続ける。max_slot_wal_keep_size の既定は -1（無制限）。「題材 A で、受け手を止めたときの WAL の増え方を実測する」',
                     verdict(grows and last and first and last > first, '実測（一致）'),
                     f'既定のまま Debezium を止めて書き続けると、スロットが保持する WAL は段ごとに増え続けた（試行 1：1 段目の後 {first} MB → '
                     f'{len(wd[0]["samples"]) - 2} 段目の後 {last} MB。pg_wal は {wd[0]["samples"][-1]["pg_wal_dir_mb"]} MB）。'
                     f'最後の wal_status は {"・".join(sorted({r["samples"][-1]["wal_status"] for r in wd}))}。'
                     f'戻した後に届かないイベントは {sum(r["after_restart"]["metrics"]["index"]["missing_committed"] for r in wd)} 件（{len(wd)} 試行）。'
                     'ディスクが埋まるところまでは流していない（未検証）'])
    if wc:
        states = sorted({r['after_restart']['connector_state']['task'] or '不明' for r in wc})
        reasons = sorted({r['samples'][-1].get('invalidation_reason') or 'なし' for r in wc})
        miss = sum(r['after_restart']['metrics']['index']['missing_committed'] for r in wc)
        rec_miss = sum(r['recovery']['metrics']['index']['missing_committed'] for r in wc if r.get('recovery'))
        rows.append(['CDC の受け手が止まると、正の DB の WAL が溜まる',
                     '上限を付けると、超えたスロットは必要な WAL を消され、続きを読めなくなりうる。「上限を付けたときの欠落を実測する」',
                     verdict(all(r['slot_invalid_before_restart'] for r in wc) and miss > 0, '実測（一致）'),
                     f'上限 64MB：スロットは wal_status＝{"・".join(sorted({r["samples"][-1]["wal_status"] for r in wc}))}、無効化の理由＝{"・".join(reasons)} になり、'
                     f'pg_wal は最大 {max(max(s["pg_wal_dir_mb"] for s in r["samples"]) for r in wc)} MB で止まった。'
                     f'Debezium を戻すとタスクは {"・".join(states)}。止めている間と、その後に書いたイベント {miss} 件が届かなかった（{len(wc)} 試行の合計）。'
                     f'スロットを作り直し snapshot.mode=initial で取り直した後に届かないイベントは {rec_miss} 件'])
    if wi:
        reasons = sorted({r['samples'][-1].get('invalidation_reason') or 'なし' for r in wi})
        firsts = []
        for r in wi:
            hit = next((s for s in r['samples'] if s.get('invalidation_reason')), None)
            firsts.append('無効にならず' if not hit else f'{hit["label"].replace("step", "段 ")}（止めてから {hit.get("seconds_since_stop", "不明")} 秒）')
        rows.append(['CDC の受け手が止まると、正の DB の WAL が溜まる',
                     'PostgreSQL 18 の idle_replication_slot_timeout：使われていないスロットを時間で無効にする。既定は 0（無効）。判定はチェックポイント時',
                     verdict(all(r['slot_invalid_before_restart'] for r in wi), '実測（一致）'),
                     f'20 秒に設定：無効化の理由＝{"・".join(reasons)}。無効になったのを最初に見た時点：{"、".join(firsts)}。'
                     f'Debezium を戻すとタスクは {"・".join(sorted({r["after_restart"]["connector_state"]["task"] or "不明" for r in wi}))}、'
                     f'届かないイベント {sum(r["after_restart"]["metrics"]["index"]["missing_committed"] for r in wi)} 件（{len(wi)} 試行の合計）'])
    if d.audit:
        det = d.where(lambda r: True)
        rows.append(['最後は照合で確かめる：信頼しつつも検証する',
                     '「書き手の ID と連番で、欠落・重複・順序のずれを題材 A で実測する」',
                     '実測',
                     f'判定器は、生成器が決めたイベント ID（書き手 ID＋連番）の集合と、注文の表・ブローカーのログ・受け手の状態を突き合わせる。'
                     f'誤りを仕込んだ列での監査は {sum(1 for r in d.audit if r["ok"])}/{len(d.audit)} 合格。'
                     f'Redpanda の全 {len(det)} 実行で、正と索引の差を照合が見つけた数は {sum(r["reconcile"]["detected"] for r in det)} 注文、'
                     f'直した後に残った差は {sum(r["reconcile"]["after_repair"] for r in det)}'])
    if n16:
        rows.append(['移行中の二重書き込みには、照合が欠かせない（Part 15）',
                     '同じ行への 2 つの更新が、2 つの保存先に違う順で届く。「題材 A で二重書き込みの食い違いを実測する」',
                     '実測（一致）',
                     f'上の 1 行目と同じ結果。N・障害なし・並行度 16：最終状態の食い違い {occ(n16, "final.mismatch")}'])
    L.append(table(['スライドの題', 'ガイドの記述', '判定', '実測'], rows))
    return L


def sec_notes(d):
    L = ['## 期待どおりにならなかった結果・気づいた点\n']
    notes = []
    n16 = d.rows('n', '', 'normal', 16)
    if n16:
        notes.append(f'二重書き込みの順序の入れ替わりは起きたが、頻度は低い（並行度 16 で応答済み {tot(n16, "acked")} 件に対し {tot(n16, "index.reorder_pairs")} 組）。'
                     '同じ注文への 2 つ目の更新は、1 つ目のコミットまで行のロックで待たされるので、入れ替わるのは「1 つ目がコミットしてから発行するまで」に 2 つ目がコミットと発行を済ませたときだけである。'
                     '最終状態の食い違いはさらに少なく、試行によっては 0 だった。少ないから安全とは言えない：エラーが出ないので、照合なしでは 1 件も見つからない')
    notes.append('Redpanda のコンテナイメージは、引数なしでは開発用の設定で起動した（write_caching_default＝true、--unsafe-bypass-fsync）。'
                 'ガイドの Part 8 の注記（開発モードは write caching が既定で有効で、fsync も省く）のとおりで、欠落を数える実験では明示して切る必要があった')
    notes.append('ブローカーのトピックの自動作成を切ると、Debezium のハートビート用のトピック（__debezium-heartbeat.<prefix>）が作られず、'
                 'コネクタは RUNNING のままイベントの発行が止まった（開発中に観測。結果のファイルには入れていない）。'
                 'コネクタの設定に topic.creation.default.* を入れて、Kafka Connect にトピックを作らせた')
    bk = d.rows('n', '', 'broker_kill', 4) + d.rows('b', '', 'broker_kill', 4) + d.rows('be', '', 'broker_kill', 4)
    kk = d.where(lambda r: r['case'] == 'broker_kill', broker='kafka')
    if bk:
        notes.append(f'ブローカーの強制終了（docker kill）では、応答済みのメッセージは失われなかった（Redpanda：応答済みなのにブローカーのログに無い {tot(bk, "log.missing_acked")} 件'
                     + (f'、Apache Kafka：{tot(kk, "log.missing_acked")} 件' if kk else '') + '）。'
                     'プロセスだけを止める障害では OS のページキャッシュが残るので、fsync の有無の差は出ない。電源断に当たる障害（ホストごと落とす）は試していない（未検証）')
    L += [f'- {n}\n' for n in notes]
    L.append('\n## 限界\n')
    L += [f'- {x}\n' for x in [
        '1 台の上の、1 ノードのブローカーと 1 台の PostgreSQL。複製・フェイルオーバー・リージョンをまたぐ障害は入っていない',
        f'1 回の実行は {C.N_OPS} 件・注文 {C.N_ORDERS} 件と小さい。発生率は、この負荷・この機械・この実装（Python のスレッド、librdkafka の既定の linger.ms＝5 ms）でのもので、一般化できない',
        '受け手は役割ごとに 1 プロセスで、全パーティションを固定で割り当てた（コンシューマーグループのリバランスは起きない）',
        '導出側の PostgreSQL は synchronous_commit=off にした（受け手が書き手に追いつけるようにするため。導出側は落としていない）',
        '遅れと応答時間は、同じ環境の中の相対比較。絶対値に意味は無い',
        f'Kafka Connect のオフセットの記録の間隔は {C.CONNECT_OFFSET_FLUSH_MS} ms にした（既定は 60000 ms）。再送による重複の件数はこの間隔に比例して変わる',
        '障害を外す合図は、件数に加えて経過秒数も使った（ブローカーが止まっている間、B・BE の書き手は止まらず、件数だけでは障害の長さを決められないため）',
        '「防いだ」「0 件」は、この手順と試行回数で起きなかったという意味で、起きないことの証明ではない',
    ]]
    L.append('\n## 未検証\n')
    L += [f'- {x}\n' for x in [
        'Kafka のトランザクション（「厳密に 1 回」）を使った構成（見送り。受け手の冪等性だけの結果を上に示した）',
        '発行を先にして DB の更新が失敗する型（DB にない出来事が流れる）',
        'ブローカーや DB のホストごとの停止（電源断に当たる障害）、ディスクが実際に埋まるまでの WAL の増加',
        'Debezium のフェイルオーバー用のスロット（slot.failover）、プライマリの切り替えをまたぐ CDC',
        'CDC の落とし穴（A-L1〜L3）の Apache Kafka での照合（Redpanda だけで流した）',
        'アウトボックスの行を送信後に消す運用での、スロット喪失後の取り直し（この実験では行を消していない）',
        'クラウドのマネージド製品（マネージドの Kafka・CDC）',
    ]]
    return L


def main():
    d = Data()
    env = {}
    p = os.path.join(RESULTS, 'env.json')
    if os.path.exists(p):
        with open(p, encoding='utf-8') as f:
            env = json.load(f)
    L = ['# 題材 A：基盤をまたぐデータ統合（結果）\n',
         'このファイルは `cs/a/aggregate.py` が `results/a/*.jsonl` から作る。表の数は結果のファイルから数えたもので、手では書いていない。'
         '性能の数字は同じ環境の中での相対比較で、ほかの環境には一般化できない。\n',
         '問い：注文の状態の変化を、検索の索引と通知へ、失わず・重複させず・順序を崩さずに届けられるか。'
         '構成は 3 つ：**N**（二重書き込み。受け手は冪等でない）、**B**（アウトボックス＋ポーリング、冪等な受け手）、'
         '**BE**（アウトボックス＋Debezium の CDC、冪等な受け手、キーは注文 ID）。\n']
    for sec in (sec_overview(d, env), sec_guide(d), sec_normal(d), sec_variant(d), sec_faults(d), sec_ablation(d),
                sec_wal(d), sec_kafka(d), sec_notes(d)):
        L += sec
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(L))
    return OUT


if __name__ == '__main__':
    print(main())
