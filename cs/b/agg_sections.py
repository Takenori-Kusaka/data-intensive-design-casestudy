"""Tables of results/b/summary.md, one function per experiment. Every number is counted
from results/b/*.jsonl."""
import os
from collections import defaultdict
from statistics import median

from ..common.record import RESULTS, read_jsonl
from . import b12_schema as B12, b3_ddl as B3, b4_swap as B4, b5_rebuild as B5, b6_sor as B6, b7_mv as B7, schemas as S

FMT_JA = {'avro': 'Avro', 'protobuf': 'Protocol Buffers', 'json': 'JSON Schema'}
MODE_JA = {'NONE': 'N 検査なし（NONE）', 'DEFAULT': 'B 既定のまま（BACKWARD）', 'BACKWARD_TRANSITIVE': 'BE 推移的（BACKWARD_TRANSITIVE）'}
BROKER_JA = {'redpanda': 'Redpanda', 'kafka': 'Apache Kafka'}


def table(header, rows):
    out = ['| ' + ' | '.join(header) + ' |', '| ' + ' | '.join('---' for _ in header) + ' |']
    for r in rows:
        out.append('| ' + ' | '.join('未検証' if c is None or c == '' else str(c).replace('|', '／') for c in r) + ' |')
    return '\n'.join(out) + '\n'


def occ(vals):
    """'sum（trials with at least one / trials）'; a plain 0 when nothing happened."""
    vals = list(vals)
    if not vals:
        return '未検証'
    s = sum(vals)
    return '0' if s == 0 else f'**{s}**（{sum(1 for v in vals if v)}/{len(vals)} 試行）'


def med(vals, nd=1):
    vals = [v for v in vals if v is not None]
    if not vals:
        return '未検証'
    m, lo, hi = median(vals), min(vals), max(vals)
    f = (lambda x: str(int(round(x)))) if nd == 0 else (lambda x: str(round(x, nd)))
    return f(m) if lo == hi else f'{f(m)}（{f(lo)}〜{f(hi)}）'


def yes(b):
    return 'はい' if b else 'いいえ'


class Data:
    def __init__(self):
        for name in ('b1', 'b2', 'b3', 'b4', 'b5', 'b6', 'b7', 'audit'):
            setattr(self, name, read_jsonl(os.path.join(RESULTS, 'b', f'{name}.jsonl')))

    def b3of(self, exp, case):
        return [r for r in self.b3 if r['exp'] == exp and r['case'] == case]

    def b4of(self, config, variant):
        return [r for r in self.b4 if r['config'] == config and r['variant'] == variant]

    def b5of(self, broker, case, variant=None):
        return [r for r in self.b5 if r['broker'] == broker and r['case'] == case and (variant is None or r['variant'] == variant)]

    def b6of(self, config, scenario):
        return [r for r in self.b6 if r['config'] == config and r['scenario'] == scenario]


# ----------------------------------------------------------------------- B-1
def _tr(t):
    """'error／wrong／missing' out of n"""
    return f'{t["error"]}／{t["wrong"]}／{t["missing"]}'


def b1_accept(r, mode):
    return r['registry'][mode]['v2']['accepted']


def b1_broken(r):
    """(old reader on new data, new reader on old data): records that are not 'ok'."""
    t = r['traffic']
    return (t['old_reads_new']['n'] - t['old_reads_new']['ok'], t['new_reads_old']['n'] - t['new_reads_old']['ok'])


def b1_section(d):
    out = ['## B-1 イベントのスキーマ変更（新旧の書き手と受け手を混在）\n']
    if not d.b1:
        return out + ['未検証（結果のファイルが無い）\n']
    n = d.b1[0]['n_records']
    out.append(f'1 つの変更（v1 → v2）ごとに 1 回の実行。Redpanda のスキーマレジストリに、7 つの互換性モードと「何も設定しない」の計 8 通りで v1・v2 を登録し、'
               f'登録できたかを記録した。それとは別に、検査なしで配った場合として、旧い書き手（v1）と新しい書き手（v2）が同じトピックへ交互に {n} 件'
               f'（各 {n // 2} 件）を書き、旧い受け手（v1 のスキーマ）と新しい受け手（v2 のスキーマ）がそれぞれ全件を読んだ。'
               '受け手の結果は、生成器が書いた値と比べて 4 つに分ける：**エラー**（読めない）、**誤った値**（書き手が書いた値と違う値、または書き手が書いていない意味の値が入った）、'
               '**欠けた値**（エラーも誤った値も無いが、受け手のスキーマが必須とするフィールドが、型の既定値で黙って埋まった）、正しい。'
               'レジストリの何も設定しない場合の互換性モードは '
               f'{sorted({r["registry_default"]["compatibilityLevel"] for r in d.b1})} と表示された。\n')
    modes = ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']
    head = ['形式', '変更', '設計上の扱い', 'NONE', '設定なし', *modes, '旧い受け手が新しいデータを読む（エラー／誤った値／欠けた値）',
            '新しい受け手が旧いデータを読む（エラー／誤った値／欠けた値）']
    rows = []
    for fmt in S.FORMATS:
        for _i, key, _ja, intent in S.CHANGES:
            r = next((x for x in d.b1 if x['format'] == fmt and x['change'] == key), None)
            if r is None:
                continue
            acc = lambda m: '通る' if b1_accept(r, m) else '**拒否**'     # noqa: E731
            rows.append([FMT_JA[fmt], S.CHANGE_JA[key], '正常な変更' if intent == 'normal' else '非互換の変更', acc('NONE'), acc('DEFAULT'),
                         *[acc(m) for m in modes], _tr(r['traffic']['old_reads_new']), _tr(r['traffic']['new_reads_old'])])
    out.append(f'### 登録の結果と、検査なしで配ったときの受け手（各 {n // 2} 件）\n')
    out.append(table(head, rows))
    out.append('同じ版どうし（旧い受け手が旧いデータ、新しい受け手が新しいデータ）で正しく読めなかった件数の合計：'
               f'{sum(x["traffic"][k]["n"] - x["traffic"][k]["ok"] for x in d.b1 for k in ("old_reads_old", "new_reads_new"))} 件。\n')
    # configuration x outcome
    out.append('### 構成ごとの集計（変更の数）\n')
    out.append('構成は互換性モード。「止められた」は v2 の登録が拒否された変更。拒否されたら新しい書き手は配れないので、その変更による受け手の故障は起きない。'
               '「通って壊れた」は、登録が通り、かつ混在のどちらかの向きで、エラー・誤った値・欠けた値が 1 件以上出た変更。\n')
    head = ['形式', '構成', '変更の数', '止められた', '正常な変更が止められた', '通って壊れた：受け手がエラー', '通って壊れた：黙って誤った値・欠けた値', '通って無事']
    rows = []
    for fmt in S.FORMATS:
        rs = [x for x in d.b1 if x['format'] == fmt]
        for label, mode in (('N 検査なし（NONE）', 'NONE'), ('B 既定のまま（設定なし）', 'DEFAULT'), ('BE 推移的（BACKWARD_TRANSITIVE）', 'BACKWARD_TRANSITIVE'),
                            ('参考：FULL_TRANSITIVE', 'FULL_TRANSITIVE')):
            blocked = [x for x in rs if not b1_accept(x, mode)]
            passed = [x for x in rs if b1_accept(x, mode)]
            err = [x for x in passed if x['traffic']['old_reads_new']['error'] + x['traffic']['new_reads_old']['error'] > 0]
            silent = [x for x in passed if x not in err and sum(x['traffic'][k][c] for k in ('old_reads_new', 'new_reads_old')
                                                                 for c in ('wrong', 'missing')) > 0]
            rows.append([FMT_JA[fmt], label, len(rs), len(blocked), sum(1 for x in blocked if x['intent'] == 'normal'),
                         len(err), len(silent), len(passed) - len(err) - len(silent)])
    out.append(table(head, rows))
    return out


# ----------------------------------------------------------------------- B-2
def b2_section(d):
    out = ['## B-2 古いデータの読み直し（v1 → v2 → v3）\n']
    if not d.b2:
        return out + ['未検証（結果のファイルが無い）\n']
    n = d.b2[0]['n_per_phase']
    out.append(f'版を 1 つ登録するたびに、その時点で登録できている最新の版の書き手が {n} 件を書く（計 {3 * n} 件）。最後に、登録できた最新の版の受け手が、'
               'トピックを先頭から読み直す。「段ごとには通るが v3 が v1 を読めない列」は形式ごとに作った：'
               + '／'.join(f'{FMT_JA[f]}：{S.SEQ_STEP_JA[(f, "hole")]}' for f in S.FORMATS) + '。\n')
    head = ['形式', '列', '構成（レジストリの表示）', 'v2 の登録', 'v3 の登録', '読み直す受け手', '読み直し：v1 のデータ（エラー／誤った値／欠けた値）',
            '読み直し：全体で正しく読めた', '末尾だけ読む（最新の版のデータ）で正しく読めた']
    rows = []
    for fmt in S.FORMATS:
        for seq, seq_ja in S.SEQUENCES:
            for mode in B12.B2_MODES:
                r = next((x for x in d.b2 if x['format'] == fmt and x['sequence'] == seq and x['mode'] == mode), None)
                if r is None:
                    continue
                st = {s['version']: ('通る' if s['accepted'] else '**拒否**') for s in r['steps']}
                v1 = r['rebuild_by_writer'].get(f'{r["reader"]}_reads_v1')
                tot, tail = r['rebuild_total'], r['tail_read']
                rows.append([FMT_JA[fmt], seq_ja, f'{MODE_JA[mode]}（{r["effective_mode"].get("compatibilityLevel")}）', st['v2'], st['v3'],
                             r['reader'], _tr(v1) if v1 else None, f'{tot["ok"]}/{tot["n"]}', f'{tail["ok"]}/{tail["n"]}'])
    out.append(table(head, rows))
    return out


# ----------------------------------------------------------------------- B-3
def _locks(r):
    ls = r.get('locks_in_txn') or r.get('locks_sampled') or []
    strong = ['AccessExclusiveLock', 'ExclusiveLock', 'ShareRowExclusiveLock', 'ShareLock', 'ShareUpdateExclusiveLock',
              'RowExclusiveLock', 'RowShareLock', 'AccessShareLock']
    best = {}
    for rel, mode in ls:
        if rel not in best or strong.index(mode) < strong.index(best[rel]):
            best[rel] = mode
    return '、'.join(f'{rel}：{m.replace("Lock", "")}' for rel, m in sorted(best.items(), reverse=True)) or '観測できず'


def b3_section(d):
    out = ['## B-3 DB のスキーマ変更（PostgreSQL 18、書き込みをかけ続ける）\n']
    if not d.b3:
        return out + ['未検証（結果のファイルが無い）\n']
    out.append('負荷は、書き手 4 スレッドと読み手 2 スレッド（1 スレッド 1 接続、statement_timeout 3 秒、失敗した接続は捨てて張り直す）。'
               '「止まった時間」は、成功した要求の完了が 1 件も無かった最長の区間。値は 中央値（最小〜最大）。\n')
    out.append(f'### 操作ごとのロックと書き換え（{B3.ROWS_OPS:,} 行の表、各 {B3.TRIALS_OPS} 試行）\n')
    out.append('ロックは、文を実行したトランザクションが文の終わりに保持していた最も強いモード（トランザクションの中で実行できない CREATE INDEX CONCURRENTLY は、'
               '実行中に別の接続から見えたモード）。書き換えは、表の relfilenode が変わったかどうか。基準の列は、DDL の直前 1.2 秒の同じ負荷での値。\n')
    head = ['操作', '取ったロック', '表の書き換え', 'DDL の時間（ms）', '書き込みが止まった時間（ms）', '読み取りが止まった時間（ms）',
            '基準：書き込み／読み取りの最長の空き（ms）', '失敗した要求（書き込み／読み取り）', 'ロック待ちのセッションの最大数']
    rows = []
    for key, ja, *_ in B3.OPS:
        rs = d.b3of('ops', key)
        if not rs:
            rows.append([ja] + [None] * 8)
            continue
        fails = f'{sum(r["write"]["failed"] for r in rs)}／{sum(r["read"]["failed"] for r in rs)}'
        errs = sorted({e for r in rs for k in ('write', 'read') for e in r[k]['errors']})
        rows.append([ja, _locks(rs[0]), '、'.join(sorted({yes(r['rewrite']) for r in rs})), med([r['ddl_ms'] for r in rs], 0),
                     med([r['write']['max_gap_ms'] for r in rs], 0), med([r['read']['max_gap_ms'] for r in rs], 0),
                     f'{med([r["baseline_write"]["max_gap_ms"] for r in rs], 0)}／{med([r["baseline_read"]["max_gap_ms"] for r in rs], 0)}',
                     fails + (f'（{"、".join(errs)}）' if errs else ''), med([r['lock_waits']['max_waiting_sessions'] for r in rs], 0)])
    out.append(table(head, rows))
    out.append(f'### 長いトランザクションの後ろに並んだ DDL（各 {B3.TRIALS_QUEUE} 試行）\n')
    out.append(f'表を 1 回読んだだけのトランザクションを {B3.HOLD_S:.0f} 秒開いたままにし、0.5 秒後に `ALTER TABLE … ADD COLUMN`（単独なら数 ms の操作）を実行した。'
               'アプリの要求は 3 秒で時間切れになる。\n')
    head = ['条件', 'DDL が終わるまで（秒）', 'DDL の試行回数', '書き込みが止まった時間（ms）', '読み取りが止まった時間（ms）',
            '失敗した要求（書き込み／読み取り）', 'ロック待ちのセッションの最大数']
    rows = []
    for key, ja in B3.QUEUE:
        rs = d.b3of('queue', key)
        rows.append([ja] + ([None] * 6 if not rs else [
            med([r['ddl']['seconds'] for r in rs], 2), med([r['ddl']['attempts'] for r in rs], 0),
            med([r['write']['max_gap_ms'] for r in rs], 0), med([r['read']['max_gap_ms'] for r in rs], 0),
            f'{occ(r["write"]["failed"] for r in rs)}／{occ(r["read"]["failed"] for r in rs)}',
            med([r['lock_waits']['max_waiting_sessions'] for r in rs], 0)]))
    out.append(table(head, rows))
    out.append(f'### 住所の列を 2 列に分ける移行（{B3.ROWS_MIG:,} 行、各 {B3.TRIALS_MIG} 試行）\n')
    out.append('ガイドの例（address を postal_code と address_line に分ける）。アプリは 2 つの組に分かれ、0.7 秒ずらして版を上げる（逐次更新で新旧が混在する）。'
               '版は v1（旧列だけ読み書き）、v2a（両方に書き、旧列を読む）、v2b（両方に書き、新列を読む）、v3（新列だけ読み書き）。'
               '読み取りは、読んだ行の版番号から生成器の値を計算して突き合わせ、違えば「誤った読み取り」。最終状態は、負荷が触った '
               f'{B3.HOT:,} 行を生成器の値と比べる。\n')
    head = ['構成', '失敗した要求（書き込み／読み取り）', '誤った読み取り', '書き込みが止まった時間（ms）', '読み取りが止まった時間（ms）',
            '最終状態：値の食い違い（行）', '最終状態：応答済みの書き込みの欠落', '新列が NULL の行', '移行の所要時間（秒）']
    rows = []
    for key, ja in B3.MIG:
        rs = d.b3of('mig', key)
        rows.append([ja] + ([None] * 8 if not rs else [
            f'{occ(r["write"]["failed"] for r in rs)}／{occ(r["read"]["failed"] for r in rs)}', occ(r['read']['wrong'] for r in rs),
            med([r['write']['max_gap_ms'] for r in rs], 0), med([r['read']['max_gap_ms'] for r in rs], 0),
            occ(r['final']['value_mismatch'] for r in rs), occ(r['final']['lost_acked_write'] for r in rs),
            occ(r['final']['null_new_columns'] for r in rs), med([r['migration_s'] for r in rs], 1)]))
    out.append(table(head, rows))
    out.append('#### 段階ごとの失敗した要求と誤った読み取り（全試行の合計）\n')
    head = ['構成', '段階', '書き込み：成功', '書き込み：失敗', '読み取り：成功', '読み取り：失敗', '誤った読み取り', 'エラーの種類']
    rows = []
    for key, ja in B3.MIG:
        rs = d.b3of('mig', key)
        phases = []
        for r in rs:
            for p in list(r['write_by_phase']) + list(r['read_by_phase']):
                if p not in phases:
                    phases.append(p)
        order = ['before', 'ddl', 'expand', 'deploy_dual_write', 'backfill', 'deploy_read_new', 'relax_old', 'deploy_new', 'contract', 'after']
        for p in sorted(phases, key=lambda x: order.index(x) if x in order else 99):
            w = [r['write_by_phase'].get(p, {'ok': 0, 'failed': 0, 'errors': {}}) for r in rs]
            rd = [r['read_by_phase'].get(p, {'ok': 0, 'failed': 0, 'wrong': 0, 'errors': {}}) for r in rs]
            errs = sorted({e for x in w + rd for e in x['errors']})
            rows.append([ja.split('：')[0], PHASE_JA.get(p, p), sum(x['ok'] for x in w), sum(x['failed'] for x in w), sum(x['ok'] for x in rd),
                         sum(x['failed'] for x in rd), sum(x['wrong'] for x in rd), '、'.join(errs) or 'なし'])
    out.append(table(head, rows))
    return out


PHASE_JA = {'before': '開始前', 'ddl': '1 つのトランザクション（追加・埋め戻し・削除）', 'expand': '広げる（列の追加）',
            'deploy_dual_write': '両方に書く版（v2a）を配る', 'backfill': '埋め戻し', 'deploy_read_new': '新列を読む版（v2b）を配る',
            'relax_old': '旧列の NOT NULL を外す', 'deploy_new': '新列だけの版（v3）を配る', 'contract': '縮める（旧列の削除）', 'after': '完了後'}


# ----------------------------------------------------------------------- B-4
def _reads(r, key):
    return sum(v[key] for v in r['reads']['by_store'].values())


def b4_section(d):
    out = ['## B-4 導出ストアの入れ替え（ログから作り直す）\n']
    if not d.b4:
        return out + ['未検証（結果のファイルが無い）\n']
    r0 = d.b4[0]
    out.append(f'注文 {r0["n_orders"]} 件のイベント {r0["n_preload"]:,} 件を先に流し、その後は毎秒 {r0["live_rate"]} 件で流し続ける。読み手 2 スレッドが索引を読み続ける。'
               '旧の索引（注文ごとの状態・版・適用回数）から、新の索引（列を足し、状態ごとのイベント数の集計表を持つ）へ入れ替える。'
               '照合は、旧と新を同じスナップショットで 1 秒あけて 2 回読み、2 回とも食い違っていて新が動いていない注文（流れている途中ではない食い違い）が 0 で、'
               '新の集計表の合計が新の索引の適用回数の合計と一致すれば合格（最大 3 回）。最終状態は、流し終えてから、生成器から作った正解と比べる。\n')
    head = ['構成', '新しい受け手', '結果', '作り直しの時間（秒）', '照合：合格／流れていない食い違い（注文）', '照合：厳密に一致したスナップショット',
            '読み取り：見つからない', '読み取り：前より古い版', '使っているストアの最終状態：状態の食い違い（注文）', '適用回数の食い違い（注文）', '集計の食い違い（状態）']
    rows = []
    for cfg, var in B4.CASES:
        rs = d.b4of(cfg, var)
        if not rs:
            rows.append([B4.CONFIG_JA[cfg], B4.VARIANT_JA[var]] + [None] * 9)
            continue
        outc = defaultdict(int)
        for r in rs:
            outc[OUTCOME_JA[r['outcome']]] += 1
        rec = [r['reconciliation'] for r in rs if r['reconciliation']]
        serving = [r['final'][r['serving']] for r in rs]
        rows.append([B4.CONFIG_JA[cfg], B4.VARIANT_JA[var], '、'.join(f'{k} {v}/{len(rs)}' for k, v in outc.items()),
                     med([r['rebuild_s'] for r in rs], 2),
                     f'{sum(1 for x in rec if x["passed"])}/{len(rec)}／{med([x["persistent"] for x in rec], 0)}' if rec else '照合なし',
                     f'{sum(x["exact_equal_snapshots"] for x in rec)}/{sum(x["snapshots"] for x in rec)}' if rec else '照合なし',
                     occ(_reads(r, 'not_found') for r in rs), occ(_reads(r, 'regress') for r in rs),
                     occ(x['state_mismatch'] for x in serving), occ(x['count_mismatch'] for x in serving),
                     occ(x['agg_mismatch'] for x in serving) if all(x['agg_mismatch'] is not None for x in serving) else '集計表なし'])
    out.append(table(head, rows))
    ab = [r for r in d.b4 if r['outcome'] == 'aborted_by_reconciliation' and 'new' in r['final']]
    if ab:
        out.append('照合で止めた実行で、作りかけの新のストアを最後まで追従させて正解と比べた結果（切り替えていたら読み手が見たはずの状態）：\n')
        rows = []
        for var in ('bug', 'crash_noidem'):
            rs = [r for r in ab if r['variant'] == var]
            if rs:
                nw = [r['final']['new'] for r in rs]
                rows.append([B4.VARIANT_JA[var], len(rs), occ(x['state_mismatch'] for x in nw), occ(x['count_mismatch'] for x in nw),
                             occ(x['count_surplus'] for x in nw), occ(x['count_deficit'] for x in nw), occ(x['agg_mismatch'] for x in nw)])
        out.append(table(['新しい受け手', '実行', '状態の食い違い（注文）', '適用回数の食い違い（注文）', '余分に適用した回数', '足りない回数', '集計の食い違い（状態）'], rows))
        both = [r['store_bytes'] for r in ab if 'idx_old' in r['store_bytes'] and 'idx_new' in r['store_bytes']]
        if both:
            out.append(f'保存域（新旧を並べている間。照合で止めた {len(both)} 実行の終わりの値）：旧の索引 {med([b["idx_old"] / 1024 for b in both], 0)} KiB、'
                       f'新の索引と集計表 {med([(b["idx_new"] + b.get("agg_new", 0)) / 1024 for b in both], 0)} KiB。\n')
    return out


OUTCOME_JA = {'rebuilt_in_place': '同じ場所で作り直した', 'switched': '切り替えた', 'aborted_by_reconciliation': '照合で止めた（旧のまま）'}


# ----------------------------------------------------------------------- B-5
def b5_section(d):
    out = ['## B-5 作り直しの前提条件（保持・スナップショット・コンパクション・削除の印）\n']
    if not d.b5:
        return out + ['未検証（結果のファイルが無い）\n']
    out.append(f'時間は縮めた：retention.ms は {B5.RETENTION_MS} ms（Kafka の既定の 7 日の代わり）、delete.retention.ms は {B5.TOMBSTONE_MS} ms（同 1 日の代わり）。'
               'セグメントは 1 MiB・2 秒で切り、掃除の間隔は 1 秒にした（Redpanda：log_compaction_interval_ms、Apache Kafka：log.cleaner.backoff.ms と '
               'log.retention.check.interval.ms）。セグメントを切らせるため、毎秒 1 件の詰め物のレコードを流した。'
               '「製品の既定のまま」の実行は、保持の設定を変えずに同じだけ待った。\n')
    out.append('### 製品が表示したトピックの既定値\n')
    head = ['製品', 'cleanup.policy', 'retention.ms', 'retention.bytes', 'delete.retention.ms']
    rows = []
    for b in ('kafka', 'redpanda'):
        rs = d.b5of(b, 'tombstone', 'default')        # nothing but cleanup.policy and the segment settings was set
        rr = d.b5of(b, 'retention', 'default')
        t = rs[0]['topic_config_reported'] if rs else {}
        r = rr[0]['topic_config_reported'] if rr else {}
        rows.append([BROKER_JA[b], r.get('cleanup.policy') or (f'未検証（compact を指定した実行だけ）' if rs else None),
                     r.get('retention.ms') or t.get('retention.ms'), r.get('retention.bytes') or t.get('retention.bytes'),
                     t.get('delete.retention.ms') if rs else None])
    out.append(table(head, rows))
    out.append('### 保持期間とスナップショット（cleanup.policy=delete）\n')
    out.append('イベント 12,000 件のうち 8,000 件を流し、その時点の導出ストアをスナップショットとして取り（対応するオフセットも記録）、古いセグメントが消えるのを待ってから残りを流し、作り直した。'
               '注文 500 件のうち 100 件は、最初の 2,000 件にしかイベントが無い（古い注文）。\n')
    head = ['製品', '作り直し方', '試行', 'ログの先頭のオフセット', 'ログに残ったイベント', '作り直しの出発点', '無い注文', '状態の食い違い（注文）', '適用回数の食い違い（注文）', '作り直しの時間（秒）']
    rows = []
    for b in ('redpanda', 'kafka'):
        for var in ('default', 'log_only', 'snap_pos', 'snap_latest'):
            rs = d.b5of(b, 'retention', var)
            if not rs:
                if (b, 'retention', var) in B5.CASES:
                    rows.append([BROKER_JA[b], B5.VARIANT_JA[var]] + [None] * 8)
                continue
            rows.append([BROKER_JA[b], B5.VARIANT_JA[var], len(rs), med([r['log']['low'] for r in rs], 0),
                         med([r['log']['records'] for r in rs], 0) + f'/{rs[0]["events"]}',
                         {'default': '先頭（0）', 'log_only': '残っている最古', 'snap_pos': f'スナップショット＋オフセット {rs[0]["snapshot_offset"]}',
                          'snap_latest': 'スナップショット＋末尾'}[var],
                         occ(r['final']['missing'] for r in rs), occ(r['final']['state_mismatch'] for r in rs),
                         occ(r['final']['count_mismatch'] for r in rs), med([r['rebuild_s'] for r in rs], 2)])
    out.append(table(head, rows))
    out.append('### コンパクション（cleanup.policy=compact）\n')
    head = ['製品', '試行', 'レコード数（流した数 → コンパクション後）', '残ったレコードでオフセットが変わった', '同じ注文の順序の逆転', '作り直し：状態の食い違い（注文）',
            '作り直し：適用回数の食い違い（注文）', '作り直し：集計で足りないイベント', '待った時間（秒）']
    rows = []
    for b in ('redpanda', 'kafka'):
        rs = d.b5of(b, 'compact')
        rows.append([BROKER_JA[b]] + ([None] * 8 if not rs else [
            len(rs), f'{rs[0]["events"]} → {med([r["log_after"]["records"] for r in rs], 0)}',
            occ(r['log_after']['offset_changed'] for r in rs), occ(r['log_after']['order_violations'] for r in rs),
            occ(r['final']['state_mismatch'] for r in rs), occ(r['final']['count_mismatch'] for r in rs),
            med([r['final']['agg_deficit'] for r in rs], 0), med([r['waited_s'] for r in rs], 0)]))
    out.append(table(head, rows))
    out.append('### 削除の印（トゥームストーン）\n')
    out.append('注文 500 件を読み終えた受け手を止め、100 件の注文を削除（値が null のレコード）してから、待って、受け手を再開した。'
               '同じ実行の中で、止めなかった受け手、削除の処理を持たない受け手、最後に先頭から作り直した受け手も比べた。'
               '数は、削除したのに導出ストアに残った注文（全試行の合計）。監視（E-B3）は、受け手が遅れていた時間が、製品が表示した delete.retention.ms 以上なら警報。\n')
    head = ['製品', 'delete.retention.ms（表示）', '試行', '削除の印がログから消えた', '消えるまで（秒）', '止めていた時間（秒）', '監視の警報',
            '残った：止めて再開した受け手', '残った：止めなかった受け手', '残った：削除の処理が無い受け手', '残った：先頭から作り直し',
            '削除が届くまで p50／p95（ms、止めなかった受け手）']
    rows = []
    for b in ('redpanda', 'kafka'):
        for var in ('short', 'default'):
            rs = d.b5of(b, 'tombstone', var)
            if not rs:
                rows.append([BROKER_JA[b], B5.VARIANT_JA[var]] + [None] * 10)
                continue
            f = lambda k: occ(r['final'][k]['deleted_left'] for r in rs)       # noqa: E731
            rows.append([BROKER_JA[b], f'{rs[0]["topic_config_reported"].get("delete.retention.ms")}（{B5.VARIANT_JA[var]}）', len(rs),
                         f'{sum(1 for r in rs if r["tombstones_gone"])}/{len(rs)}', med([r['tombstones_gone_after_s'] for r in rs], 0),
                         med([r['slow_lag_age_ms'] / 1000 for r in rs], 0), f'{sum(1 for r in rs if r["alarm"])}/{len(rs)}',
                         f('slow'), f('fast'), f('nodel'), f('fresh'),
                         f'{med([r["delete_latency_fast"]["p50_ms"] for r in rs], 0)}／{med([r["delete_latency_fast"]["p95_ms"] for r in rs], 0)}'])
    out.append(table(head, rows))
    cdc = d.b5of('redpanda', 'cdc_delete')
    out.append('### PostgreSQL の DELETE を Debezium で流す\n')
    if not cdc:
        out.append('未検証（この実験は流していない）\n')
    else:
        out.append(f'{cdc[0]["rows"]} 行を挿入し、{cdc[0]["deleted"]} 行を 1 行ずつ DELETE した。コネクタの tombstones.on.delete は設定していない（既定）。\n')
        head = ['試行', 'トピックの削除イベント（op=d）', '値が null のレコード', '削除イベントの直後に同じキーの null', '導出に残った削除済みの行', '導出に無い行',
                '削除が届くまで p50／p95（ms）']
        rows = [[r['trial'], r['topic']['delete_events'], r['topic']['tombstones'], r['topic']['delete_followed_by_tombstone'],
                 r['final']['deleted_left'], r['final']['missing'], f'{r["delete_latency"]["p50_ms"]}／{r["delete_latency"]["p95_ms"]}'] for r in cdc]
        out.append(table(head, rows))
    return out


# ----------------------------------------------------------------------- B-6
STAGE_JA = {'0_old_only': '0 旧だけ', '1_parallel': '1 並行稼働（新を 1 回再起動）', '2_backfill': '2 埋め戻し', '3_reconcile': '3 照合',
            '4_read_new': '4 読みを切替', '5_write_new': '5 書きを切替', '6_old_removed': '6 旧をやめる', '6_rollback': '6 旧へ切り戻す'}


def b6_section(d):
    out = ['## B-6 記録のシステムの入れ替え（PostgreSQL から別のインスタンスへ）\n']
    if not d.b6:
        return out + ['未検証（結果のファイルが無い）\n']
    out.append(f'表は注文（連番の主キー、外部の ID、状態、版）で、先に {B6.PRELOAD:,} 行を入れる。書き手 4・読み手 2 スレッドが、挿入（3 割）と更新（7 割）を続ける。'
               'アプリは 2 つの組に分かれ、行き先の切り替えを 0.7 秒ずらす。並行稼働の段階で、新のインスタンスを 1 回再起動する（障害）。'
               '読み取りは、読む前に応答済みだった版より古いか、行が無ければ「古い・無い読み取り」。'
               '最終状態は、負荷が応答を受けた書き込みの記録と、最後に正になったインスタンスの表を比べる。\n')
    head = ['構成', '筋書き', '試行', '失敗した要求（書き込み）', 'エラーの種類', '古い・無い読み取り', '切り替えで書き込みが止まった時間（ms）',
            '最終：無い行', '最終：応答済みの書き込みの欠落', '最終：値の食い違い']
    rows = []
    for cfg, sc in B6.CASES:
        rs = d.b6of(cfg, sc)
        if not rs:
            rows.append([B6.CONFIG_JA[cfg], B6.SCENARIO_JA[sc]] + [None] * 8)
            continue
        errs = defaultdict(int)
        for r in rs:
            for k, v in r['write_all']['errors'].items():
                errs[k] += v
        rows.append([B6.CONFIG_JA[cfg], B6.SCENARIO_JA[sc] + f'（最後の正：{"新" if rs[0]["final_side"] == "new" else "旧"}）', len(rs),
                     occ(r['write_all']['failed'] for r in rs), '、'.join(f'{k} {v}' for k, v in sorted(errs.items())) or 'なし',
                     occ(r['read_all']['wrong'] for r in rs), med([r['write_switch']['max_gap_ms'] for r in rs], 0),
                     occ(r['final']['missing'] for r in rs), occ(r['final']['lost_acked_write'] for r in rs),
                     occ(r['final']['value_mismatch'] for r in rs)])
    out.append(table(head, rows))
    out.append('### 段階ごとの失敗した書き込みと、古い・無い読み取り（全試行の合計）\n')
    stages = list(STAGE_JA)
    head = ['構成', '筋書き'] + [STAGE_JA[s] for s in stages]
    rows = []
    for cfg, sc in B6.CASES:
        rs = d.b6of(cfg, sc)
        if not rs:
            continue
        cells = []
        for s in stages:
            if not any(s in r['write_by_phase'] or s in r['read_by_phase'] for r in rs):
                cells.append('この段階なし')
                continue
            wf = sum(r['write_by_phase'].get(s, {}).get('failed', 0) for r in rs)
            rw = sum(r['read_by_phase'].get(s, {}).get('wrong', 0) for r in rs)
            cells.append(f'{wf}／{rw}')
        rows.append([B6.CONFIG_JA[cfg], B6.SCENARIO_JA[sc]] + cells)
    out.append('各セルは「失敗した書き込み／古い・無い読み取り」。\n')
    out.append(table(head, rows))
    out.append('### 並行稼働・埋め戻し・照合の中身\n')
    head = ['構成', '試行', '二重書き込みで新への書き込みが失敗', '二重書き込みで新に行が無かった更新', '埋め戻しの前：新が欠ける・古い行', '埋め戻しの後：新が欠ける・古い行',
            '埋め戻しの時間（秒）', '切り替え前のシーケンスの値（旧／新）', '切り替え前の最大の主キー（旧）', '遮断から新が追いつくまで（秒）', '購読の適用エラー']
    rows = []
    for cfg in B6.CONFIG_JA:
        rs = [r for r in d.b6 if r['config'] == cfg]
        if not rs:
            continue
        st = [r['steps'] for r in rs]
        rows.append([B6.CONFIG_JA[cfg], len(rs), occ(r['dual_new_failed'] for r in rs) if cfg.startswith('n') else '二重書き込みなし',
                     occ(r['dual_new_missed'] for r in rs) if cfg.startswith('n') else '二重書き込みなし',
                     med([s['diff_before_backfill'] for s in st if 'diff_before_backfill' in s], 0) if cfg.startswith('n') else '初期コピーが埋め戻し',
                     occ(s['diff_after_backfill'] for s in st), med([s['backfill_s'] for s in st], 2),
                     f'{med([s["sequence_before_switch"]["pg"] for s in st], 0)}／{med([s["sequence_before_switch"]["pg2"] for s in st], 0)}',
                     med([s['max_id_before_switch']['pg'] for s in st], 0),
                     med([s['catch_up_s'] for s in st if s.get('catch_up_s') is not None], 3) if any('catch_up_s' in s for s in st) else '遮断なし',
                     occ(r['subscription_apply_errors'] for r in rs) if rs[0]['subscription_apply_errors'] is not None else '購読なし'])
    out.append(table(head, rows))
    out.append('「埋め戻しの時間」は、埋め戻しの段階に入ってから終わるまで。BE の初期コピーは購読を作った時点（並行稼働の段階）から始まっていて、この段階に入る前に終わっていた。'
               '「埋め戻しの前：新が欠ける・古い行」が大きいのは、先に入れた行が、埋め戻すまで新に無いため。\n')
    rb = [r for r in d.b6 if r['scenario'] == 'rollback']
    if rb:
        out.append('### 切り戻し\n')
        head = ['構成', '試行', '新だけに入った応答済みの書き込み（キー）', '切り戻しで書き込みが止まった時間（ms）', '切り戻し後の旧：無い行', '切り戻し後の旧：応答済みの書き込みの欠落',
                '新から旧へ追いつくまで（秒）']
        rows = []
        for cfg in B6.CONFIG_JA:
            rs = [r for r in rb if r['config'] == cfg]
            if rs:
                rows.append([B6.CONFIG_JA[cfg], len(rs), med([r['acked_on_new_only'] for r in rs], 0),
                             med([r['write_rollback']['max_gap_ms'] for r in rs], 0), occ(r['final']['missing'] for r in rs),
                             occ(r['final']['lost_acked_write'] for r in rs),
                             med([r['steps'].get('rollback_catch_up_s') for r in rs], 3) if any('rollback_catch_up_s' in r['steps'] for r in rs) else '逆向きの複製なし'])
        out.append(table(head, rows))
    return out


# ----------------------------------------------------------------------- B-7
def b7_section(d):
    out = ['## B-7 マテリアライズドビューが追随するもの（ガイド Part 12）\n']
    if not d.b7:
        return out + ['未検証（結果のファイルが無い）\n']
    pg = [r for r in d.b7 if r['product'] == 'pg']
    out.append('### PostgreSQL 18：REFRESH の間の読み取り\n')
    if not pg:
        out.append('未検証（この実験は流していない）\n')
    else:
        out.append(f'{pg[0]["rows"]:,} 行の表を {pg[0]["groups"]:,} 組に集計したビュー。元の表を更新・削除・挿入してから REFRESH し、その間、読み手 4 スレッドがビューを 1 行ずつ読み続けた。\n')
        head = ['操作', '試行', '元の表を変えた後の古い組（REFRESH 前）', 'REFRESH の時間（ms）', 'ビューに取ったロック', '読み取りが止まった時間（ms）',
                '読み取りの最大の応答（ms）', '基準：読み取りの最長の空き（ms）', '失敗した読み取り', 'REFRESH 後の古い組']
        rows = []
        for case in ('refresh', 'refresh_concurrently'):
            rs = [r for r in pg if r['case'] == case]
            if not rs:
                rows.append([B7.CASE_JA[case]] + [None] * 9)
                continue
            locks = sorted({m.replace('Lock', '') for r in rs for _rel, m in r['locks_sampled']})
            rows.append([B7.CASE_JA[case], len(rs), med([B7.n_diff(r['after_base_change']) for r in rs], 0), med([r['refresh_ms'] for r in rs], 0),
                         '、'.join(locks) or '観測できず', med([r['read']['max_gap_ms'] for r in rs], 0), med([r['read']['max_ms'] for r in rs], 0),
                         med([r['baseline_read']['max_gap_ms'] for r in rs], 0), occ(r['read']['failed'] for r in rs),
                         occ(B7.n_diff(r['after_refresh']) for r in rs)])
        out.append(table(head, rows))
    ch = [r for r in d.b7 if r['product'] == 'ch']
    out.append('### ClickHouse：増分・結合つき増分・refreshable\n')
    if not ch:
        out.append('未検証（この実験は流していない）\n')
    else:
        out.append(f'元の表に {ch[0]["rows"]:,} 行（店 {ch[0]["stores"]} 件）。各手順の後で、ビューの中身を、同じ問い合わせを元の表に直接かけた結果と比べた。'
                   f'数は、食い違った組（店、または地域）の数で、{len(ch)} 試行の 中央値（最小〜最大）。\n')
        head = ['手順', '増分 MV（店ごとの件数と合計）', '結合つき増分 MV（地域ごとの合計）', 'refreshable MV（店ごとの件数と合計）']
        rows = []
        for i, st in enumerate(ch[0]['steps']):
            rows.append([st['label']] + [med([B7.n_diff(r['steps'][i][k]) for r in ch], 0) + f'/{st[k]["keys_expected"]}'
                                         for k in ('incremental', 'incremental_join', 'refreshable')])
        out.append(table(head, rows))
    return out
