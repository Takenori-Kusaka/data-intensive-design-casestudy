"""Build results/b/summary.md from results/b/*.jsonl. Every number in the tables is
counted from the result files; nothing is typed by hand. The verdicts of the comparison
with the guide are derived from the same numbers."""
import json
import os
import re
from statistics import median

from ..common import tworuns
from ..common.record import RESULTS
from . import agg_sections as A, b3_ddl as B3, b5_rebuild as B5, config as C, schemas as S
from .agg_sections import med, occ, table

OUT = os.path.join(RESULTS, 'b', 'summary.md')
NA = '未検証'
# Columns that hold counts depending on the instant of an operation under load: where the two runs differ, both are written.
COUNTS = re.compile('失敗した要求|誤った読み取り|：失敗$|エラーの種類|照合：|^[0-6] |ログの先頭のオフセット|ログに残ったイベント|レコード数'
                    '|古い・無い読み取り|監視の警報|削除の印がログから消えた|二重書き込みで|埋め戻しの[前後]|購読の適用エラー|新だけに入った')


def _env():
    p = os.path.join(RESULTS, 'env.json')
    return json.load(open(p, encoding='utf-8')) if os.path.exists(p) else {}


def overview(d):
    out = ['## 実行の概要\n']
    names = [('b1', 'B-1 イベントのスキーマ変更'), ('b2', 'B-2 古いデータの読み直し'), ('b3', 'B-3 DB のスキーマ変更'),
             ('b4', 'B-4 導出ストアの入れ替え'), ('b5', 'B-5 作り直しの前提条件'), ('b6', 'B-6 記録のシステムの入れ替え'),
             ('b7', 'B-7 マテリアライズドビュー')]
    rows = [[ja, f'{k}.jsonl', len(getattr(d, k))] for k, ja in names]
    rows.append(['判定器の監査', 'audit.jsonl', f'{len(d.audit)}（合格 {sum(1 for r in d.audit if r["ok"])}）'])
    out.append(table(['実験', '結果のファイル', '実行の数（行）'], rows))
    out.append('1 回の実行ごとに、`dic-b-` のコンテナとボリュームを消して作り直した。乱数の種は 20261119＋試行番号。'
               '数の書き方：「**12**（3/5 試行）」は、試行の合計が 12 で、5 試行のうち 3 試行で 1 件以上起きたこと。「中央値（最小〜最大）」は試行の間の幅。\n')
    env = _env()
    out.append('### 製品の版とイメージ\n')
    rows = [[img, '、'.join((env.get('images', {}).get(img) or {}).get('repo_digests', [])) or NA,
             f'{(env.get("images", {}).get(img) or {}).get("size_mb", NA)} MB'] for img in C.IMAGES.values()]
    out.append(table(['イメージ（タグ）', 'ダイジェスト', '大きさ'], rows))
    sv = env.get('topic_b_server_versions') or {}
    tb = env.get('topic_b') or {}
    out.append('サーバーが返した版：' + ('、'.join(f'{k} {v}' for k, v in sorted(sv.items())) or NA) + '。クライアント：'
               + ('、'.join(f'{k} {v}' for k, v in sorted(tb.items()) if not k.startswith('run_date')) or NA)
               + f'。結果のファイルに記録した日付（UTC）：{tb.get("run_date", NA)}〜{tb.get("run_date_last", NA)}。\n')
    mem = env.get('topic_b_container_memory_seen') or {}
    if mem:
        out.append('コンテナのメモリ（docker stats で見た最大の値）：' + '、'.join(f'{k} {v}' for k, v in sorted(mem.items())) + '。\n')
    return out


# --------------------------------------------------------------- comparison
def _b1(d, fmt, key):
    return next((r for r in d.b1 if r['format'] == fmt and r['change'] == key), None)


def _t(r, k):
    t = r['traffic'][k]
    return f'エラー {t["error"]}・誤った値 {t["wrong"]}・欠けた値 {t["missing"]}（{t["n"]} 件中）'


def _rej(r):
    return [m for m in B12_MODES if not r['registry'][m]['v2']['accepted']]


B12_MODES = ['BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE']


def _rejs(r):
    x = _rej(r)
    return '全モードで通る' if not x else ('拒否：' + '・'.join(x))


def safe(rows, title, claim, fn):
    try:
        verdict, measured = fn()
    except (KeyError, IndexError, TypeError, StopIteration, ZeroDivisionError):
        verdict, measured = NA, '結果のファイルに該当の実行が無い'
    rows.append([title, claim, verdict, measured])


def _snaps(d):
    """Snapshots in which old and new were exactly equal, per variant; the denominator is the number of snapshots taken."""
    def one(v):
        rs = d.b4of('be', v)
        return (sum(r['reconciliation']['exact_equal_snapshots'] for r in rs), sum(r['reconciliation']['snapshots'] for r in rs), len(rs))
    a, b = one('clean'), one('crash')
    return (f'欠陥なしの {a[2]} 試行で {a[0]}/{a[1]} 個、受け手が 1 回落ちた {b[2]} 試行で {b[0]}/{b[1]} 個'
            f'（合わせて {a[0] + b[0]}/{a[1] + b[1]} 個。分母は、照合で取ったスナップショットの数）')


def _v1(r):
    """What the newest reader got from the v1 records when re-reading from the start."""
    v1 = r['rebuild_by_writer'][f'{r["reader"]}_reads_v1']
    tot = r['rebuild_total']
    return (f'{A.FMT_JA[r["format"]]} v1 のデータ {v1["n"]} 件のうち エラー {v1["error"]}・誤った値 {v1["wrong"]}'
            f'（読み直した全 {tot["n"]} 件では エラー {tot["error"]}・誤った値 {tot["wrong"]}）')


def v3(all_ok, none_ok=False, yes='一致', part='一部だけ再現', no='食い違い'):
    return yes if all_ok else (no if none_ok else part)


def refer(rows, claims):
    """The verdict of a row that only points at earlier rows: taken from those rows, not typed."""
    vs = [r[2] for r in rows if any(r[1].startswith(c) for c in claims)]
    if len(vs) != len(claims):
        return NA
    if all(v.startswith('一致') for v in vs):
        return vs[0] if len(set(vs)) == 1 else '一致'
    return '一部だけ再現（指した行のうち、一致でないもの：' + '、'.join(sorted({v for v in vs if not v.startswith('一致')})) + '）'


def comparison(d):
    rows = []
    T1 = '両方向に安全な変更は、既定値つきの追加と削除'

    def avro_add_del():
        a, b = _b1(d, 'avro', 'add_optional'), _b1(d, 'avro', 'del_optional')
        ok = all(A.b1_broken(x) == (0, 0) and not _rej(x) for x in (a, b))
        return v3(ok), (f'追加：{_rejs(a)}、旧→新のデータ {_t(a, "old_reads_new")}、新→旧 {_t(a, "new_reads_old")}。'
                        f'削除：{_rejs(b)}、{_t(b, "old_reads_new")}、{_t(b, "new_reads_old")}')
    safe(rows, T1, 'Avro：追加・削除は、既定値があれば両方向に安全', avro_add_del)

    def avro_nodefault():
        a, b = _b1(d, 'avro', 'add_required'), _b1(d, 'avro', 'del_required')
        ok = (a['traffic']['new_reads_old']['error'] > 0 and a['traffic']['old_reads_new']['ok'] == a['traffic']['old_reads_new']['n']
              and b['traffic']['old_reads_new']['error'] > 0 and b['traffic']['new_reads_old']['ok'] == b['traffic']['new_reads_old']['n'])
        return v3(ok), (f'既定値なしの追加：新しい受け手が旧いデータを読むと {_t(a, "new_reads_old")}、逆向きは {_t(a, "old_reads_new")}（{_rejs(a)}）。'
                        f'既定値なしの削除：旧い受け手が新しいデータを読むと {_t(b, "old_reads_new")}、逆向きは {_t(b, "new_reads_old")}（{_rejs(b)}）')
    safe(rows, T1, 'Avro：既定値の無い追加は後方互換を、既定値の無い削除は前方互換を壊す', avro_nodefault)

    def avro_rename():
        a, b = _b1(d, 'avro', 'rename'), _b1(d, 'avro', 'rename_noalias')
        lib = a['traffic']['new_reads_old']['ok'] == a['traffic']['new_reads_old']['n'] and a['traffic']['old_reads_new']['error'] > 0
        reg = a['registry']['BACKWARD']['v2']['accepted']
        verdict = '一致' if lib and reg else ('条件つき（読むライブラリは別名で読めたが、レジストリは BACKWARD で拒否した）' if lib else '食い違い')
        return verdict, (f'別名つき：新しい受け手（fastavro）が旧いデータを読むと {_t(a, "new_reads_old")}、旧い受け手が新しいデータを読むと {_t(a, "old_reads_new")}。'
                         f'Redpanda のレジストリは {_rejs(a)}。別名なし：{_t(b, "new_reads_old")}／{_t(b, "old_reads_new")}、{_rejs(b)}')
    safe(rows, T1, 'Avro：名前の変更は、別名に対応した実装なら後方互換。前方互換にはならない', avro_rename)

    def avro_type():
        a, b, c = _b1(d, 'avro', 'widen'), _b1(d, 'avro', 'narrow'), _b1(d, 'avro', 'retype')
        ok = (a['traffic']['new_reads_old']['ok'] == a['traffic']['new_reads_old']['n'] and b['traffic']['new_reads_old']['error'] > 0
              and c['traffic']['new_reads_old']['error'] > 0)
        return v3(ok), (f'int → long：新しい受け手が旧いデータを読むと {_t(a, "new_reads_old")}、旧い受け手が新しいデータを読むと {_t(a, "old_reads_new")}（{_rejs(a)}）。'
                        f'long → int：{_t(b, "new_reads_old")}（{_rejs(b)}）。整数 → 文字列：{_t(c, "new_reads_old")}（{_rejs(c)}）')
    safe(rows, T1, 'Avro：型の変更は、決まった昇格だけ（int から long など）', avro_type)

    def pb_add_del():
        a, b, c = _b1(d, 'protobuf', 'add_optional'), _b1(d, 'protobuf', 'del_optional'), _b1(d, 'protobuf', 'del_required')
        no_err = all(x['traffic'][k]['error'] == 0 and x['traffic'][k]['wrong'] == 0 and not _rej(x)
                     for x in (a, b, c) for k in ('old_reads_new', 'new_reads_old'))
        miss = c['traffic']['old_reads_new']['missing']
        verdict = ('条件つき（エラーも誤った値も無く登録も通るが、受け手が必ずあると見なしていたフィールドを消すと、既定値で黙って埋まった）'
                   if no_err and miss else v3(no_err))
        return verdict, (f'追加：{_rejs(a)}、{_t(a, "old_reads_new")}。任意のフィールドの削除：{_rejs(b)}、{_t(b, "old_reads_new")}。'
                         f'{A.change_ja("protobuf", "del_required").split(" ", 1)[1]}（消したのは、受け手が使っている status）：{_rejs(c)}、旧い受け手が新しいデータを読むと {_t(c, "old_reads_new")}')
    safe(rows, T1, 'Protocol Buffers：新しい番号での追加は安全。削除も安全（番号は reserved に）', pb_add_del)

    def pb_rename_type():
        a, b = _b1(d, 'protobuf', 'rename'), _b1(d, 'protobuf', 'narrow')
        ok = A.b1_broken(a) == (0, 0) and b['traffic']['new_reads_old']['wrong'] > 0
        return v3(ok), (f'名前の変更（同じ番号）：{_rejs(a)}、{_t(a, "old_reads_new")}。int64 → int32：{_rejs(b)}、新しい受け手が旧いデータを読むと {_t(b, "new_reads_old")}'
                        '（2^31 を超える値を 5 件に 1 件入れた）。JSON に直したときの名前の扱いは未検証')
    safe(rows, T1, 'Protocol Buffers：名前の変更はバイナリでは安全。型の変更は、読めても値が切り捨てられることがある', pb_rename_type)

    def pb_reuse():
        a = _b1(d, 'protobuf', 'reuse_number')
        w = a['traffic']['old_reads_new']['wrong'] + a['traffic']['new_reads_old']['wrong']
        return v3(w > 0, yes='一致（誤った値として再現。解析エラーは出なかった）'), (
            f'消した番号 5（note）に別の意味の文字列（email）を割り当てた：{_rejs(a)}。旧い受け手が新しいデータを読むと {_t(a, "old_reads_new")}、'
            f'新しい受け手が旧いデータを読むと {_t(a, "new_reads_old")}。旧い受け手の note に email の値が入った')
    safe(rows, T1, 'Protocol Buffers：番号・意味の再利用は、解析エラー、情報の漏えい、破損につながる', pb_reuse)

    def js_open_closed():
        a, b = _b1(d, 'json', 'add_optional'), _b1(d, 'json', 'closed_add')
        ok = A.b1_broken(a) == (0, 0) and b['traffic']['old_reads_new']['error'] > 0
        return v3(ok), (f'additionalProperties を書かないスキーマ：任意プロパティの追加は {_rejs(a)}、旧い受け手は {_t(a, "old_reads_new")}。'
                        f'additionalProperties: false のスキーマ：同じ追加が {_rejs(b)}、旧い受け手は {_t(b, "old_reads_new")}')
    safe(rows, T1, 'JSON Schema は、未知のプロパティを通すか拒否するかで判定が変わる', js_open_closed)

    def three_formats():
        n = {f: sum(1 for r in d.b1 if r['format'] == f) for f in S.FORMATS}
        rej = sum(1 for r in d.b1 if not r['registry']['DEFAULT']['v2']['accepted'])
        brk = sum(1 for r in d.b1 if A.b1_broken(r) != (0, 0))
        return '実測', (f'変更の数：Avro {n["avro"]}、Protocol Buffers {n["protobuf"]}、JSON Schema {n["json"]}（計 {len(d.b1)}）。レジストリの既定のままで登録が拒否された変更 {rej}、'
                        f'検査なしで配ると受け手がどちらかの向きで壊れた変更 {brk}。形式ごとの表は B-1 の節')
    safe(rows, T1, '「題材 B で実測する：この変更を 3 形式で流し、登録の拒否と、壊れた受け手を数える」', three_formats)

    T2 = '互換性モードは、向きと比べる範囲の 2 軸で選ぶ'

    def writer_first():
        bad = [r for r in d.b1 if r['registry']['BACKWARD']['v2']['accepted'] and r['traffic']['old_reads_new']['n'] != r['traffic']['old_reads_new']['ok']]
        names = '、'.join(f'{A.FMT_JA[r["format"]]} {A.change_ja(r["format"], r["change"]).split(" ", 1)[1]}' for r in bad)
        return v3(len(bad) > 0), f'BACKWARD で登録が通った変更のうち、旧い受け手が新しいデータを正しく読めなかったもの {len(bad)} 件：{names}'
    safe(rows, T2, 'BACKWARD のまま書き手を先に出すと、古い読み手が読める保証は無い', writer_first)

    def seven_modes():
        acc = {m: sum(1 for r in d.b1 if r['registry'][m]['v2']['accepted']) for m in ['NONE'] + B12_MODES}
        same = all(r['registry'][m]['v2']['accepted'] == r['registry'][m + '_TRANSITIVE']['v2']['accepted']
                   for r in d.b1 for m in ('BACKWARD', 'FORWARD', 'FULL'))
        return '実測', ('Redpanda は 7 つのモードをすべて受け付けた。登録が通った変更の数（' + f'{len(d.b1)} 件中）：'
                        + '、'.join(f'{m} {n}' for m, n in acc.items())
                        + f'。版が 2 つだけの B-1 では、推移的なモードとそうでないモードの結果は{"同じ" if same else "違った"}（差が出るのは B-2）')
    safe(rows, T2, 'モードは向き（BACKWARD／FORWARD／FULL）と範囲（直前の版／すべての版）の組み合わせに NONE を足した 7 つ。Redpanda は同じ 7 つを持つ', seven_modes)

    T3 = '既定は直前の版とだけ比べ、古いデータは守らない'

    def default_mode():
        lv = sorted({r['registry_default']['compatibilityLevel'] for r in d.b1})
        return v3(lv == ['BACKWARD']), f'何も設定していないレジストリの GET /config の表示：{lv}（{len(d.b1)} 実行）'
    safe(rows, T3, 'Redpanda 内蔵（26.2）の既定は BACKWARD', default_mode)

    def hole():
        parts, allok = [], True
        for f in S.FORMATS:
            dflt = next(r for r in d.b2 if r['format'] == f and r['sequence'] == 'hole' and r['mode'] == 'DEFAULT')
            tr = next(r for r in d.b2 if r['format'] == f and r['sequence'] == 'hole' and r['mode'] == 'BACKWARD_TRANSITIVE')
            passed = all(s['accepted'] for s in dflt['steps'])
            v1 = dflt['rebuild_by_writer'].get(f'{dflt["reader"]}_reads_v1', {'error': 0, 'wrong': 0, 'missing': 0, 'n': 0})
            bad = v1['error'] + v1['wrong'] + v1['missing']
            stopped = not tr['steps'][2]['accepted']
            allok = allok and passed and bad > 0 and stopped and tr['rebuild_total']['ok'] == tr['rebuild_total']['n']
            parts.append(f'{A.FMT_JA[f]}：既定では 3 版とも{"通り" if passed else "は通らず"}、v3 の受け手の読み直しで v1 のデータ {v1["n"]} 件のうち エラー {v1["error"]}・誤った値 {v1["wrong"]}。'
                         f'BACKWARD_TRANSITIVE では v3 の登録が{"拒否され" if stopped else "通り"}、読み直しは {tr["rebuild_total"]["ok"]}/{tr["rebuild_total"]["n"]} 件が正しい')
        return v3(allok), '。'.join(parts) + '。末尾だけ読む受け手（最新の版のデータ）は、どの構成でも全件を正しく読めた（B-2 の表）'
    safe(rows, T3, '直前の版とだけ比べると、段ごとの検査は通るが、版 3 の読み手は版 1 のデータを読めない。すべての版を読むなら TRANSITIVE。「題材 B で実測する：先頭からの読み直しで落ちる件数」', hole)
    rows.append([T3, 'Confluent Schema Registry の既定は BACKWARD、Apicurio Registry は規則なし（スライドに「未検証」）', NA, '題材 B は Redpanda の内蔵レジストリだけで測った。Confluent と Apicurio は動かしていない'])

    T4 = '契約の強制は、書き手側と CI に置く'

    def no_enforce():
        n = sum(r['n_records'] for r in d.b1)
        rej = sum(1 for r in d.b1 if not r['registry']['DEFAULT']['v2']['accepted'])
        return '一部だけ実測', (f'B-1 の {len(d.b1)} 実行で、既定のモードなら登録を拒否される v2（{rej} 件の変更）も含め、書き手は別の subject（NONE）で得たスキーマ ID を付けて計 {n} 件を発行し、'
                              'ブローカーは 1 件も拒否しなかった。Redpanda のサーバー側のスキーマ ID 検証（企業版）、CI での検査、シリアライザの自動登録の停止は未検証')
    safe(rows, T4, 'レジストリが検査するのは登録のときだけ。ブローカーは通常、データの形を強制しない', no_enforce)
    rows.append(['データ契約は、スキーマに品質と条件を足した合意', '策定中とされる仕様（dprod-contracts v0.7）の本体は未確認', NA, '文献の確認で、実測の対象ではない'])

    T5 = 'expand/contract：非互換の変更を 3 段階に分ける'

    def expand():
        n, be = d.b3of('mig', 'n'), d.b3of('mig', 'be')
        nf = sum(r['write']['failed'] + r['read']['failed'] for r in n)
        bf = sum(r['write']['failed'] + r['read']['failed'] + r['read']['wrong'] for r in be)
        bm = sum(r['final']['value_mismatch'] + r['final']['lost_acked_write'] + r['final']['null_new_columns'] for r in be)
        return v3(nf > 0 and bf == 0 and bm == 0), (
            f'1 つのトランザクションで変える N：失敗した要求 {occ(r["write"]["failed"] + r["read"]["failed"] for r in n)}（書き込みと読み取りの合計。内訳は B-3 の表）、書き込みが止まった時間 {med([r["write"]["max_gap_ms"] for r in n], 0)} ms。'
            f'expand/contract の BE：失敗した要求と誤った読み取り {bf}（{len(be)} 試行）、最終状態の食い違い {bm}、書き込みが止まった時間 {med([r["write"]["max_gap_ms"] for r in be], 0)} ms、'
            f'所要時間は N {med([r["migration_s"] for r in n], 1)} 秒に対し BE {med([r["migration_s"] for r in be], 1)} 秒')
    safe(rows, T5, '新旧の両方を支える期間を置けば、どの版のアプリも動ける。「各段階の失敗の数は題材 B で測る」', expand)

    def expand_cost():
        nd, ob = d.b3of('mig', 'be_nodual'), d.b3of('mig', 'be_onebackfill')
        return '実測', (f'両方に書く期間を省くと：誤った読み取り {occ(r["read"]["wrong"] for r in nd)}、最終状態の値の食い違い {occ(r["final"]["value_mismatch"] for r in nd)} 行（失敗した要求は {sum(r["write"]["failed"] + r["read"]["failed"] for r in nd)}）。'
                        f'埋め戻しを 1 文で行うと：書き込みが止まった時間 {med([r["write"]["max_gap_ms"] for r in ob], 0)} ms（分割した BE は {med([r["write"]["max_gap_ms"] for r in d.b3of("mig", "be")], 0)} ms）。'
                        '縮める段階をやり残した状態は測っていない（未検証）')
    safe(rows, T5, '費用は、2 つの版を並べて保つ期間。縮める段階をやり残すと、始める前より悪い状態になりうる', expand_cost)

    T6 = 'PostgreSQL の DDL は、ロックと書き換えを調べる'

    def op(key):
        rs = d.b3of('ops', key)
        if not rs:
            raise KeyError(key)
        return rs

    def ops_fact(key):
        rs = op(key)
        return f'ロック {A._locks(rs[0])}、書き換え {"あり" if rs[0]["rewrite"] else "なし"}、書き込みの停止 {med([r["write"]["max_gap_ms"] for r in rs], 0)} ms、読み取りの停止 {med([r["read"]["max_gap_ms"] for r in rs], 0)} ms'

    def has_lock(key, mode, rel='orders_t'):
        return any(r == rel and m == mode for r, m in (op(key)[0]['locks_in_txn'] or op(key)[0]['locks_sampled']))

    def add_col():
        a, b, c = op('add_col_default'), op('add_col_volatile'), op('add_col')
        ok = not a[0]['rewrite'] and b[0]['rewrite'] and has_lock('add_col_default', 'AccessExclusiveLock')
        return v3(ok), f'定数の DEFAULT：{ops_fact("add_col_default")}。volatile な DEFAULT（random()）：{ops_fact("add_col_volatile")}。既定値なし：{ops_fact("add_col")}'
    safe(rows, T6, '列の追加：volatile でない DEFAULT は表を書き換えない（ロックは ACCESS EXCLUSIVE）。volatile な DEFAULT は表と索引を全部書き換える', add_col)

    def alter_type():
        a, b = op('alter_type'), op('alter_type_nowrite')
        return v3(a[0]['rewrite']), f'int → bigint：{ops_fact("alter_type")}。varchar(20) → text：{ops_fact("alter_type_nowrite")}'
    safe(rows, T6, '列の型の変更：通常は、表と索引を全部書き換える', alter_type)

    def constraints():
        ok = (has_lock('add_fk', 'ShareRowExclusiveLock') and has_lock('add_fk', 'ShareRowExclusiveLock', 'customers')
              and has_lock('validate_check', 'ShareUpdateExclusiveLock'))
        return v3(ok), (f'CHECK：{ops_fact("add_check")}。CHECK を NOT VALID で：{ops_fact("add_check_notvalid")}、その後の VALIDATE：{ops_fact("validate_check")}。'
                        f'外部キー：{ops_fact("add_fk")}。外部キーの VALIDATE：{ops_fact("validate_fk")}。SET NOT NULL：{ops_fact("set_not_null")}。'
                        f'NOT NULL を NOT VALID で：{ops_fact("add_notnull_notvalid")}、その VALIDATE：{ops_fact("validate_notnull")}')
    safe(rows, T6, '制約の追加は既存の行を走査し、その間、更新は待たされる。外部キーのロックは SHARE ROW EXCLUSIVE（参照先にも）。NOT VALID で足し、あとで VALIDATE（SHARE UPDATE EXCLUSIVE）', constraints)

    def index():
        a, b = op('create_index'), op('create_index_conc')
        wa, ra = med([r['write']['max_gap_ms'] for r in a], 0), med([r['read']['max_gap_ms'] for r in a], 0)
        ok = has_lock('create_index', 'ShareLock') and has_lock('create_index_conc', 'ShareUpdateExclusiveLock')
        return v3(ok), f'通常の作成：{ops_fact("create_index")}（書き込み {wa} ms に対し読み取り {ra} ms）。CONCURRENTLY：{ops_fact("create_index_conc")}。失敗時に INVALID の索引が残ることは未検証'
    safe(rows, T6, '索引の通常の作成は、書き込みを止める（読み取りは可）。CONCURRENTLY で避ける。失敗時は INVALID が残る', index)

    def queue():
        a, b, c = d.b3of('queue', 'plain'), d.b3of('queue', 'long_txn'), d.b3of('queue', 'long_txn_timeout')
        stall = min(r['write']['max_gap_ms'] for r in b)
        return ('未確認だった点の実測' if stall > 3000 else '未確認だった点の実測（待ちは短かった）'), (
            f'表を読んだだけのトランザクションを {B3.HOLD_S:.0f} 秒開いたまま、列の追加を実行。lock_timeout なし：DDL は {med([r["ddl"]["seconds"] for r in b], 2)} 秒待ち、その間、'
            f'書き込みが {med([r["write"]["max_gap_ms"] for r in b], 0)} ms、読み取りが {med([r["read"]["max_gap_ms"] for r in b], 0)} ms 止まり、'
            f'時間切れで失敗した要求は 書き込み {occ(r["write"]["failed"] for r in b)}・読み取り {occ(r["read"]["failed"] for r in b)}。'
            f'DDL の後ろに、あとから来た読み書きが並んだ（ロック待ちのセッション最大 {med([r["lock_waits"]["max_waiting_sessions"] for r in b], 0)}）。'
            f'lock_timeout 200 ms＋再試行：DDL は {med([r["ddl"]["attempts"] for r in c], 0)} 回目で成功、書き込みの停止は {med([r["write"]["max_gap_ms"] for r in c], 0)} ms、'
            f'失敗した要求 {sum(r["write"]["failed"] + r["read"]["failed"] for r in c)}。長いトランザクションなし：停止 {med([r["write"]["max_gap_ms"] for r in a], 0)} ms')
    safe(rows, T6, 'lock_timeout の既定は 0 で、ロックは無期限に待つ。「長いトランザクションの後ろに DDL が並んだときに何が起きるかは、文書に明記が見つからなかったので、題材 B で測る」', queue)

    T7 = 'Kafka の既定は 7 日保持。削除の印は 1 日後に消えうる'

    def defaults():
        k = d.b5of('kafka', 'tombstone', 'default')[0]['topic_config_reported']
        ok = k.get('retention.ms') == '604800000' and k.get('delete.retention.ms') == '86400000' and k.get('retention.bytes') == '-1'
        return v3(ok), (f'Apache Kafka 4.4.0 が表示したトピックの値（cleanup.policy と セグメントの設定だけを指定）：retention.ms={k.get("retention.ms")}、'
                        f'retention.bytes={k.get("retention.bytes")}、delete.retention.ms={k.get("delete.retention.ms")}。cleanup.policy の既定（delete）は、指定して作ったので未検証')
    safe(rows, T7, 'Apache Kafka 4.4 の既定：retention.ms 604800000（7 日）、retention.bytes -1、delete.retention.ms 86400000（1 日）', defaults)

    def rp_defaults():
        t = d.b5of('redpanda', 'tombstone', 'default')[0]['topic_config_reported']
        r = d.b5of('redpanda', 'retention', 'default')[0]['topic_config_reported']
        return '未確認だった点の実測', (f'Redpanda v26.2.7 が表示したトピックの値：cleanup.policy={r.get("cleanup.policy")}、retention.ms={r.get("retention.ms")}、'
                                    f'retention.bytes={r.get("retention.bytes")}、delete.retention.ms={t.get("delete.retention.ms")}（compact のトピック）')
    safe(rows, T7, 'Redpanda など Kafka 互換の製品の既定値は、ここでは確かめていない', rp_defaults)

    def compaction():
        parts, ok = [], True
        for b in ('kafka', 'redpanda'):
            rs = d.b5of(b, 'compact')
            ok = ok and all(r['log_after']['offset_changed'] == 0 and r['log_after']['order_violations'] == 0 and r['final']['state_mismatch'] == 0 for r in rs)
            parts.append(f'{A.BROKER_JA[b]}：{rs[0]["events"]} 件が {med([r["log_after"]["records"] for r in rs], 0)} 件に（注文 {B5.N_ORDERS} 件）。残ったレコードのオフセットの変化 {sum(r["log_after"]["offset_changed"] for r in rs)}、'
                         f'順序の逆転 {sum(r["log_after"]["order_violations"] for r in rs)}、作り直した索引の状態の食い違い {sum(r["final"]["state_mismatch"] for r in rs)}、'
                         f'イベント数の集計で足りない数 {med([r["final"]["agg_deficit"] for r in rs], 0)}')
        return v3(ok), '。'.join(parts)
    safe(rows, T7, 'コンパクション：キーごとに少なくとも最後の値が残る。順序とオフセットは変わらない（途中の値は残らない）', compaction)

    def tomb():
        parts, ok = [], True
        for b in ('kafka', 'redpanda'):
            s, df = d.b5of(b, 'tombstone', 'short'), d.b5of(b, 'tombstone', 'default')
            ls, ld = sum(r['final']['slow']['deleted_left'] for r in s), sum(r['final']['slow']['deleted_left'] for r in df)
            ok = ok and ls > 0 and ld == 0
            parts.append(f'{A.BROKER_JA[b]}：保持を {B5.TOMBSTONE_MS} ms に縮めると、削除の印は {med([r["tombstones_gone_after_s"] for r in s], 0)} 秒後にログから消え、'
                         f'止めていた受け手に削除済みの注文が {occ(r["final"]["slow"]["deleted_left"] for r in s)} 件残った（各試行 {s[0]["deleted"]} 件を削除）。'
                         f'既定（{df[0]["topic_config_reported"].get("delete.retention.ms")}）のまま同じだけ待つと、印は{"消え" if any(r["tombstones_gone"] for r in df) else "残り"}、残った注文は {ld}')
        return v3(ok, yes='一致（保持を縮めた条件で再現）'), '。'.join(parts) + '。先頭から作り直した受け手に残った注文は全実行で ' + str(sum(r['final']['fresh']['deleted_left'] for r in d.b5 if r['case'] == 'tombstone'))
    safe(rows, T7, '最初から読む受け手が、delete.retention.ms のうちにログの末尾まで追いつかないと、削除の印を見落としうる（題材 B で実測する）', tomb)

    T8 = '作り直せるための 6 つの前提は、既定では成り立たない'

    def retention():
        lo, df = d.b5of('redpanda', 'retention', 'log_only'), d.b5of('redpanda', 'retention', 'default')
        k = d.b5of('kafka', 'retention', 'log_only')
        ok = all(r['final']['missing'] > 0 for r in lo) and all(r['final']['missing'] == 0 and r['final']['count_mismatch'] == 0 for r in df)
        return v3(ok, yes='一致（保持を 5 秒に縮めた条件で再現）'), (
            f'保持を {B5.RETENTION_MS} ms に縮めた Redpanda：先頭のオフセットが {med([r["log"]["low"] for r in lo], 0)} まで進み、残った最古から作り直すと、無い注文 {occ(r["final"]["missing"] for r in lo)}、'
            f'適用回数の食い違い {occ(r["final"]["count_mismatch"] for r in lo)} 注文、状態の食い違い {occ(r["final"]["state_mismatch"] for r in lo)}。'
            + (f'Apache Kafka：無い注文 {occ(r["final"]["missing"] for r in k)}、適用回数の食い違い {occ(r["final"]["count_mismatch"] for r in k)}。' if k else 'Apache Kafka：未検証。')
            + f'既定の保持のまま同じだけ待った Redpanda：無い注文 {sum(r["final"]["missing"] for r in df)}、食い違い {sum(r["final"]["count_mismatch"] + r["final"]["state_mismatch"] for r in df)}。7 日そのものは待っていない')
    safe(rows, T8, '① 保持期間が、作り直す範囲を覆う：崩れると、古いログが消えていて最初から読めない', retention)

    def snapshot():
        sp, sl = d.b5of('redpanda', 'retention', 'snap_pos'), d.b5of('redpanda', 'retention', 'snap_latest')
        bad = lambda rs: sum(r['final']['missing'] + r['final']['state_mismatch'] + r['final']['count_mismatch'] for r in rs)   # noqa: E731
        return v3(bad(sp) == 0 and bad(sl) > 0), (
            f'スナップショット＋記録したオフセット（{sp[0]["snapshot_offset"]}）からのログ：無い注文・食い違いの合計 {bad(sp)}（{len(sp)} 試行）。'
            f'位置を記録せず、スナップショット＋ログの末尾から：状態の食い違い {occ(r["final"]["state_mismatch"] for r in sl)} 注文、適用回数の食い違い {occ(r["final"]["count_mismatch"] for r in sl)} 注文')
    safe(rows, T8, '② 足りない分はスナップショット：ログ上の位置と対応しないと、続きを当てられない', snapshot)
    safe(rows, T8, '③ コンパクションの意味を知る：途中の値は残らない。期限を過ぎると削除の印も消えうる',
         lambda: (refer(rows, ['コンパクション：', '最初から読む受け手が']), '上の「コンパクション」と「削除の印」の行のとおり'))

    def schema_pre():
        bad = [r for r in d.b2 if r['sequence'] == 'hole' and r['mode'] == 'DEFAULT']
        return v3(all(r['rebuild_total']['n'] != r['rebuild_total']['ok'] for r in bad)), (
            '既定のモードで v3 まで登録できた列で、v3 の受け手が先頭から読み直すと：'
            + '、'.join(_v1(r) for r in bad)
            + '。Avro と JSON Schema は読めずに止まる型、Protocol Buffers は誤って読む型')
    safe(rows, T8, '④ スキーマに互換がある：崩れると、古いイベントを今のコードが読めず、再生が止まるか、誤って読む', schema_pre)
    rows.append([T8, '⑤ 処理が決定的：崩れると、流すたびに結果が変わる', NA, '題材 B の受け手は決定的な処理だけで、now()・乱数・処理時間の窓を使う処理は作っていない。同じ入力を 2 回流した差分も取っていない'])

    def side_effect():
        rs = d.b4of('be_norecon', 'crash_noidem')
        return '一部だけ実測', (f'外部への副作用（メールや決済）とゲートウェイは作っていない（未検証）。近い実測として、作り直しの途中で落ちて再開した冪等でない受け手は、'
                              f'同じイベントを 2 回適用した：適用回数の食い違い {occ(r["final"]["new"]["count_mismatch"] for r in rs)} 注文、余分な適用 {occ(r["final"]["new"]["count_surplus"] for r in rs)} 回')
    safe(rows, T8, '⑥ 外部への副作用を止められる：崩れると、再生で二重に実行する', side_effect)

    T9 = '導出ストアは、新旧を並べて作り、照合して切り替える'

    def swap():
        n, be = d.b4of('n', 'clean'), d.b4of('be', 'clean')
        bad_n = sum(A._reads(r, 'not_found') + A._reads(r, 'regress') for r in n)
        bad_be = sum(A._reads(r, 'not_found') + A._reads(r, 'regress') + A._reads(r, 'error') for r in be)
        ok = bad_n > 0 and bad_be == 0 and all(r['outcome'] == 'switched' and r['final']['new']['state_mismatch'] == 0 for r in be)
        part = ('一部だけ再現（並べて切り替える側でも、切り替えの途中に読み取りの異常が出た）'
                if bad_n > 0 and bad_be > 0 and all(r['outcome'] == 'switched' and r['final']['new']['state_mismatch'] == 0 for r in be) else '一部だけ再現')
        return v3(ok, part=part), (f'一括（同じ場所で作り直す）：読み取りで見つからない {occ(A._reads(r, "not_found") for r in n)}、前より古い版 {occ(A._reads(r, "regress") for r in n)}。'
                        f'並べて切り替える：{sum(1 for r in be if r["outcome"] == "switched")}/{len(be)} 試行で切り替え、'
                        f'読み取りで見つからない {occ(A._reads(r, "not_found") for r in be)}、前より古い版 {occ(A._reads(r, "regress") for r in be)}、エラー {occ(A._reads(r, "error") for r in be)}、'
                        f'新のストアの最終状態の食い違い {sum(r["final"]["new"]["state_mismatch"] + r["final"]["new"]["count_mismatch"] + r["final"]["new"]["agg_mismatch"] for r in be)}')
    safe(rows, T9, '並べて切り替えれば、どの段階でも旧のビューに戻せる。一括で切り替えると、問題が出たとき戻れるシステムが無い', swap)

    def recon():
        bug, nr = d.b4of('be', 'bug'), d.b4of('be_norecon', 'bug')
        ok = all(r['outcome'] == 'aborted_by_reconciliation' for r in bug) and sum(r['final']['new']['state_mismatch'] for r in nr) > 0
        return v3(ok), (f'新しい受け手に欠陥を仕込むと、照合ありは {sum(1 for r in bug if r["outcome"] == "aborted_by_reconciliation")}/{len(bug)} 試行で切り替えを止め、'
                        f'旧のまま読み取りの異常は {sum(A._reads(r, "not_found") + A._reads(r, "regress") for r in bug)}。照合なしは切り替えてしまい、'
                        f'状態の食い違い {occ(r["final"]["new"]["state_mismatch"] for r in nr)} 注文、前より古い版の読み取り {occ(A._reads(r, "regress") for r in nr)}')
    safe(rows, T9, '照合する：件数と集計値を、新旧と元で比べ、検証が済んでから切り替える', recon)

    def swap_cost():
        be = d.b4of('be', 'clean')
        ab = [r['store_bytes'] for r in d.b4 if r['outcome'] == 'aborted_by_reconciliation' and 'idx_old' in r['store_bytes'] and 'idx_new' in r['store_bytes']]
        return '実測', (f'作り直しの時間 {med([r["rebuild_s"] for r in be], 2)} 秒（イベント {be[0]["n_preload"]:,} 件＋流れている分）、切り替えの全体 {med([r["swap_s"] for r in be], 1)} 秒。'
                        f'切り替えた {len(be)} 試行の新のストアは正解と一致（食い違い {sum(r["final"]["new"]["state_mismatch"] + r["final"]["new"]["count_mismatch"] for r in be)}）。'
                        f'並べている間の保存域（照合で止めた {len(ab)} 実行で、新旧を同じ時点で測った。新は列と集計表が多い）は、新が旧の、'
                        + '、'.join(f'{ja}の {n} 実行で {x}' for ja, n, x, _kib in A.store_groups(d))
                        + '。受け手の種類で 2 つの群に分かれ、間の値の実行は無いので、全体の中央値は書かない')
    safe(rows, T9, '払うもの：出力の保存域が一時的に 2 倍。「題材 B で実測する：導出ストアの差し替え、作り直しの一致率と所要時間」', swap_cost)

    T10 = 'マテリアライズドビューは、製品で意味が違う'

    def mv_pg():
        a = [r for r in d.b7 if r['product'] == 'pg' and r['case'] == 'refresh']
        b = [r for r in d.b7 if r['product'] == 'pg' and r['case'] == 'refresh_concurrently']
        ga, gb = min(r['read']['max_gap_ms'] for r in a), max(r['read']['max_gap_ms'] for r in b)
        ok = all(A.B7.n_diff(r['after_base_change']) > 0 and A.B7.n_diff(r['after_refresh']) == 0 for r in a + b) and ga > 3 * gb
        return v3(ok), (f'元の表を変えた後、REFRESH までビューの古い組は {med([A.B7.n_diff(r["after_base_change"]) for r in a + b], 0)}、REFRESH 後は {sum(A.B7.n_diff(r["after_refresh"]) for r in a + b)}。'
                        f'REFRESH（{med([r["refresh_ms"] for r in a], 0)} ms）の間、読み取りは {med([r["read"]["max_gap_ms"] for r in a], 0)} ms 止まった。'
                        f'CONCURRENTLY（{med([r["refresh_ms"] for r in b], 0)} ms）では {med([r["read"]["max_gap_ms"] for r in b], 0)} ms')
    safe(rows, T10, 'PostgreSQL 18：REFRESH で中身を完全に置き換える。次の REFRESH までの変更には追随しない。CONCURRENTLY は読み取りを妨げない（UNIQUE 索引が要る）。「題材 B で実測する：REFRESH 中の読み取りの待ち」', mv_pg)

    def mv_ch():
        ch = [r for r in d.b7 if r['product'] == 'ch']
        st = {s['step']: i for i, s in enumerate(ch[0]['steps'])}
        g = lambda step, k: med([A.B7.n_diff(r['steps'][st[step]][k]) for r in ch], 0)      # noqa: E731
        mn = lambda step, k: min(A.B7.n_diff(r['steps'][st[step]][k]) for r in ch)          # noqa: E731
        ok = (all(A.B7.n_diff(r['steps'][st['insert']][k]) == 0 for r in ch for k in ('incremental', 'incremental_join'))
              and mn('update_source', 'incremental') > 0 and mn('delete_source', 'incremental') > 0
              and mn('change_join_right', 'incremental_join') > 0 and mn('drop_partition', 'incremental') > 0
              and all(A.B7.n_diff(r['steps'][st['refresh']]['refreshable']) == 0 for r in ch))
        return v3(ok), (f'挿入の後：増分 MV の食い違い {g("insert", "incremental")}、結合つき {g("insert", "incremental_join")}。元の行の更新の後：{g("update_source", "incremental")} 店。'
                        f'削除の後：{g("delete_source", "incremental")} 店。結合の右側の更新の後：結合つき MV の食い違い {g("change_join_right", "incremental_join")} 地域。'
                        f'パーティションの削除の後：{g("drop_partition", "incremental")} 店。refreshable は更新の前 {g("insert_again", "refreshable")} 店、SYSTEM REFRESH VIEW の後 {g("refresh", "refreshable")}')
    safe(rows, T10, 'ClickHouse の増分 MV は挿入のトリガー：元の表の既存データの更新・削除・パーティション削除、結合の右側の変更は、ビューを変えない。refreshable は全体を計算し直す。「題材 B で実測する：増分 MV で元の行を更新・削除したときの不一致」', mv_ch)

    T11 = '削除は、導出データまで届いて完了する'

    def del_impl():
        rs = [r for r in d.b5 if r['case'] == 'tombstone']
        return v3(all(r['final']['nodel']['deleted_left'] == r['deleted'] and r['final']['fast']['deleted_left'] == 0 for r in rs)), (
            f'削除の印を受け取っても消す処理が無い受け手：削除した {sum(r["deleted"] for r in rs)} 件のうち {sum(r["final"]["nodel"]["deleted_left"] for r in rs)} 件が残った（{len(rs)} 実行）。'
            f'消す処理を持ち、止めなかった受け手：{sum(r["final"]["fast"]["deleted_left"] for r in rs)} 件')
    safe(rows, T11, '受け手ごとに実装が要る：削除イベントを受け取っても、消す処理が無ければ残る', del_impl)
    safe(rows, T11, '遅れた受け手は見落としうる：削除の印は、既定で 1 日を過ぎると消えうる',
         lambda: (refer(rows, ['最初から読む受け手が']), '上の「削除の印」の行のとおり。1 日そのものは待っていない'))

    def cdc():
        rs = d.b5of('redpanda', 'cdc_delete')
        ok = all(r['topic']['delete_events'] == r['deleted'] and r['topic']['tombstones'] == r['deleted'] and r['topic']['delete_followed_by_tombstone'] == r['deleted'] for r in rs)
        return v3(ok), (f'tombstones.on.delete を設定しないコネクタで {rs[0]["deleted"]} 行を DELETE：削除イベント {med([r["topic"]["delete_events"] for r in rs], 0)} 件、値が null のレコード {med([r["topic"]["tombstones"] for r in rs], 0)} 件、'
                        f'削除イベントの直後に同じキーの null が続いた組 {med([r["topic"]["delete_followed_by_tombstone"] for r in rs], 0)}。導出側に残った行 {sum(r["final"]["deleted_left"] for r in rs)}')
    safe(rows, T11, 'Debezium 3.7 の PostgreSQL コネクタは、既定で、削除イベントのあとに同じキーで値が null のトゥームストーンを出す', cdc)

    def del_time():
        rs = [r for r in d.b5 if r['case'] == 'tombstone']
        c = d.b5of('redpanda', 'cdc_delete')
        return '実測', (f'削除の印を直接流した場合（止めなかった受け手）：p50 {med([r["delete_latency_fast"]["p50_ms"] for r in rs], 0)} ms、p95 {med([r["delete_latency_fast"]["p95_ms"] for r in rs], 0)} ms（{len(rs)} 実行。100 件を 1 回で流し、受け手が 1 つのトランザクションで適用したので、p50 と p95 はほぼ同じになる）。'
                        + (f'PostgreSQL の DELETE から Debezium 経由：p50 {med([r["delete_latency"]["p50_ms"] for r in c], 0)} ms、p95 {med([r["delete_latency"]["p95_ms"] for r in c], 0)} ms（{len(c)} 実行。受け手は DELETE を終えてから読み始めたので、受け手の起動の時間を含む）。' if c else 'Debezium 経由は未検証。')
                        + f'取りこぼしは、止めなかった受け手で {sum(r["final"]["fast"]["deleted_left"] for r in rs)} 件')
    safe(rows, T11, '「題材 B で、削除が届くまでの時間と取りこぼしを測る」', del_time)
    rows.append([T11, '集計やモデルには効かない：行の単位では消せない。正から消して、導出を作り直す', NA, '集計表から削除済みの注文の分を引く処理は作っていない。学習済みのモデルは扱っていない'])
    rows.append(['キューは配ると消え、ログは残って読み直せる／窓はイベント時間で切り、遅れて届く分の扱いを決める', 'Kafka 4.2 からの share groups（公式文書の記述。未検証）／Flink 2.3 のウォーターマーク（公式文書の記述。未検証）', NA, '題材 B の対象外（share groups と Flink は動かしていない）'])

    T12 = '「PostgreSQL 互換」の幅は製品ごとに違う'
    rows.append([T12, 'Aurora DSQL・Spanner・CockroachDB の既定の分離レベルと使えない機能（公式文書の記述。未検証）', NA, '題材 B では PostgreSQL 18 だけを使った。CockroachDB の分離レベルごとの異常は題材 D の結果を参照。Aurora DSQL と Spanner は動かしていない'])
    safe(rows, '導出データの側は、作り直して入れ替えられる', 'どの段階からも戻れる。「題材 B で、導出ストアの差し替えと、ログからの再構築の一致率を実測する」',
         lambda: (refer(rows, ['並べて切り替えれば', '照合する：', '① 保持期間が', '② 足りない分は']),
                  '上の「導出ストアは、新旧を並べて作り、照合して切り替える」の行と、作り直しの前提 ①・② の行、B-4・B-5 の表のとおり'))

    T13 = '記録のシステムの入れ替えは、書籍の手順だけでは足りない'

    def sor():
        be, n = d.b6of('be', 'complete'), d.b6of('n', 'complete')
        lost = lambda rs: sum(r['final']['missing'] + r['final']['lost_acked_write'] for r in rs)       # noqa: E731
        return '実測', (f'6 段階を通した BE（{len(be)} 試行）：失敗した書き込み {occ(r["write_all"]["failed"] for r in be)}（遮断した旧へ書こうとした側）、切り替えで書き込みが止まった時間 {med([r["write_switch"]["max_gap_ms"] for r in be], 0)} ms、'
                        f'古い・無い読み取り {occ(r["read_all"]["wrong"] for r in be)}、最後の新に無い行・欠落した書き込み {lost(be)}。'
                        f'二重書き込みの N（{len(n)} 試行）：失敗した書き込み {occ(r["write_all"]["failed"] for r in n)}、古い・無い読み取り {occ(r["read_all"]["wrong"] for r in n)}、欠落 {occ(r["final"]["missing"] + r["final"]["lost_acked_write"] for r in n)}')
    safe(rows, T13, '「題材 B で実測する：新旧のアプリを混在させた各段階の失敗数、書き込みが止まった時間、再構築の一致率」', sor)

    def window_():
        be, nr, n = d.b6of('be', 'rollback'), d.b6of('be_norev', 'rollback'), d.b6of('n', 'rollback')
        lost = lambda rs: occ(r['final']['missing'] + r['final']['lost_acked_write'] for r in rs)       # noqa: E731
        ok = sum(r['final']['missing'] + r['final']['lost_acked_write'] for r in be) == 0 and sum(r['final']['missing'] + r['final']['lost_acked_write'] for r in nr) > 0
        return v3(ok), (f'書きを新へ切り替えた後で旧へ切り戻す：逆向きの複製あり（BE）は、旧に無い行・欠落した書き込み {lost(be)}（新だけに入っていた書き込み {med([r["acked_on_new_only"] for r in be], 0)} キー）。'
                        f'逆向きの複製なし：{lost(nr)}。二重書き込みの N：{lost(n)}')
    safe(rows, T13, '窓の間は、新に入った書き込みを旧へ戻す手段が要る（本ガイドの整理）', window_)

    T14 = '移行中の二重書き込みは Part 6 の問題を持ち込む'

    def one_side():
        n = [r for r in d.b6 if r['config'].startswith('n')]
        who = 'と、'.join(f'「{A.B6.CONFIG_JA[c]}」の {sum(1 for r in n if r["config"] == c)} 試行' for c in A.B6.CONFIG_JA if any(r['config'] == c for r in n))
        return v3(sum(r['dual_new_failed'] for r in n) > 0), (
            f'二重書き込みの間に新のインスタンスを 1 回再起動すると、新への書き込みの失敗が {occ(r["dual_new_failed"] for r in n)}（{who}の合計）。ほかに、新にまだ行が無くて当たらなかった更新が {occ(r["dual_new_missed"] for r in n)}'
            '（埋め戻しの前から二重書き込みを始めたため。どちらも要求は成功として応答した）。'
            f'論理レプリケーションの BE は、同じ再起動の後、照合の時点の食い違い {sum(r["steps"]["diff_after_backfill"] for r in d.b6 if r["config"].startswith("be"))}')
    safe(rows, T14, '片方だけ成功する：旧への書き込みは成功し、新は失敗する。新に行が欠ける', one_side)
    rows.append([T14, '並行更新が逆順に届く：同じ行への 2 つの更新が、旧と新で違う順に入る', NA, '題材 B の負荷は、1 つの行を 1 つの書き手だけが更新する作りで、この型は起きない。二重書き込みの順序の入れ替わりは題材 A の結果を参照'])

    def backfill():
        n, nv = [r for r in d.b6 if r['config'] == 'n'], [r for r in d.b6 if r['config'] == 'n_vbackfill']
        a, b = sum(r['steps']['diff_after_backfill'] for r in n), sum(r['steps']['diff_after_backfill'] for r in nv)
        pn, pv = a / max(1, len(n)), b / max(1, len(nv))
        xn, xv = sorted(r['steps']['diff_after_backfill'] for r in n), sorted(r['steps']['diff_after_backfill'] for r in nv)
        apart = bool(xn and xv) and xv[-1] < xn[0]
        if a > 0 and b == 0:
            verdict = '一致'
        elif a > 0 and apart:
            verdict = '一部だけ再現（版を比べる埋め戻しのほうが、どの試行でも少なかったが、0 にはならなかった）'
        elif a > 0:
            verdict = '一部だけ再現（埋め戻しの後に古い・無い行が残った。版を比べる埋め戻しでも残り、試行ごとの件数の範囲は重なった。減ったとは言えない）'
        else:
            verdict = '食い違い'
        return verdict, (f'上書きする埋め戻しの後、新が古いまま・無い行 {occ(xn)}（1 試行あたり {round(pn, 1)}、試行ごとの範囲 {xn[0]}〜{xn[-1]}）。'
                         f'版を比べて古ければ捨てる埋め戻し：{occ(xv)}（1 試行あたり {round(pv, 1)}、試行ごとの範囲 {xv[0]}〜{xv[-1]}）。'
                         '上書きによるものと、二重書き込みの更新が「新にまだ行が無い」ために当たらなかったものは、この数では分けられない。'
                         '版を比べても残る理由は調べていない。「無ければ入れる」は未検証')
    safe(rows, T14, '埋め戻しと新しい書き込みが競う：埋め戻しが、新しい値を古い値で上書きする。版を比べ古ければ捨てる（本ガイドの整理）', backfill)

    def seq():
        be = [r for r in d.b6 if r['config'].startswith('be')]
        ns = d.b6of('be_noseq', 'complete')
        seqs = sorted({r['steps']['sequence_before_switch']['pg2'] for r in be})
        uv = lambda rs: sum(r['write_all']['errors'].get('UniqueViolation:23505', 0) for r in rs)      # noqa: E731
        ok = seqs == [1] and uv(ns) > 0 and uv(d.b6of('be', 'complete')) == 0
        return v3(ok, yes='一致（シーケンスについて。DDL は未検証）'), (
            f'切り替えの直前、新のシーケンスの値は {seqs}（旧は {med([r["steps"]["sequence_before_switch"]["pg"] for r in be], 0)}、新の主キーの最大は {med([r["steps"]["max_id_before_switch"]["pg2"] for r in be], 0)}）。'
            f'シーケンスを進めずに切り替えると、主キーの重複で失敗した挿入 {occ(r["write_all"]["errors"].get("UniqueViolation:23505", 0) for r in ns)}。進めた BE は {uv(d.b6of("be", "complete"))}。'
            'スキーマは新旧とも手で作った。DDL が複製されないこと自体は試していない')
    safe(rows, T14, '複製が運ばないものがある：PostgreSQL 18 の論理レプリケーションは、DDL とシーケンスの値を運ばない。切り替えの前にシーケンスを進める', seq)
    rows.append(['乗り換えやすさは、EU では法律の要求でもある', '改正案（Digital Omnibus）の中身は未確認。法の解釈', NA, '法の要求は実測の対象ではない'])
    return rows


def ablation(d):
    rows = []

    def add(name, what, fn):
        try:
            rows.append([name, what, fn()])
        except (KeyError, IndexError, TypeError, StopIteration):
            rows.append([name, what, NA])
    add('BE − 推移的な検査（B-2）', '既定の BACKWARD に戻す', lambda: '、'.join(
        f'{A.FMT_JA[r["format"]]}：v3 が登録され、読み直しで エラー {r["rebuild_total"]["error"]}・誤った値 {r["rebuild_total"]["wrong"]}'
        for r in d.b2 if r['sequence'] == 'hole' and r['mode'] == 'DEFAULT') + '（BE は v3 を拒否し、どちらも 0）')
    add('BE − 冪等な受け手（B-4）', '作り直しの途中で落ちた受け手が、配られたものをすべて適用する', lambda: (
        f'照合なしで切り替えた場合：適用回数の食い違い {occ(r["final"]["new"]["count_mismatch"] for r in d.b4of("be_norecon", "crash_noidem"))} 注文、'
        f'集計の食い違い {occ(r["final"]["new"]["agg_mismatch"] for r in d.b4of("be_norecon", "crash_noidem"))} 状態（状態の食い違いは {sum(r["final"]["new"]["state_mismatch"] for r in d.b4of("be_norecon", "crash_noidem"))}）。'
        f'照合ありでは {sum(1 for r in d.b4of("be", "crash_noidem") if r["outcome"] == "aborted_by_reconciliation")}/{len(d.b4of("be", "crash_noidem"))} 試行で切り替えを止めた。'
        f'冪等な受け手は同じ障害で食い違い {sum(r["final"]["new"]["count_mismatch"] + r["final"]["new"]["state_mismatch"] for r in d.b4of("be", "crash"))}'))
    add('BE − 照合（B-4）', '作り終えたら、比べずに切り替える', lambda: (
        f'欠陥のある受け手で：状態の食い違い {occ(r["final"]["new"]["state_mismatch"] for r in d.b4of("be_norecon", "bug"))} 注文、前より古い版の読み取り {occ(A._reads(r, "regress") for r in d.b4of("be_norecon", "bug"))}。'
        f'欠陥が無ければ食い違い {sum(r["final"]["new"]["state_mismatch"] + r["final"]["new"]["count_mismatch"] for r in d.b4of("be_norecon", "clean"))}（照合を外した害は、欠陥があるときだけ出た）'))
    add('BE − スナップショット（B-5）', '保持期間を過ぎたログだけから作り直す', lambda: (
        f'無い注文 {occ(r["final"]["missing"] for r in d.b5of("redpanda", "retention", "log_only"))}、適用回数の食い違い {occ(r["final"]["count_mismatch"] for r in d.b5of("redpanda", "retention", "log_only"))} 注文'
        f'（スナップショット＋位置ありは {sum(r["final"]["missing"] + r["final"]["count_mismatch"] + r["final"]["state_mismatch"] for r in d.b5of("redpanda", "retention", "snap_pos"))}）'))
    add('BE − 遅れの監視（B-5、E-B3）', '止まっていた受け手を、そのまま再開する', lambda: (
        f'削除済みの注文が残った：{occ(r["final"]["slow"]["deleted_left"] for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "short")}。'
        f'監視は {sum(1 for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "short" and r["alarm"])}/{sum(1 for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "short")} 実行で警報を出し、'
        f'警報に従って先頭から作り直すと残りは {sum(r["final"]["fresh"]["deleted_left"] for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "short")}'))
    add('BE − 分割した埋め戻し（B-3、E-B1）', '全行を 1 文で埋め戻す', lambda: (
        f'書き込みが止まった時間 {med([r["write"]["max_gap_ms"] for r in d.b3of("mig", "be_onebackfill")], 0)} ms（BE は {med([r["write"]["max_gap_ms"] for r in d.b3of("mig", "be")], 0)} ms）。失敗した要求 {sum(r["write"]["failed"] + r["read"]["failed"] for r in d.b3of("mig", "be_onebackfill"))}'))
    add('BE − 両方に書く期間（B-3、E-B1）', '埋め戻しの後、旧版から新版へ直接切り替える', lambda: (
        f'誤った読み取り {occ(r["read"]["wrong"] for r in d.b3of("mig", "be_nodual"))}、最終状態の値の食い違い {occ(r["final"]["value_mismatch"] for r in d.b3of("mig", "be_nodual"))} 行'))
    add('BE − シーケンスを進める（B-6）', '新のシーケンスを初期値のまま切り替える', lambda: (
        f'主キーの重複で失敗した挿入 {occ(r["write_all"]["errors"].get("UniqueViolation:23505", 0) for r in d.b6of("be_noseq", "complete"))}'))
    add('BE − 遮断（B-6）', '旧への書き込みを止めずに、2 つの組を 0.7 秒ずらして切り替える', lambda: (
        f'失敗した書き込み {occ(r["write_all"]["failed"] for r in d.b6of("be_nofence", "complete"))}（主キーの重複 {sum(r["write_all"]["errors"].get("UniqueViolation:23505", 0) for r in d.b6of("be_nofence", "complete"))}）、'
        f'購読の適用エラー {occ(r["subscription_apply_errors"] for r in d.b6of("be_nofence", "complete"))}、最後の新に無い行・欠落 {occ(r["final"]["missing"] + r["final"]["lost_acked_write"] for r in d.b6of("be_nofence", "complete"))}。'
        f'切り替えで書き込みが止まった時間は {med([r["write_switch"]["max_gap_ms"] for r in d.b6of("be_nofence", "complete")], 0)} ms（遮断ありの BE は {med([r["write_switch"]["max_gap_ms"] for r in d.b6of("be", "complete")], 0)} ms）'))
    add('BE − 逆向きの複製（B-6）', '切り戻しの窓を持たずに書きを切り替え、後で旧へ戻す', lambda: (
        f'旧に無い行 {occ(r["final"]["missing"] for r in d.b6of("be_norev", "rollback"))}、欠落した応答済みの書き込み {occ(r["final"]["lost_acked_write"] for r in d.b6of("be_norev", "rollback"))}'
        f'（BE は {sum(r["final"]["missing"] + r["final"]["lost_acked_write"] for r in d.b6of("be", "rollback"))}）'))
    return rows


def normal_ops(d):
    rows = []

    def add(name, fn):
        try:
            rows.append([name, fn()])
        except (KeyError, IndexError, TypeError, StopIteration):
            rows.append([name, NA])
    normal = [r for r in d.b1 if r['intent'] == 'normal']
    add('B-1 互換性検査：正常な変更（既定値つきの追加・削除）が止められた数', lambda: '、'.join(
        f'{label} {sum(1 for r in normal if not r["registry"][m]["v2"]["accepted"])}/{len(normal)}'
        for label, m in (('既定のまま', 'DEFAULT'), ('BACKWARD_TRANSITIVE', 'BACKWARD_TRANSITIVE'), ('FORWARD', 'FORWARD'), ('FULL_TRANSITIVE', 'FULL_TRANSITIVE')))
        + '。止められたのは：' + ('、'.join(sorted({f'{A.FMT_JA[r["format"]]} {A.change_ja(r["format"], r["change"]).split(" ", 1)[1]}（{"・".join(_rej(r))}）' for r in normal if _rej(r)})) or 'なし'))
    add('B-2 推移的な検査：任意フィールドを 2 回足す列', lambda: '、'.join(
        f'{A.FMT_JA[r["format"]]} {sum(1 for s in r["steps"] if s["accepted"])}/3 版が登録でき、読み直しは {r["rebuild_total"]["ok"]}/{r["rebuild_total"]["n"]} 件が正しい'
        for r in d.b2 if r['sequence'] == 'benign' and r['mode'] == 'BACKWARD_TRANSITIVE'))
    add('B-3 expand/contract：正規の読み書き', lambda: (
        f'失敗した要求 {sum(r["write"]["failed"] + r["read"]["failed"] for r in d.b3of("mig", "be"))}、誤った読み取り {sum(r["read"]["wrong"] for r in d.b3of("mig", "be"))}（{len(d.b3of("mig", "be"))} 試行）。'
        f'所要時間は {med([r["migration_s"] for r in d.b3of("mig", "be")], 1)} 秒（1 つのトランザクションの N は {med([r["migration_s"] for r in d.b3of("mig", "n")], 1)} 秒）、DDL は 3 回に分かれ、アプリは 3 回配り直す'))
    add('B-3 lock_timeout＋再試行：DDL 自身の費用', lambda: (
        f'長いトランザクションの後ろでは DDL が {med([r["ddl"]["attempts"] for r in d.b3of("queue", "long_txn_timeout")], 0)} 回目で成功し（その前の {med([r["ddl"]["attempts"] - 1 for r in d.b3of("queue", "long_txn_timeout")], 0)} 回は lock_timeout で失敗）、終わるまで {med([r["ddl"]["seconds"] for r in d.b3of("queue", "long_txn_timeout")], 2)} 秒'
        f'（lock_timeout なしは {med([r["ddl"]["seconds"] for r in d.b3of("queue", "long_txn")], 2)} 秒で、1 回で成功）。その間に失敗した要求は {sum(r["write"]["failed"] + r["read"]["failed"] for r in d.b3of("queue", "long_txn_timeout"))}'))
    add('B-4 並べて切り替える：欠陥の無い切り替えが照合で止められた数', lambda: (
        f'{sum(1 for r in d.b4of("be", "clean") + d.b4of("be", "crash") if r["outcome"] != "switched")}/{len(d.b4of("be", "clean") + d.b4of("be", "crash"))} 試行'
        f'（欠陥なし {len(d.b4of("be", "clean"))} 試行＋受け手が 1 回落ちた {len(d.b4of("be", "crash"))} 試行。照合の回数は、1 回目の照合を含めて最大 {max(r["reconciliation"]["rounds"] for r in d.b4of("be", "clean") + d.b4of("be", "crash"))} 回）。'
        f'厳密な一致（1 つのスナップショットで新旧が完全に同じ）を条件にしていたら通らない：一致したスナップショットは、{_snaps(d)}。'
        f'切り替えの途中に前より古い版を読んだ読み取り：欠陥なし {occ(A._reads(r, "regress") for r in d.b4of("be", "clean"))}、受け手が 1 回落ちた場合 {occ(A._reads(r, "regress") for r in d.b4of("be", "crash"))}'))
    add('B-5 遅れの監視：既定の保持での誤った警報', lambda: (
        f'{sum(1 for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "default" and r["alarm"])}/{sum(1 for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "default")} 実行'))
    add('B-6 遮断つきの切り替え：正規の書き込みの失敗と停止', lambda: (
        f'失敗した書き込み {occ(r["write_all"]["failed"] for r in d.b6of("be", "complete"))}（遮断した旧へ、まだ切り替わっていない組が書こうとした分）、'
        f'書き込みが止まった時間 {med([r["write_switch"]["max_gap_ms"] for r in d.b6of("be", "complete")], 0)} ms（遮断しない切り替えは {med([r["write_switch"]["max_gap_ms"] for r in d.b6of("be_nofence", "complete")], 0)} ms、N は {med([r["write_switch"]["max_gap_ms"] for r in d.b6of("n", "complete")], 0)} ms）'))
    add('B-6 読みを先に新へ切り替える：非同期の複製による古い読み取り', lambda: (
        f'読みを切り替えてから書きを切り替えるまでの段階で、古い・無い読み取り {sum(r["read_by_phase"].get("4_read_new", {}).get("wrong", 0) for r in d.b6of("be", "complete") + d.b6of("be", "rollback"))}'
        f'／{sum(r["read_by_phase"].get("4_read_new", {}).get("ok", 0) for r in d.b6of("be", "complete") + d.b6of("be", "rollback"))} 読み取り'))
    return rows


def notes(d):
    """Unexpected results and observations; the numbers come from the result files."""
    out = ['## 期待どおりにならなかった結果・気づいた点\n']

    def add(fn):
        try:
            out.append('- ' + fn() + '\n')
        except (KeyError, IndexError, TypeError, StopIteration, ValueError, ZeroDivisionError):
            pass
    pb = [r for r in d.b1 if r['format'] == 'protobuf' and r['registry']['FULL_TRANSITIVE']['v2']['accepted']
          and sum(r['traffic'][k][c] for k in ('old_reads_new', 'new_reads_old') for c in ('wrong', 'missing')) > 0]
    add(lambda: (f'Protocol Buffers では、最も厳しい FULL_TRANSITIVE でも登録が通り、エラーも出ないのに、受け手の値が誤る・欠ける変更が {len(pb)} 件あった：'
                 + '、'.join(A.change_ja(r['format'], r['change']).split(' ', 1)[1] for r in pb)
                 + '。このうち Avro と JSON Schema にも同じ変更があるもの（' + ('、'.join(
                     A.change_ja('avro', r['change']).split(' ', 1)[1] for r in pb
                     if _b1(d, 'avro', r['change']) and _b1(d, 'json', r['change'])) or 'なし')
                 + '）は、Avro と JSON Schema では、FULL_TRANSITIVE で拒否されるか、受け手がエラーで止まった：'
                 + '、'.join(f'{A.FMT_JA[f]} {sum(1 for r in pb if _b1(d, f, r["change"]) and (not _b1(d, f, r["change"])["registry"]["FULL_TRANSITIVE"]["v2"]["accepted"] or sum(_b1(d, f, r["change"])["traffic"][k]["error"] for k in ("old_reads_new", "new_reads_old")) > 0))}'
                             f'/{sum(1 for r in pb if _b1(d, f, r["change"]))} 件' for f in ('avro', 'json'))
                 + '。同じ変更が Avro と JSON Schema に無いもの（番号の再利用など）は、比べていない'))
    add(lambda: ('Avro の別名（aliases）つきの名前の変更は、読む側のライブラリ（fastavro）では旧いデータを読めたが、Redpanda のレジストリは BACKWARD で拒否した（'
                 + _rejs(_b1(d, 'avro', 'rename')) + '）。レジストリが別名を互換性の判定に使っていないように見える。原因は調べていない'))
    add(lambda: ('JSON Schema で additionalProperties を書かないスキーマ（未知のプロパティを通す）に任意プロパティを足す変更は、Redpanda では ' + _rejs(_b1(d, 'json', 'add_optional'))
                 + '。同じ名前のプロパティを消してから別の型で足し直す列（B-2）は、既定のモードで 3 版とも通り、読み直しで落ちた。ほかのレジストリが同じ判定をするかは未検証'))
    add(lambda: (f'Redpanda が表示した delete.retention.ms の既定は {d.b5of("redpanda", "tombstone", "default")[0]["topic_config_reported"].get("delete.retention.ms")}、'
                 f'Apache Kafka は {d.b5of("kafka", "tombstone", "default")[0]["topic_config_reported"].get("delete.retention.ms")}。'
                 f'既定のままの実行では、どちらも、印が消えるかを約 {med([r["waited_s"] for r in d.b5 if r["case"] == "tombstone" and r["variant"] == "default"], 0)} 秒観測した間、削除の印は残っていた'
                 '（この観測の時間は、「削除の印」の表の「受け手を止めていた時間」とは別の量）'
                 '（この長さでは、Redpanda の既定で印がいつ消えるか・消えないかは分からない）'))
    add(lambda: (f'列の型を変えた後、読み手の要求が {sum(r["read"]["errors"].get("FeatureNotSupported:0A000", 0) for r in d.b3of("ops", "alter_type") + d.b3of("ops", "alter_type_nowrite"))} 件失敗した'
                 f'（SQLSTATE 0A000。{len(d.b3of("ops", "alter_type") + d.b3of("ops", "alter_type_nowrite"))} 実行の合計、読み手は 2 接続）。'
                 '原因は調べていない（エラーの文言は記録していない。接続を張り直した後は成功した）。推定：読み手の接続が持っていたプリペアド文の結果の型が変わった。'
                 f'表を書き換えない varchar(20) → text でも、読み取りが {med([r["read"]["max_gap_ms"] for r in d.b3of("ops", "alter_type_nowrite")], 0)} ms 止まった'
                 f'（書き込みは {med([r["write"]["max_gap_ms"] for r in d.b3of("ops", "alter_type_nowrite")], 0)} ms）。この差の原因は調べていない'))
    add(lambda: (f'流れている更新の下では、新旧のストアが 1 つのスナップショットで完全に一致することはほとんど無かった（欠陥の無い切り替えの照合で、一致したスナップショットは '
                 f'{_snaps(d)}）。'
                 '最初に作った照合（1 回でも完全に一致すれば合格）は、欠陥の無い切り替えも止めた（開発中に観測。結果のファイルには入れていない）。'
                 'そこで、2 回のスナップショットの両方で食い違い、かつ新が動いていない注文だけを数える形に変えた'))
    add(lambda: (f'少しずつ切り替える間、読み手が前より古い版を読むことがあった：欠陥なし {occ(A._reads(r, "regress") for r in d.b4of("be", "clean"))}、受け手が 1 回落ちた場合 {occ(A._reads(r, "regress") for r in d.b4of("be", "crash"))}。'
                 '0 件の回と 1 件以上の回があり、試行の数（各 5）では頻度は言えない。最終状態は正解と一致していた。原因は調べていない。'
                 '推定：新旧の追従の差を、読む先を行き来する読み手が見た。読む先を利用者ごとに固定する作りは試していない'))
    add(lambda: (f'版を比べる埋め戻しにしても、二重書き込みの構成では、埋め戻しの後に新が古い・無い行が {occ(r["steps"]["diff_after_backfill"] for r in d.b6 if r["config"] == "n_vbackfill")} 残り、'
                 f'移し切った後の新にも応答済みの書き込みの欠落が {occ(r["final"]["lost_acked_write"] for r in d.b6of("n_vbackfill", "complete"))} あった。'
                 '原因は調べていない。推定：埋め戻しが旧から行を読んだ後、新へ入れる前に来た更新は、新にまだ行が無いので当たらず（要求は成功として応答する）、その後に埋め戻しが古い版を入れる。この経路は切り分けていない'))
    add(lambda: ('論理レプリケーションの初期コピーは、購読を作った時点（並行稼働の段階）から始まり、埋め戻しの段階に入る前に終わっていた（表の「埋め戻しの時間」は、段階に入ってからの待ちで '
                 f'{med([r["steps"]["backfill_s"] for r in d.b6 if r["config"].startswith("be")], 2)} 秒）。初期コピーそのものの所要時間は測れていない（未検証）'))
    add(lambda: (f'遮断なしの切り替えで、応答済みの書き込みの欠落が出たのは {sum(1 for r in d.b6of("be_nofence", "complete") if r["final"]["lost_acked_write"] + r["final"]["missing"])}/{len(d.b6of("be_nofence", "complete"))} 試行だった。'
                 f'主キーの重複による挿入の失敗は {sum(1 for r in d.b6of("be_nofence", "complete") if r["write_all"]["errors"].get("UniqueViolation:23505", 0))}/{len(d.b6of("be_nofence", "complete"))} 試行で出た'
                 f'（{sum(r["write_all"]["errors"].get("UniqueViolation:23505", 0) for r in d.b6of("be_nofence", "complete"))} 件）。0.7 秒のずれと、1 つの行を 1 つの書き手だけが更新する負荷での値で、欠落が出るかどうかは試行によった'))
    add(lambda: (f'読みを新へ先に切り替えた段階（書きはまだ旧）で、BE の古い・無い読み取りは {sum(r["read_by_phase"].get("4_read_new", {}).get("wrong", 0) for r in d.b6 if r["config"].startswith("be"))} 件'
                 f'（{sum(r["read_by_phase"].get("4_read_new", {}).get("ok", 0) for r in d.b6 if r["config"].startswith("be"))} 読み取り）。非同期の複製なので古い読み取りは起きうる。この負荷と 1 台の上の遅れでの件数で、遅れを入れた条件は試していない'))
    add(lambda: ('REFRESH MATERIALIZED VIEW CONCURRENTLY は読み取りを止めなかったが、所要時間は通常の REFRESH の約 '
                 f'{round(median([r["refresh_ms"] for r in d.b7 if r.get("case") == "refresh_concurrently"]) / median([r["refresh_ms"] for r in d.b7 if r.get("case") == "refresh"]), 1)} 倍だった'))
    add(lambda: ('削除が届くまでの時間は、同じホストの時計で測った参考値。削除の印を直接流した実行では 100 件を 1 回で流して 1 つのトランザクションで適用し、Debezium の実行では受け手を DELETE の後に起動した。'
                 'どちらも、流れ続ける削除 1 件ずつの遅れではない'))
    return out


TAIL = """
## 見送り

- **オンラインスキーマ変更の専用の道具**（pg_repack、pgroll など）：入れていない。失うのは、表の書き換えが要る変更（列の型の変更など）を、書き込みを止めずに 1 つの操作で済ませる手段。この題材では、書き換えの要る変更を expand/contract（新しい列を足して移す）に置き換えた。見直す条件：expand/contract の 3 回の配り直しが回らないほど変更が多い、または表が大きく埋め戻しに日単位でかかる場合

## 限界

- 1 台の上の、1 ノードのブローカーと PostgreSQL。表は 5 万〜60 万行、イベントは 1 回の実行で 1〜2 万件と小さい。止まった時間や所要時間は、この機械・この大きさでの相対比較で、絶対値は一般化できない
- 保持の時間は縮めた（7 日 → 5 秒、1 日 → 3 秒）。掃除の間隔とセグメントの大きさも小さくし、毎秒 1 件の詰め物を流してセグメントを切らせた。既定の時間そのものは待っていない。「製品の既定のまま」の実行は、数十秒待って何も消えないことを見ただけである
- スキーマレジストリは Redpanda の内蔵のものだけ。受け手は fastavro・protobuf（Python、upb）・jsonschema で、ほかの言語の実装は試していない。書き込みの枠は「0x00＋スキーマ ID 4 バイト＋本体」を自前で付け、Confluent のシリアライザは使っていない（Protocol Buffers のメッセージ索引は付けていない）
- 「正常な変更」「非互換の変更」の区別は、ガイドの表に合わせて実験の前に決めた。実測の結果（どちらの向きで壊れたか）は B-1 の表に別の列で示した
- 受け手と負荷は Python のスレッドで、同じプロセスの中にある。受け手の「落ちる」は、DB にコミットした後、オフセットを記録せずに止める形で入れた（プロセスの強制終了ではない）
- B-3 の負荷は更新と 1 行の読み取りだけで、挿入は入っていない。アプリの時間切れは 3 秒、長いトランザクションは 6 秒に固定した
- B-4 の新旧のストアは同じ PostgreSQL の別の表で、別の製品への入れ替えではない。読み手の「前より古い版」は、同じ読み手が同じ注文について前に見た版より古い版を見た回数で、正との差（古さ）そのものは測っていない
- B-6 の新旧は同じ版の PostgreSQL 18 で、別の製品への移行ではない。1 つの行は 1 つの書き手だけが更新する。逆向きの複製は、順方向の購読を止めてから作った（双方向を同時には動かしていない）。切り戻した後の再移行は試していない
- B-6 の遮断は、アプリのロールから書き込みの権限を外す形で入れた。ネットワークの遮断や、旧のインスタンスの停止は試していない
- 「0 件」「防いだ」は、この手順と試行回数で起きなかったという意味で、起きないことの証明ではない

## 未検証

- Confluent Schema Registry と Apicurio Registry での互換性検査（Redpanda の内蔵レジストリだけで測った）
- Redpanda のサーバー側のスキーマ ID 検証（企業版）、CI での互換性検査、シリアライザの自動登録の停止
- Protocol Buffers を JSON に直したときの名前の変更、proto2 の required、Avro の union・enum・入れ子の変更
- 作り直しの前提 ⑤（処理の決定性）と ⑥（外部への副作用を止めるゲートウェイ）
- Kafka の既定の 7 日・1 日を実際に待った場合、ディスクが埋まるまでの挙動、階層化ストレージ
- 集計の表や学習済みのモデルからの削除（行の単位で消せない導出データ）
- CREATE INDEX CONCURRENTLY が失敗したときの INVALID の索引、expand/contract の縮める段階をやり残した状態
- 論理レプリケーションで DDL が複製されないこと自体（スキーマは最初から手で両方に作った）、「無ければ入れる」埋め戻し、並行更新が旧と新に逆順で届く型（題材 A を参照）
- 別の製品への記録のシステムの移行（PostgreSQL から CockroachDB など）、クラウドのマネージド製品の移行の道具
- 「PostgreSQL 互換」の製品ごとの幅（Aurora DSQL、Spanner）
- Kafka の share groups、Flink のウォーターマーク（ガイドの Part 12 が「未検証」と書いた点。題材 B の対象外）
"""


def sections(d):
    """Everything whose numbers come from the result files, rendered from one run."""
    out = ['## ガイドの記述との照合\n\n「ガイドの記述」の欄は、実測の結果を反映する前のガイド（コミット ad5dd6b）の文で、何と照合したかの記録である。「スライドの題」の欄は、いまのガイドの題に合わせてある。\n']
    out.append('ガイド `data-intensive-design-guide` の Part 7（deck/p07.js）、Part 12（deck/p12.js）、Part 15（deck/p15.js）の記述と、スライドが「題材 B で実測する」「未確認」「未検証」と書いた点を、'
               '下の結果と比べた。判定は結果のファイルの数から付けた。「一部だけ再現」は、記述のうち再現した点としなかった点が混じることを表し、中身は実測の列に書いた。\n')
    out.append(table(['スライドの題', 'ガイドの記述', '判定', '実測'], comparison(d)))
    for sec in (A.b1_section, A.b2_section, A.b3_section, A.b4_section, A.b5_section, A.b6_section, A.b7_section):
        out += sec(d)
    out.append('## 除去実験：重点を 1 つずつ外す\n')
    out.append(table(['外したもの', '外し方', '再び壊れたもの'], ablation(d)))
    out.append('## 正常な操作：基準と重点の費用\n')
    out.append('互換性検査や移行の手順のせいで、正規の変更・書き込み・読み取りが止められた、失敗した、遅れた数。\n')
    out.append(table(['見たもの', '実測'], normal_ops(d)))
    out += notes(d)
    return out


def main():
    d1, d = A.Data(1), A.Data(2)
    out = ['# 題材 B：置換と更新の演習（結果）\n',
           'このファイルは `cs/b/aggregate.py` が `results/b/*.jsonl`（2 回目の実行）と `results/b/run1/*.jsonl`（1 回目の実行）から作る。'
           '表の数は結果のファイルから数えたもので、手では書いていない。'
           '性能の数字は同じ環境の中での相対比較で、ほかの環境には一般化できない。\n',
           '問い：動かしたまま、スキーマを変え、導出ストアを入れ替え、ログから作り直せるか。構成は **N**（素朴な作り）、**B**（共通の基準：互換性検査は製品の既定のまま）、'
           '**BE**（基準＋題材 B の重点：推移的な検査、expand/contract、新旧を並べて照合してから切り替える、削除の印が消える前に受け手が追いつくことの監視）。\n',
           tworuns.LEGEND.format(t='b'),
           'B-1・B-2（スキーマの検査と読み直し）は決定的で、2 回とも同じ結果だった。B-3〜B-7 は、負荷をかけ続けながら操作する実験で、'
           '件数は操作の瞬間と並行の進み方に頼る。試行は 2〜5 回で、0 件と 1 件以上が 2 回の実行で入れ替わった行がある。\n']
    out += overview(d)
    out.append('実行の概要（実行の数、版、メモリ）は 2 回目のもの。1 回目も同じ実行 ID の集合で、監査の合格数も同じだった（`results/RERUN.md`）。\n')
    out += tworuns.merge(sections(d1), sections(d), COUNTS, 'topic B')
    out.append('## 判定器の監査\n')
    by = {}
    for r in d.audit:
        by.setdefault(r['judge'], []).append(r)
    out.append('誤りを仕込んだデータと、誤りの無いデータを判定器に流し、仕込んだ数だけを数えることを確かめた（`cs/b/audit.py`、結果は audit.jsonl）。\n')
    out.append(table(['判定器', 'ケースの数', '合格', 'ケース'], [[k, len(v), sum(1 for r in v if r['ok']), '、'.join(r['case'] for r in v)] for k, v in sorted(by.items())]))
    out.append(TAIL.strip() + '\n')
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, 'w', encoding='utf-8', newline='\n') as f:
        f.write('\n'.join(out).rstrip() + '\n')


if __name__ == '__main__':
    main()
