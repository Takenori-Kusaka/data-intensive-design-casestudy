"""Sections of results/c/summary.md (see aggregate.py). Everything is computed from the rows."""
from statistics import median

from . import c2_schema as C2, c3_retention as C3, c4_rerun as C4, c5_index as C5, c6_collector as C6, config as C
from . import queries as Q

NA = '未検証'
PROD_JA = {'pg': 'PostgreSQL（行）', 'ch': 'ClickHouse（列）', 'duck': 'DuckDB＋Parquet（ファイル）'}
PROD_S = {'pg': 'PostgreSQL', 'ch': 'ClickHouse', 'duck': 'DuckDB'}
PRODUCTS = ['pg', 'ch', 'duck']


def table(header, rows):
    out = ['| ' + ' | '.join(header) + ' |', '| ' + ' | '.join(['---'] * len(header)) + ' |']
    for r in rows:
        cells = [str(c).replace('|', '｜').replace('\n', ' ') for c in r]
        assert len(cells) == len(header) and all(c.strip() for c in cells), (header, r)
        out.append('| ' + ' | '.join(cells) + ' |')
    return '\n'.join(out) + '\n'


def fms(x):
    """Milliseconds with a sensible number of digits."""
    if x is None:
        return NA
    if x >= 10_000:
        return f'{x / 1000:.1f} 秒'
    if x >= 100:
        return f'{x:.0f} ms'
    if x >= 10:
        return f'{x:.1f} ms'
    return f'{x:.2f} ms'


def rng(xs, f=fms):
    xs = list(xs)
    if not xs:
        return NA
    if len(xs) == 1:
        return f(xs[0])
    return f'{f(median(xs))}（{f(min(xs))}〜{f(max(xs))}）'


def mb(b):
    return f'{b / 1e6:.1f} MB'


def times(a, b):
    return f'{a / b:.1f} 倍' if b else NA


def qmed(r, q, key='queries'):
    return median(r[key][q]['ms'])


def sel(rows, **kw):
    return [r for r in rows if all(r.get(k) == v for k, v in kw.items())]


def one(xs):
    """The single value all runs agree on, or the range."""
    s = sorted(set(xs))
    return str(s[0]) if len(s) == 1 else f'{s[0]}〜{s[-1]}'


# ====================================================================== C-1
def c1_section(d):
    out = ['## C-1 行指向・列指向・ファイル（同じデータ、同じ問い合わせ）\n',
           '同じ CSV を、PostgreSQL 18（ヒープの表。主キーは event_id、ts に B ツリー）、ClickHouse（MergeTree、ORDER BY (ts, event_id)）、'
           'DuckDB が書いた 1 つの Parquet ファイル（行は時刻の順、圧縮は既定）に入れた。規模は '
           + '、'.join(f'{k}＝{v:,} 行' for k, v in C.SCALES.items()) + f'。各 {C.TRIALS} 試行（試行ごとにコンテナとボリュームを作り直す）。'
           '取り込みは、サーバーが同じ CSV ファイルを直接読む形（PostgreSQL は COPY、ClickHouse は INSERT … SELECT FROM file()、'
           'DuckDB は read_csv から COPY … TO）。\n',
           '### 容量と取り込み時間\n']
    rows = []
    for sc in C.SCALES:
        for p in PRODUCTS:
            rs = sel(d.c1, scale=sc, product=p)
            if not rs:
                continue
            s0 = rs[0]['size']
            if p == 'pg':
                det = (f'COPY {rng(r["ingest"]["load_ms"] for r in rs)}、主キー {rng(r["ingest"]["pk_ms"] for r in rs)}、'
                       f'ts の索引 {rng(r["ingest"]["index_ts_ms"] for r in rs)}、VACUUM ANALYZE {rng(r["ingest"]["vacuum_analyze_ms"] for r in rs)}')
                size = f'{mb(s0["total_bytes"])}（表 {mb(s0["data_bytes"])}、索引 {mb(s0["index_bytes"])}）'
            elif p == 'ch':
                det = (f'INSERT {rng(r["ingest"]["load_ms"] for r in rs)}、OPTIMIZE FINAL {rng(r["ingest"]["optimize_final_ms"] for r in rs)}'
                       f'（パーツ {one(r["size"]["parts_before_optimize"] for r in rs)} → {one(r["size"]["parts"] for r in rs)}）')
                size = f'{mb(s0["total_bytes"])}（圧縮前 {mb(s0["uncompressed_bytes"])}）'
            else:
                det = f'CSV を読んで Parquet を書く 1 文（行グループ {one(r["size"]["row_groups"] for r in rs)}、{"・".join(s0["compression"])}）'
                size = mb(s0['total_bytes'])
            rows.append([sc, f'{rs[0]["n_rows"]:,}', mb(rs[0]['csv_bytes']), PROD_JA[p], size,
                         f'{s0["total_bytes"] / rs[0]["csv_bytes"]:.2f}', rng(r['ingest']['total_ms'] for r in rs), det, len(rs)])
    out.append(table(['規模', '行数', 'CSV', '製品', '容量', 'CSV に対する比', '取り込みの合計', '内訳', '試行'], rows))
    out.append('### 問い合わせ時間（暖機あり。ページキャッシュは捨てていない）\n')
    rows = []
    for sc in C.SCALES:
        for q in Q.QUERIES:
            cells = [sc, Q.QUERY_JA[q]]
            for p in PRODUCTS:
                rs = sel(d.c1, scale=sc, product=p)
                cells.append(rng(qmed(r, q) for r in rs) if rs else NA)
            rows.append(cells)
    out.append(table(['規模', '問い合わせ', PROD_JA['pg'], PROD_JA['ch'], PROD_JA['duck']], rows))
    n = sum(r['mismatch_total'] for r in d.c1)
    out.append(f'どの回も、結果を生成器の正解と突き合わせた（不一致 {n} 件）。時間はクライアントで測った壁時計の時間で、'
               'PostgreSQL と ClickHouse はネットワーク越し（psycopg、HTTP と JSON）、DuckDB は同じプロセスの中なので、'
               '短い問い合わせほど経路の差を含む。点の検索と範囲は、繰り返しごとにキーと窓を変えた。'
               'PostgreSQL の点の検索・範囲は索引を使い、ClickHouse の範囲は並び順（ts）を使う。ClickHouse と Parquet には event_id の索引は無い。\n')
    out.append('### 集計 1 本が読んだ量（店舗ごとの件数と金額：23 列のうち 2 列）\n')
    rows = []
    for sc in C.SCALES:
        for p in PRODUCTS:
            rs = sel(d.c1, scale=sc, product=p)
            if not rs or rs[0]['scan']['read_bytes'] is None:
                continue
            s = rs[0]['scan']
            how = {'pg': 'EXPLAIN (ANALYZE, BUFFERS) の共有バッファのブロック × 8 KB／表の大きさ',
                   'ch': '応答ヘッダーの read_bytes（圧縮前）／表の圧縮前の大きさ',
                   'duck': 'プロセスが OS に読み取りを頼んだバイト数（/proc/self/io の rchar）／ファイルの大きさ'}[p]
            rows.append([sc, PROD_JA[p], mb(s['read_bytes']), mb(s['stored_bytes']), f'{100 * s["read_bytes"] / s["stored_bytes"]:.1f}%', how])
    out.append(table(['規模', '製品', '読んだ量', '全体', '割合', '測り方'], rows))
    out.append('### 1 行ずつ書く／まとめて書く（' + one(r['rowwrite']['rows'] for r in d.c1) + ' 行）\n')
    rows = []
    for p, variants in (('pg', [('single', '1 文 1 行の INSERT（自動コミット）')]),
                        ('ch', [('single', '1 要求 1 行（既定のまま：async_insert=1、完了を待つ）'),
                                ('single_sync', '1 要求 1 行（async_insert=0）'),
                                ('single_nowait', '1 要求 1 行（wait_for_async_insert=0：書き込みを待たずに応答）')]),
                        ('duck', [('single', '1 行ごとに Parquet ファイルを 1 つ書く')])):
        rs = sel(d.c1, product=p)
        if not rs:
            continue
        batch = [r['rowwrite']['batch_ms'] for r in rs]
        for key, ja in variants:
            v = [r['rowwrite'][f'{key}_ms'] for r in rs]
            w = rs[0]['rowwrite']
            if p == 'ch':
                note = (f'直後のパーツ {one(r["rowwrite"][f"{key}_parts"] for r in rs)}（まとめては {one(r["rowwrite"]["batch_parts"] for r in rs)}）、'
                        f'応答の直後に読めた行 {one(r["rowwrite"][f"{key}_rows_at_return"] for r in rs)}、最後は {one(r["rowwrite"][f"{key}_rows_after"] for r in rs)} 行')
            elif p == 'duck':
                note = (f'ファイル {w["single_files"]} 個で {w["single_bytes"] / 1e3:.0f} KB（まとめては 1 個で {w["batch_bytes"] / 1e3:.0f} KB）。'
                        f'全部を数える問い合わせは {rng(median(r["rowwrite"]["single_scan_ms"]) for r in rs)}（まとめては {rng(median(r["rowwrite"]["batch_scan_ms"]) for r in rs)}）')
            else:
                note = f'最後は {one(r["rowwrite"]["rows_after"] for r in rs)} 行'
            rows.append([PROD_JA[p], ja, rng(v), rng(batch), times(median(v), median(batch)), note, len(rs)])
    out.append(table(['製品', '1 行ずつの書き方', '1 行ずつ', 'まとめて 1 回', '比', '補足', '実行'], rows))
    out.append('3 つの規模の実行（製品ごとに 15）を合わせた。書き込み先は、問い合わせの表とは別の空の表・ディレクトリ。\n')
    out.append('### 1 つの PostgreSQL で兼ねる：集計を並べたときの、キーの検索と挿入\n')
    rows = []
    for sc in C.SCALES:
        rs = sel(d.c1, scale=sc, product='pg')
        for k in map(str, (0, 1, 4)):
            if not rs:
                continue
            m = [r['mixed'][k] for r in rs]
            rows.append([sc, f'{rs[0]["n_rows"]:,}', k, rng(x['lookup']['p50_ms'] for x in m), rng(x['lookup']['p95_ms'] for x in m),
                         rng(x['insert']['p50_ms'] for x in m), rng(x['insert']['p95_ms'] for x in m),
                         rng((x['lookup']['n'] for x in m), lambda v: f'{v:.0f}'), sum(x['failed'] for x in m),
                         rng((x['analytic_queries_done'] for x in m), lambda v: f'{v:.0f}')])
    out.append(table(['規模', '行数', '並べた集計の本数', '検索 p50', '検索 p95', '挿入 p50', '挿入 p95', '検索の回数（6 秒）', '失敗', '終わった集計の回数'], rows))
    out.append('1 つの接続が、主キーの検索と 1 行の挿入を交互に 6 秒続け、その横で「店舗ごとの集計」を 0・1・4 本の接続が繰り返した。'
               'p50・p95 は 1 試行の中の値で、表は 5 試行の中央値と幅。CPU は 8 コア、PostgreSQL の並列ワーカーは既定（1 つの問い合わせに 2 つまで）。\n')
    return out


def _c1_ratio(d, q, a, b, sc):
    ra, rb = sel(d.c1, scale=sc, product=a), sel(d.c1, scale=sc, product=b)
    return median(qmed(r, q) for r in ra) / median(qmed(r, q) for r in rb)


# ====================================================================== C-2
CODE = {'stop': '止', 'unreadable': '読', 'quarantined': '保', 'row_missing': '落', 'null': 'N', 'defaulted': '0',
        'altered': '変', 'raw_text': '文', 'faithful': '○', 'field_absent': '無'}
PRIMARY_JA = {'stop': '取り込みで止まる', 'unreadable': '読み出しで落ちる', 'quarantined': '保留する',
              'silent': '黙って落とす・埋める・変える', 'query_error': '問い合わせで落ちる', 'query_wrong': '問い合わせが黙って違う値を返す',
              'ok': '揺れを保ったまま正しく読める'}


def drift_codes(r):
    """Short codes of what happened to the drifted rows (legend in the summary)."""
    if not r['ingest']['ok']:
        return ['stop']
    if r['counts']['drifted']['unreadable'] or (r['case'] == 'none' and r['counts']['clean']['unreadable']):
        return ['unreadable']
    g = r['counts']['drifted'] if r['case'] != 'none' else r['counts']['clean']
    out = []
    for k in ('quarantined', 'row_missing', 'null', 'defaulted', 'altered', 'raw_text', 'faithful'):
        if g[k]:
            out.append('field_absent' if k == 'null' and r['field_absent'] else k)
    return out


def drift_primary(r):
    codes = drift_codes(r)
    for k in ('stop', 'unreadable', 'quarantined'):
        if k in codes:
            return k
    if any(k in codes for k in ('row_missing', 'null', 'field_absent', 'defaulted', 'altered')):
        return 'silent'
    if not r['query']['ok']:
        return 'query_error'
    if not r['query']['equal'] or not r['query_avg']['equal']:
        return 'query_wrong'
    return 'ok'


def _qcode(q):
    return 'E' if not q['ok'] else ('＝' if q['equal'] else '≠')


def drift_cell(r):
    s = '・'.join(CODE[c] for c in drift_codes(r))
    s += f'｜合計{_qcode(r["query"])} 平均{_qcode(r["query_avg"])}'
    if r['query_tolerant']:
        s += f' 寛容{_qcode(r["query_tolerant"])}'
    return s


def clean_damage(r):
    return sum(v for k, v in r['counts']['clean'].items() if k not in ('faithful',))


def c2_section(d):
    out = ['## C-2 取り込み時スキーマと読み出し時スキーマ\n', '### 列に分けて入れる／JSON のまま入れる（容量・取り込み・問い合わせ）\n']
    ja = {'pg_cols': 'PostgreSQL：型つきの列', 'pg_jsonb': 'PostgreSQL：jsonb の 1 列', 'ch_cols': 'ClickHouse：型つきの列',
          'ch_json': 'ClickHouse：JSON 型の 1 列', 'duck_parquet': 'DuckDB：Parquet（型つき）', 'duck_ndjson': 'DuckDB：NDJSON を read_json で直接読む'}
    rows = []
    for v in C2.PERF_VARIANTS:
        rs = sel(d.perf, variant=v)
        if not rs:
            continue
        rows.append([ja[v], mb(rs[0]['size']['total_bytes']),
                     rng(r['ingest']['total_ms'] for r in rs) if v != 'duck_ndjson' else '取り込みなし（ファイルのまま）',
                     rng(qmed(r, 'agg_store') for r in rs), rng(qmed(r, 'agg_day') for r in rs), rng(qmed(r, 'filter_count') for r in rs),
                     sum(r['mismatch_total'] for r in rs), len(rs)])
    out.append(table(['持ち方', '容量', '取り込み', '集計（店舗ごと）', '集計（日ごと）', '絞り込み（顧客 1 人の件数と金額）', '不一致', '試行'], rows))
    p0 = d.perf[0] if d.perf else None
    if p0:
        out.append(f'{p0["n_rows"]:,} 行、23 列。入力は CSV {mb(p0["csv_bytes"])}、NDJSON {mb(p0["ndjson_bytes"])}（null の項目はキーごと省く）。'
                   'どれも索引なし（ClickHouse の型つきの列だけ、並び順が ts）。JSON の持ち方では、PostgreSQL は `(doc->>\'amount\')::int`、'
                   'ClickHouse は `doc.amount.:Int64`（型を指定した部分列）で読んだ。DuckDB の read_json は毎回ファイルを読んで型を判定する。\n')
    cj = sel(d.perf, variant='ch_json')
    if cj:
        s = cj[0]['size']
        out.append(f'ClickHouse の JSON 型は、取り込み時にパスごとの型を決める。文字列で送った ts は {"、".join(f"{k}（{v:,} 行）" for k, v in s["ts_dynamic_types"].items())} になり、'
                   f'`doc.ts.:String` で読めた行は {s["ts_as_string_nonnull"]:,} 行だった（エラーにはならず NULL が返る）。\n')
    out.append('### スキーマが揺れたとき、失敗はどこに出るか\n')
    r0 = d.drift[0] if d.drift else None
    if r0:
        out.append(f'{r0["n_rows"]:,} 行（7 項目）の NDJSON の、後ろの {C2.DRIFT_ROWS - C2.DRIFT_FROM:,} 行だけ形を変えた。取り込んだあと、'
                   '揺れた項目を読む人が 1 行ずつ何を得るかを、生成器の正解（上流が意味していた値）と比べた。'
                   '続けて、その項目の合計と平均を素直な問い合わせで取り、正解と比べた。JSON のまま持つ作りでは、'
                   '変換できない値を NULL にする寛容な問い合わせも試した。\n')
    out.append('記号（揺れた行の結果）：**止**＝取り込みがエラーで止まり、揺れていない行も入らない／**読**＝取り込みは通るが、その項目を読むとエラー／'
               '**保**＝保留の表に入り、本体には入らない／**落**＝行が黙って落ちる／**N**＝黙って NULL／**無**＝その項目の列が無く、黙って落ちる／'
               '**0**＝黙って 0 で埋まる／**変**＝黙って値が変わる（丸め）／**文**＝数値でない文字列のまま残る／**○**＝上流の意味どおりに読める。'
               '「｜」の後ろは問い合わせ：**＝** 正解と同じ、**≠** エラーなしで違う値、**E** エラー（寛容＝変換できない値を NULL にする問い合わせ）。\n')
    cases = [c for c in C2.CASES if c != 'none']
    for p in PRODUCTS:
        rows = []
        for pl in C2.PIPELINES[p]:
            cells = [f'{C2.PIPELINE_JA[f"{p}:{pl}"]}［{C2.KIND[pl]}］']
            for c in ['none'] + cases:
                r = sel(d.drift, product=p, pipeline=pl, case=c)
                cells.append(drift_cell(r[0]) if r else '対象外（CSV の列名は先頭行だけ）')
            rows.append(cells)
        out.append(f'**{PROD_S[p]}**\n')
        out.append(table(['取り込みの作り［種類］', '揺れなし'] + [C2.CASES[c]['ja'] for c in cases], rows))
    out.append('「止」のときは表が空（または無い）ので、集計は正解と違う値かエラーになる。\n')
    out.append('種類：N＝製品の既定のまま型つきで取り込む、skip＝製品の「エラーを飛ばす」指定、pass＝形を決めずにそのまま持つ・読む、'
               'BE＝検査した型つきの列＋未知のキーを入れる列＋保留の表（必須：event_id・ts・store_id・status・amount・distance_m。'
               '整数は JSON の整数だけを通す。delivery_fee は任意）。\n')
    out.append('#### 種類ごとの、失敗の出方（揺れのある 7 ケース × 取り込みの作り）\n')
    kinds = ['N', 'skip', 'pass', 'BE']
    prim = list(PRIMARY_JA)
    rows = []
    for k in kinds:
        rs = [r for r in d.drift if r['kind'] == k and r['case'] != 'none']
        rows.append([k, len(rs)] + [sum(1 for r in rs if drift_primary(r) == p) for p in prim]
                    + [sum(1 for r in rs if clean_damage(r) > 0)])
    out.append(table(['種類', '実行の数'] + [PRIMARY_JA[p] for p in prim] + ['揺れていない行まで欠けた実行'], rows))
    out.append('1 つの実行を、左の列から順に最初に当てはまる 1 つに数えた（止まる → 読み出しで落ちる → 保留 → 黙って … → 問い合わせで落ちる → '
               '問い合わせが黙って違う → 正しく読める）。「黙って違う値」は、合計か平均のどちらかが正解と違う場合。\n')
    out.append('#### 黙って変わった行・保留した行の数\n')
    rows = []
    for r in d.drift:
        if r['case'] == 'none':
            continue
        g = r['counts']['drifted']
        silent = g['row_missing'] + g['null'] + g['defaulted'] + g['altered']
        if r['ingest']['ok'] and (silent or g['quarantined']) and not g['unreadable']:
            rows.append([PROD_S[r['product']], C2.PIPELINE_JA[f'{r["product"]}:{r["pipeline"]}'], C2.CASES[r['case']]['ja'],
                         g['row_missing'], g['null'], g['defaulted'], g['altered'], g['quarantined'], clean_damage(r),
                         {'E': 'エラー', '＝': '正解と同じ', '≠': '違う値'}[_qcode(r['query'])],
                         {'E': 'エラー', '＝': '正解と同じ', '≠': '違う値'}[_qcode(r['query_avg'])]])
    out.append(table(['製品', '取り込みの作り', 'ケース', '落ちた行', 'NULL', '0 で埋まる', '値が変わる', '保留', '揺れていない行の被害',
                      '合計', '平均'], rows))
    return out


# ====================================================================== C-3
def c3_section(d):
    out = ['## C-3 圧縮・並び順・保持\n', f'### 並び順とコーデックを変えたときの容量（{C.SCALES[C3.SCALE]:,} 行）\n']
    rows = []
    seen = []
    for r in d.compress:
        k = (r['product'], r['order'], r['codec'])
        if k not in seen:
            seen.append(k)
    for p, o, c in seen:
        rs = sel(d.compress, product=p, order=o, codec=c)
        r0 = rs[0]
        top = '、'.join(f'{n} {mb(b)}' for n, b, _u in r0['top_columns'][:3]) or '列ごとの内訳なし（行の単位で持つ）'
        codec = {'none': '設定なし（ヒープ）', 'lz4': 'LZ4（既定）', 'zstd': 'ZSTD(3)' if p == 'ch' else 'ZSTD（水準は DuckDB の既定）',
                 'snappy': 'Snappy（既定）', 'uncompressed': '圧縮なし'}.get(c, c)
        rows.append([PROD_S[p] + ('＋Parquet' if p == 'duck' else ''), C3.ORDER_JA[o], codec,
                     mb(r0['bytes']) + (f'（別に ts の索引 {mb(r0["index_bytes"])}）' if r0['index_bytes'] else ''),
                     rng(r['write_ms'] for r in rs), rng(qmed(r, 'range_ts') for r in rs), rng(qmed(r, 'agg_store') for r in rs),
                     rng(qmed(r, 'agg_day') for r in rs), top, len(rs)])
    out.append(table(['製品', '並び順', 'コーデック', '容量', '書き込み', '範囲（10 分の窓）', '集計（店舗ごと）', '集計（日ごと）', '大きい列（圧縮後）', '試行'], rows))
    out.append('ClickHouse のコーデックは全列に同じものを指定した（既定は指定なし）。書き込みは OPTIMIZE FINAL までの時間。'
               f'結果の不一致は {sum(r["mismatch_total"] for r in d.compress)} 件。\n')
    out.append(f'### 古い 10 日分を落とす（30 日のうち。{C.SCALES[C3.SCALE]:,} 行）\n')
    rows = []
    for p in PRODUCTS:
        for m in C3.RETAIN[p]:
            rs = sel(d.retain, product=p, method=m)
            if not rs:
                continue
            steps = []
            for i, st in enumerate(rs[0]['steps']):
                steps.append(f'{st["step"]}：{rng(r["steps"][i]["ms"] for r in rs)} → {mb(median(r["steps"][i]["bytes_after"] for r in rs))}')
            extra = ''
            if p == 'ch':
                extra = f'。使わなくなったパーツがディスクに {mb(median(r["inactive_bytes"] for r in rs))} 残っていた（測定の時点）'
            rows.append([PROD_S[p], C3.RETAIN_JA[f'{p}:{m}'], mb(median(r['bytes_before'] for r in rs)), '／'.join(steps) + extra,
                         rng(r['total_ms'] for r in rs), mb(median(r['bytes_final'] for r in rs)),
                         sum(r['check']['mismatch'] for r in rs), len(rs)])
    out.append(table(['製品', '落とし方', '前の容量', '段ごとの時間 → その後の容量', '時間の合計', '最後の容量', '残った行の不一致', '試行'], rows))
    out.append('「残った行の不一致」は、残った行数と金額の合計を、生成器の正解（11 日目以降）と比べた結果。'
               'ClickHouse の容量は有効なパーツだけを数えた。TTL は now() ではなく固定の境目で表した式にした（壁時計に頼らないため）。\n')
    return out


# ====================================================================== C-4
def _j(j):
    return f'重複 {j["duplicates"]}・欠落 {j["missing"]}・古い値 {j["stale"]}・余分 {j["extra"]}'


def _jr(rs, key):
    """Counts over trials: one value, or the range when trials differ."""
    return '・'.join(f'{ja} {one(r[key][k] for r in rs)}' for k, ja in (('duplicates', '重複'), ('missing', '欠落'), ('stale', '古い値'), ('extra', '余分')))


def c4_section(d):
    out = ['## C-4 同じ範囲のバッチをもう一度流す\n',
           f'1 日分 {C4.DAY_ROWS:,} 行のバッチ。1 日目が入った表に、2 日目のバッチを 2 回流し、続けて 3 日目を 1 回流す。'
           f'訂正後の入力は、{C4.CHANGED} 行の金額が変わり、{C4.REMOVED} 行が消え、{C4.ADDED} 行が増えたもの。'
           '表にあるべき行（event_id と金額）は生成器から作り、SELECT で読んだ行と比べた。\n']
    rows = []
    for p in PRODUCTS:
        for st in C4.STRATEGIES[p]:
            for sc in C4.SCENARIOS:
                rs = sel(d.c4, product=p, strategy=st, scenario=sc)
                if not rs:
                    continue
                errs = [sum(1 for e in r['run_errors'][:2] if e) for r in rs]
                err = 'なし' if not any(errs) else f'2 回目が失敗（{one(errs)} 回）'
                var = ''
                if rs[0]['read_variants']:
                    var = '（3 日目の後：' + '、'.join(f'{ {"final": "FINAL つき", "optimized": "OPTIMIZE FINAL の後"}[k]} 誤り {one(r["read_variants"][k]["wrong"] for r in rs)}'
                                                  for k in rs[0]['read_variants']) + '）'
                rows.append([PROD_S[p], C4.STRATEGY_JA[f'{p}:{st}'],
                             C4.SCENARIO_JA[sc] if st != 'now_window' else '同じ処理を 6 時間後にもう一度',
                             err, _jr(rs, 'after_rerun'), _jr(rs, 'final') + var, one(r['day3_missing'] for r in rs), len(rs)])
    out.append(table(['製品', '流し方', '場面', '実行のエラー', '流し直した後の表', '3 日目を足した後の表', '3 日目の欠落', '試行'], rows))
    out.append('重複＝同じ event_id の 2 行目以降、欠落＝あるべき行が無い、古い値＝訂正前の金額のまま、余分＝あるべきでない行がある。'
               'ClickHouse は背景のマージに左右されるので 3 試行、ほかは決定的なので 1 試行。'
               '「主キーあり」の INSERT は 2 回目がエラーで全体が戻るので、表は 1 回目のまま（訂正と、止まった 1 回目の残りは入らない）。\n')
    return out


# ====================================================================== C-5
def c5_section(d):
    out = ['## C-5 索引の費用\n',
           f'索引を先に作ってから取り込み、続けて {C5.INSERT_ROWS:,} 行を {C5.INSERT_BATCH} 行ずつ挿入し、索引が効くはずの読み取りを測った。'
           f'PostgreSQL の表と ClickHouse・DuckDB は {C.SCALES[C5.SCALE]:,} 行、PostgreSQL の jsonb は {C5.G_ROWS:,} 件。\n']
    rows = []
    for p in PRODUCTS:
        for v in C5.VARIANTS[p]:
            rs = sel(d.c5, product=p, variant=v)
            if not rs:
                continue
            r0 = rs[0]
            reads = []
            for k, ja in (('filter_sum', '顧客 1 人の件数と金額'), ('filter_rows', '顧客 1 人の行（全列）'), ('key_exists', 'キーがある文書の数')):
                if k in r0['reads']:
                    reads.append(f'{ja} {rng(qmed(r, k, "reads") for r in rs)}')
            plan = r0.get('plans') or {}
            ptxt = '；'.join(f'{ {"filter_sum": "絞り込み", "key_exists": "キーの有無"}[k]}：{" → ".join(v)}' for k, v in plan.items() if v)
            if p == 'ch':
                ptxt = f'絞り込みで読んだ行 {one(r["read_rows"] for r in rs)}'
            idx = mb(r0['index_bytes']) if p != 'duck' else '表と同じファイルの中'
            rows.append([PROD_S[p], C5.VARIANT_JA[f'{p}:{v}'], rng(r['load_ms'] for r in rs), rng(r['insert_ms'] for r in rs),
                         mb(r0['data_bytes']), idx, '／'.join(reads), ptxt or '記録なし', sum(r['mismatch_total'] for r in rs), len(rs)])
    out.append(table(['製品', '索引', '取り込み', f'{C5.INSERT_ROWS:,} 行の挿入', 'データ', '索引の容量', '読み取り', '実行計画など', '不一致', '試行'], rows))
    out.append('PostgreSQL の索引の容量は主キーを含む。DuckDB の「データ」はデータベースのファイル全体。挿入は、PostgreSQL が 1 文 100 行の INSERT（自動コミット）、'
               'ClickHouse が 100 行ずつの同期の INSERT（async_insert=0）、DuckDB がパラメーターつきの 1 文 100 行の INSERT。'
               '製品ごとにクライアントの経路が違うので、挿入の時間は同じ製品の中で索引の有無を比べる。\n')
    return out


# ====================================================================== C-6
def c6_section(d):
    out = ['## C-6 Collector の送信で、まだ失う場面（ガイド Part 13）\n',
           f'送り手が OTLP/HTTP で {C6.REQUESTS} 要求（1 要求 {C6.PER_REQUEST} 件のログ、計 {C6.REQUESTS * C6.PER_REQUEST:,} 件）を Collector へ送り、'
           f'Collector が下流（受信した ID を数える受け口）へ送る。障害は {C6.FAULT_FROM + 1}〜{C6.FAULT_TO} 番目の要求の間だけ入れた。'
           '送り手は再送しない。受け付け済み（HTTP 200）なのに下流に届かなかったログを「失った」と数える。'
           'Collector の再試行は 0.5〜1 秒おき、上限は 120 秒（「再試行の時間切れ」の場面だけ 3 秒）、1 回の送信の時間切れは 2 秒に縮めた。'
           'キューの大きさは既定のまま（「あふれ」の場面だけ 10 要求）。\n']
    rows = []
    for dz in C6.SCENARIOS:
        for sc in C6.SCENARIOS[dz]:
            rs = sel(d.c6, design=dz, scenario=sc)
            if not rs:
                continue
            j = [r['judge'] for r in rs]
            st = {}
            for r in rs:
                for k, v in r['status_counts'].items():
                    st.setdefault(k, []).append(v)
            stt = '、'.join(f'{"接続の失敗" if k == "0" else "HTTP " + k}：{one(v)} 件' for k, v in sorted(st.items()))
            lost_at = sorted({tuple(r['lost_requests_first_last']) for r in rs if r['lost_requests_first_last']})
            rows.append([C6.DESIGN_JA[dz], C6.SCENARIO_JA[sc], one(x['acked'] for x in j), one(x['rejected'] for x in j) + f'（{stt}）',
                         one(x['lost_acked'] for x in j) + (f'（要求 {"／".join(f"{a}〜{b}" for a, b in lost_at)}）' if lost_at else ''),
                         one(x['duplicates'] for x in j), len(rs)])
    out.append(table(['送信の設計', '場面', '受け付けたログ', '断ったログ（送り手への応答）', '受け付けたのに届かなかったログ', '重複して届いたログ', '試行'], rows))
    rows = []
    for dz in C6.SCENARIOS:
        rs = sel(d.c6, design=dz, scenario='normal')
        if rs:
            rows.append([C6.DESIGN_JA[dz], rng(r['delay_normal_ms']['p50_ms'] for r in rs), rng(r['delay_normal_ms']['p95_ms'] for r in rs), len(rs)])
    out.append('正常時に、送ってから受け口に届くまでの遅れ（要求ごと。送り手は前の要求の応答を待って次を送る）：\n')
    out.append(table(['送信の設計', 'p50', 'p95', '試行'], rows))
    out.append('Kafka の役は Redpanda（1 ノード、fsync あり）。「受け側は既定のまま」は、Kafka から読む側の Collector が、'
               '読んだメッセージに先に印を付け（message_marking.after=false）、自分のメモリのキューに入れて下流へ送る作り。'
               '保持の場面では、トピックを retention.ms=2000・segment.ms=1000 にし、止めている間は毎秒 1 件の詰め物を送ってセグメントを切らせた。'
               '強制終了は docker kill（SIGKILL）で、すぐ docker start した。\n')
    return out


# ============================================================== comparison
def comparison(d):
    rows = []

    def add(slide, claim, verdict, measured):
        rows.append([slide, claim, verdict, measured])

    def c1m(sc, p, q):
        return median(qmed(r, q) for r in sel(d.c1, scale=sc, product=p))

    def size(sc, p):
        return sel(d.c1, scale=sc, product=p)[0]['size']['total_bytes']
    scales = [s for s in C.SCALES if sel(d.c1, scale=s)]
    big = scales[-1]
    csvb = sel(d.c1, scale=big)[0]['csv_bytes']
    nbig = sel(d.c1, scale=big)[0]['n_rows']

    # ---- Part 5
    s = 'どちらも、書き込み・読み取り・空間のどれかを払う'
    add(s, '「題材 C で容量・取り込み時間・問い合わせ時間を同じ環境で測る。LSM と B ツリーは直接比べない」', '実測（LSM と B ツリーの比較は対象外）',
        f'{nbig:,} 行（CSV {mb(csvb)}）で、容量は ' + '、'.join(f'{PROD_S[p]} {mb(size(big, p))}' for p in PRODUCTS)
        + '。取り込みは ' + '、'.join(f'{PROD_S[p]} {rng(r["ingest"]["total_ms"] for r in sel(d.c1, scale=big, product=p))}' for p in PRODUCTS)
        + '。問い合わせは C-1 の表。B ツリーと LSM ツリーの比較は、設計のとおり測っていない')

    s = '行指向は 1 件の読み書き、列指向は大量の走査に合う'
    fr = {p: sel(d.c1, scale=big, product=p)[0]['scan'] for p in PRODUCTS}
    frac = {p: fr[p]['read_bytes'] / fr[p]['stored_bytes'] for p in PRODUCTS}
    add(s, '行指向では、少数の列のために行を丸ごと読む。列指向は、使う列だけを読める',
        '一致' if frac['pg'] > 0.9 and frac['ch'] < 0.2 and frac['duck'] < 0.2 else '食い違い',
        f'23 列のうち 2 列を使う集計が読んだ量（{big}）：PostgreSQL は表の {100 * frac["pg"]:.0f}%、ClickHouse は圧縮前の大きさの {100 * frac["ch"]:.1f}%、'
        f'DuckDB＋Parquet はファイルの {100 * frac["duck"]:.1f}%（測り方は製品ごとに違う。C-1 の表）')
    ts_lz4 = sel(d.compress, product='ch', order='ts', codec='lz4')
    rnd_lz4 = sel(d.compress, product='ch', order='random', codec='lz4')
    ts_z = sel(d.compress, product='ch', order='ts', codec='zstd')
    pq_un = sel(d.compress, product='duck', order='ts', codec='uncompressed')
    pq_sn = sel(d.compress, product='duck', order='ts', codec='snappy')
    pq_rnd = sel(d.compress, product='duck', order='random', codec='snappy')
    comp_ok = size(big, 'ch') < size(big, 'pg') and size(big, 'duck') < size(big, 'pg')
    txt = (f'CSV に対する容量（{big}）：PostgreSQL {size(big, "pg") / csvb:.2f}（索引を除くと {sel(d.c1, scale=big, product="pg")[0]["size"]["data_bytes"] / csvb:.2f}）、'
           f'ClickHouse {size(big, "ch") / csvb:.2f}、Parquet {size(big, "duck") / csvb:.2f}。')
    v = '一致' if comp_ok else '食い違い'
    if ts_lz4 and rnd_lz4 and ts_z and pq_un and pq_sn and pq_rnd:
        d_order = rnd_lz4[0]['bytes'] / ts_lz4[0]['bytes'] - 1
        d_order_pq = pq_rnd[0]['bytes'] / pq_sn[0]['bytes'] - 1
        txt += (f'並び順を乱数の列に変えると ClickHouse は {100 * d_order:+.0f}%、Parquet は {100 * d_order_pq:+.0f}%。'
                f'コーデックは、ClickHouse の LZ4 → ZSTD(3) で {mb(ts_lz4[0]["bytes"])} → {mb(ts_z[0]["bytes"])}、'
                f'Parquet の圧縮なし → Snappy で {mb(pq_un[0]["bytes"])} → {mb(pq_sn[0]["bytes"])}')
        if comp_ok and max(d_order, d_order_pq) < 0.2:
            v = '一致（列ごとの圧縮は効いた。このデータでは、並び順による差は 2 割未満だった）'
    add(s, '列ごとにまとめて置く。並べた列は圧縮がよく効く', v, txt)
    pg_point_best = all(c1m(sc, 'pg', 'point_pk') < min(c1m(sc, 'ch', 'point_pk'), c1m(sc, 'duck', 'point_pk')) for sc in scales)
    agg_q = ['agg_store', 'agg_day', 'topn']
    pg_agg_worst = [sc for sc in scales if all(c1m(sc, 'pg', q) > max(c1m(sc, 'ch', q), c1m(sc, 'duck', q)) for q in agg_q)]
    v = '一致' if pg_point_best and len(pg_agg_worst) == len(scales) else (
        '一部だけ再現（' + ('点の検索は全規模で行指向が短い' if pg_point_best else '点の検索は行指向が短いとは限らなかった')
        + f'。集計 3 本が全部、行指向で最も長かった規模は {"・".join(pg_agg_worst) or "なし"}）')
    add(s, '行指向はキーで少数の行を読み書きする問い合わせに、列指向は多数の行を走査する集計に向く', v,
        '点の検索（キーで 1 行）：' + '、'.join(f'{sc} は PostgreSQL {fms(c1m(sc, "pg", "point_pk"))}／ClickHouse {fms(c1m(sc, "ch", "point_pk"))}／DuckDB {fms(c1m(sc, "duck", "point_pk"))}' for sc in scales)
        + '。店舗ごとの集計：' + '、'.join(f'{sc} は {fms(c1m(sc, "pg", "agg_store"))}／{fms(c1m(sc, "ch", "agg_store"))}／{fms(c1m(sc, "duck", "agg_store"))}' for sc in scales)
        + '。キーでない列の絞り込みと範囲は C-1 の表（範囲は、索引のある PostgreSQL が最も短かった）')
    rw = {}
    for p in PRODUCTS:
        rs = sel(d.c1, product=p)
        rw[p] = median(r['rowwrite']['single_ms'] for r in rs) / median(r['rowwrite']['batch_ms'] for r in rs)
    chs = sel(d.c1, product='ch')
    rw_sync = median(r['rowwrite']['single_sync_ms'] for r in chs) / median(r['rowwrite']['batch_ms'] for r in chs)
    dk = sel(d.c1, product='duck')[0]['rowwrite']
    v = '一致' if rw['ch'] > 1 and rw['duck'] > 1 else '食い違い'
    if v == '一致' and rw['pg'] > 5:
        v = '一致（列指向とファイルで。行指向の PostgreSQL でも、1 行ずつはまとめて書くより遅かった）'
    add(s, '列指向は、1 行ずつの書き込みは非効率。ためてまとめて書く', v,
        f'{dk["rows"]} 行を 1 行ずつ書く時間は、まとめて 1 回に対して、ClickHouse {rw["ch"]:.0f} 倍（既定の async_insert=1 で完了を待つ）・{rw_sync:.0f} 倍（async_insert=0）、'
        f'Parquet {rw["duck"]:.0f} 倍（1 行 1 ファイル。容量は {dk["single_bytes"] / dk["batch_bytes"]:.0f} 倍）、PostgreSQL {rw["pg"]:.0f} 倍')
    add(s, '「題材 C で、PostgreSQL（行）・ClickHouse（列）・DuckDB＋Parquet（ファイル）の容量・取り込み時間・問い合わせ時間を測る」', '実測',
        f'3 つの規模 × 3 製品 × {C.TRIALS} 試行（{len(d.c1)} 実行）。6 本の問い合わせの結果は 3 製品とも生成器の正解に一致（不一致 {sum(r["mismatch_total"] for r in d.c1)} 件）')

    s = '1 つで足りるうちは分けない、も正当な判断'
    mx = {}
    for sc in scales:
        rs = sel(d.c1, scale=sc, product='pg')
        mx[sc] = {k: median(r['mixed'][k]['lookup']['p95_ms'] for r in rs) for k in ('0', '1', '4')}
    hit1 = [sc for sc in scales if mx[sc]['1'] > 1.5 * mx[sc]['0']]
    hit4 = [sc for sc in scales if mx[sc]['4'] > 1.5 * mx[sc]['0']]
    if hit4 and not hit1:
        v = f'一部だけ再現（集計を 4 本並べた {"・".join(hit4)} で、検索の p95 が 1.5 倍を超えた。1 本では、どの規模でも超えなかった）'
    elif hit1:
        v = f'一致（集計 1 本でも {"・".join(hit1)} で検索の p95 が 1.5 倍を超えた）'
    else:
        v = '再現せず（この規模と本数では、検索の p95 は 1.5 倍を超えなかった）'
    add(s, '1 つの RDB で兼ねると、大きな集計が OLTP の資源を使う。行指向のまま走査する', v,
        '主キーの検索の p95（集計なし → 1 本 → 4 本）：' + '、'.join(f'{sc} は {fms(mx[sc]["0"])} → {fms(mx[sc]["1"])} → {fms(mx[sc]["4"])}' for sc in scales)
        + f'。失敗した検索・挿入は {sum(r["mixed"][k]["failed"] for r in sel(d.c1, product="pg") for k in ("0", "1", "4"))} 件。集計は表の {100 * frac["pg"]:.0f}% を読んだ')
    add(s, '「兼ねてよい規模の線引きは DDIA にも無い。境目は題材 C の実測で示す」（データ量と問い合わせの形を変えて測る）',
        '一部だけ実測（3 つの規模と 6 つの形で差の出方は測った。分けるべき境目の線は、この規模では引けていない）',
        '行指向の PostgreSQL が店舗ごとの集計に要した時間：' + '、'.join(f'{sc}（{sel(d.c1, scale=sc)[0]["n_rows"]:,} 行）{fms(c1m(sc, "pg", "agg_store"))}' for sc in scales)
        + '。同じ集計の ClickHouse に対する比：' + '、'.join(f'{sc} {_c1_ratio(d, "agg_store", "pg", "ch", sc):.1f} 倍' for sc in scales)
        + '、DuckDB＋Parquet に対する比：' + '、'.join(f'{sc} {_c1_ratio(d, "agg_store", "pg", "duck", sc):.1f} 倍' for sc in scales)
        + f'。最大の規模でも表は {mb(sel(d.c1, scale=big, product="pg")[0]["size"]["data_bytes"])} で、コンテナのメモリ（{C.PG_MEMORY}）に収まる。メモリに収まらない規模は未検証')

    s = '索引は、読み取りを買い、書き込みと領域で払う導出データ'
    i0, i1, i3, ic = (sel(d.c5, product='pg', variant=v) for v in ('idx0', 'idx1', 'idx3', 'idx_cover'))
    if i0 and i1 and i3 and ic:
        m = lambda rs, k: median(r[k] for r in rs)  # noqa: E731
        rd = lambda rs, q: median(qmed(r, q, 'reads') for r in rs)  # noqa: E731
        load1, load3 = m(i1, 'load_ms') / m(i0, 'load_ms'), m(i3, 'load_ms') / m(i0, 'load_ms')
        ins1, ins3 = m(i1, 'insert_ms') / m(i0, 'insert_ms'), m(i3, 'insert_ms') / m(i0, 'insert_ms')
        buys = rd(i1, 'filter_sum') < rd(i0, 'filter_sum') / 2
        pays = load1 > 1.1 and i1[0]['index_bytes'] > i0[0]['index_bytes']
        v = '一致' if buys and pays else '一部だけ再現'
        if buys and pays and ins1 < 1.1:
            v = f'一致（一括の取り込みと領域で。{C5.INSERT_BATCH} 行ずつの挿入では、索引 1 本の差は 1 割未満だった）'
        add(s, '索引を足すと問い合わせは速くなり、書き込みは遅くなり、ディスクの領域も使う', v,
            f'PostgreSQL、{i0[0]["n_rows"]:,} 行。顧客 1 人の絞り込み：索引なし {fms(rd(i0, "filter_sum"))} → 1 本 {fms(rd(i1, "filter_sum"))}。'
            f'取り込み：{fms(m(i0, "load_ms"))} → 1 本 {fms(m(i1, "load_ms"))}（{load1:.2f} 倍）→ 3 本 {fms(m(i3, "load_ms"))}（{load3:.2f} 倍）。'
            f'{C5.INSERT_ROWS:,} 行の挿入：{fms(m(i0, "insert_ms"))} → {fms(m(i1, "insert_ms"))}（{ins1:.2f} 倍）→ {fms(m(i3, "insert_ms"))}（{ins3:.2f} 倍）。'
            f'索引の容量（主キーを含む）：{mb(i0[0]["index_bytes"])} → {mb(i1[0]["index_bytes"])} → {mb(i3[0]["index_bytes"])}（表は {mb(i0[0]["data_bytes"])}）')
        only = any('Index Only Scan' in x for x in ic[0]['plans']['filter_sum'])
        bigger = ic[0]['index_bytes'] > i1[0]['index_bytes'] and m(ic, 'load_ms') > m(i1, 'load_ms')
        faster = rd(ic, 'filter_sum') < 0.9 * rd(i1, 'filter_sum')
        v = '一致' if only and bigger and faster else ('一部だけ再現（索引だけで答える計画になり、領域と取り込み時間は増えた。読み取りの時間の差は 1 割未満で、この大きさでは見えなかった）'
                                                   if only and bigger else '食い違い')
        add(s, '一部の列を中に持つ索引は、一部の問い合わせに索引だけで答える。さらに領域を使い、書き込みも遅くなる', v,
            f'customer_id INCLUDE (amount, ts)：実行計画は {" → ".join(ic[0]["plans"]["filter_sum"])}。索引の容量 {mb(ic[0]["per_index_bytes"]["ix_customer_cover"])}'
            f'（列を持たない索引は {mb(i1[0]["per_index_bytes"]["ix_customer"])}）。取り込み {fms(m(ic, "load_ms"))}（同 {fms(m(i1, "load_ms"))}）。'
            f'件数と金額の読み取り {fms(rd(ic, "filter_sum"))}（同 {fms(rd(i1, "filter_sum"))}）')
        add(s, '索引を足しても消しても、DB の内容は変わらない', '一致' if sum(r['mismatch_total'] for r in d.c5) == 0 else '食い違い',
            f'C-5 の {len(d.c5)} 実行で、索引の有無にかかわらず、読み取りの結果は生成器の正解と一致（不一致 {sum(r["mismatch_total"] for r in d.c5)} 件）')
        dk0, dk1 = sel(d.c5, product='duck', variant='tbl0'), sel(d.c5, product='duck', variant='tbl_art')
        c0, c1 = sel(d.c5, product='ch', variant='skip0'), sel(d.c5, product='ch', variant='skip_bloom')
        if dk0 and dk1 and c0 and c1:
            add(s, '索引は全部には張らない。問い合わせの形を知っている人が、効くものを選ぶ', '実測',
                f'DuckDB の表に customer_id の索引を張っても、実行計画は {" → ".join(dk1[0]["plans"]["filter_sum"]) or "記録なし"} のままで、'
                f'ファイルは {mb(dk0[0]["data_bytes"])} → {mb(dk1[0]["data_bytes"])} に増えた（件数と金額 {fms(rd(dk0, "filter_sum"))} → {fms(rd(dk1, "filter_sum"))}、行の取得 {fms(rd(dk0, "filter_rows"))} → {fms(rd(dk1, "filter_rows"))}）。'
                f'ClickHouse の bloom_filter のスキップ索引は、読む行を {one(r["read_rows"] for r in c0)} → {one(r["read_rows"] for r in c1)} に減らし、'
                f'件数と金額は {fms(rd(c0, "filter_sum"))} → {fms(rd(c1, "filter_sum"))}、行の取得は {fms(rd(c0, "filter_rows"))} → {fms(rd(c1, "filter_rows"))}')
    pgb = sel(d.c1, scale=big, product='pg')
    add(s, '「索引を作り直す時間と、その間の問い合わせの遅さを、障害対応と移行の計画に入れておく」', '一部だけ実測（作る時間だけ。その間の問い合わせの遅さは未検証）',
        f'{nbig:,} 行の表で、主キーの作成 {rng(r["ingest"]["pk_ms"] for r in pgb)}、ts の索引の作成 {rng(r["ingest"]["index_ts_ms"] for r in pgb)}（取り込みの直後、ほかの負荷なし）')

    s = 'まとめ：買うものと払うものを、対で言えるようにする'
    add(s, '「列指向は走査と圧縮を買い、1 行ずつの書き込みで払う」「数字は題材 C の実測と、各製品の公式文書で補う」', '実測',
        '上の「行指向は 1 件の読み書き、列指向は大量の走査に合う」の 4 行と、C-1・C-3 の表。払うものとして測れたのは、1 行ずつの書き込みと、キーで 1 行を読む時間')
    add('計算と保存を分けた先に、開いた表形式がある', 'クラウドのデータウェアハウスとレイクハウス（スライドに「未検証」）。Snowflake の Iceberg の表への外部からの書き込みの可否は「未確認」', NA,
        '題材 C のファイルは、1 台の上の Parquet だけ。オブジェクトストレージ、Iceberg などの表形式、クラウドの製品は動かしていない')

    # ---- Part 4
    s = '1 つの DB の中で、表と文書を組み合わせられる'
    be = {c: sel(d.drift, product='pg', pipeline='be', case=c) for c in C2.CASES}
    if all(be.values()):
        add(s, '決まった構造の項目は列に置き、揺れる部分だけ jsonb の列に置く（スライドでは「架空の例（本ガイドでは未実行）」）', '実測',
            f'PostgreSQL の BE の作り（型つきの列＋未知のキーの jsonb＋保留の表）で、増えた項目（tip）は {be["field_added"][0]["counts"]["drifted"]["faithful"]:,} 行とも jsonb から読め、'
            f'消えた任意の項目は NULL で入り、型の違う行は {be["text_in_number"][0]["quarantined"]:,} 行とも保留に回った（C-2 の表）。スライドの例そのもの（注文の options 列と GIN）は動かしていない')
    g0, g1, g2 = (sel(d.c5, product='pg', variant=v) for v in ('jsonb_noidx', 'jsonb_gin_ops', 'jsonb_gin_path'))
    if g0 and g1 and g2:
        rd = lambda rs, q: median(qmed(r, q, 'reads') for r in rs)  # noqa: E731
        small = g2[0]['index_bytes'] < g1[0]['index_bytes']
        fast = rd(g2, 'filter_sum') <= rd(g1, 'filter_sum')
        ops_idx = any('ix_gin' in x for x in g1[0]['plans']['key_exists'])
        path_idx = any('ix_gin' in x for x in g2[0]['plans']['key_exists'])
        v = '一致' if small and fast and ops_idx and not path_idx else '一部だけ再現'
        add(s, 'GIN の既定の jsonb_ops はキーの存在演算子も使える。jsonb_path_ops は使えないが、通常は小さく、検索も速い', v,
            f'{g0[0]["n_rows"]:,} 件の文書。索引の容量：jsonb_ops {mb(g1[0]["index_bytes"])}、jsonb_path_ops {mb(g2[0]["index_bytes"])}（表は {mb(g0[0]["data_bytes"])}）。'
            f'包含の検索（@>）：索引なし {fms(rd(g0, "filter_sum"))}、jsonb_ops {fms(rd(g1, "filter_sum"))}、jsonb_path_ops {fms(rd(g2, "filter_sum"))}。'
            f'キーの存在（?）の実行計画：jsonb_ops は {" → ".join(g1[0]["plans"]["key_exists"])}、jsonb_path_ops は {" → ".join(g2[0]["plans"]["key_exists"])}。'
            f'取り込み：索引なし {fms(median(r["load_ms"] for r in g0))}、jsonb_ops {fms(median(r["load_ms"] for r in g1))}、jsonb_path_ops {fms(median(r["load_ms"] for r in g2))}')
    add(s, 'どの更新も行全体に行レベルのロックを取る。文書を扱える大きさに抑えて競合を減らす', NA, '題材 C は追記と一括の取り込みだけで、同じ文書への並行の更新は測っていない')
    add(s, '「ほかの DB・クラウド製品の JSON 対応は、表の対象外（未検証）」', '一部だけ実測（ClickHouse の JSON 型と DuckDB の read_json。クラウドの製品は未検証）',
        'C-2 で、ClickHouse の JSON 型の列と、DuckDB の read_json・JSON 型を、容量・問い合わせ時間・スキーマが揺れたときの出方について測った')
    add('分析には、ファクトを中心にしたスタースキーマ', 'One Big Table は、容量は増えるが、問い合わせが速くなることもある', NA,
        '題材 C のデータは 1 つの平らな表（注文イベント）だけで、スタースキーマとの比較はしていない')

    # ---- Part 13
    s = '取り込みは、スキーマが揺れたときの動きを先に決める'
    if d.drift:
        cases = [c for c in C2.CASES if c != 'none']
        per = []
        differ = 0
        for c in cases:
            got = []
            for p in PRODUCTS:
                r = sel(d.drift, product=p, pipeline='typed', case=c)
                if r:
                    got.append((PROD_S[p], '・'.join(CODE[x] for x in drift_codes(r[0]))))
            if len({g for _p, g in got}) > 1:
                differ += 1
            per.append(f'{C2.CASES[c]["ja"]}：' + '、'.join(f'{p} {g}' for p, g in got))
        add(s, '揺れたときの動きを決めていないと、実装の偶然で決まる', '一致' if differ else '再現せず',
            f'製品の既定のまま型つきで取り込む作りで、同じ入力に対する結果が製品の間で違ったケースは {differ}/{len(cases)}。' + '。'.join(per) + '（記号は C-2 の節）')
        nk = [r for r in d.drift if r['case'] != 'none']
        stop = [r for r in nk if drift_primary(r) == 'stop']
        add(s, '止める：誤ったデータが下流へ行かない。失うのは鮮度（上流の変更で下流が止まる）', '一致' if stop and all(clean_damage(r) > 0 for r in stop) else '一部だけ再現',
            f'取り込みがエラーで止まった {len(stop)} 実行のすべてで、揺れた {C2.DRIFT_ROWS - C2.DRIFT_FROM:,} 行は入らず、'
            f'揺れていない行も入らなかった（被害は {one(clean_damage(r) for r in stop)} 行）')
        beq = [r for r in nk if r['kind'] == 'BE' and r['counts']['drifted']['quarantined']]
        add(s, '保留して所有者に確認：正しい分は進み、問題が記録に残る。保留を見る人と手順が要る',
            '一致' if beq and all(clean_damage(r) == 0 for r in beq) else '一部だけ再現',
            f'BE の作りで行を保留した {len(beq)} 実行のすべてで、保留は {one(r["quarantined"] for r in beq)} 行、揺れていない行の被害は {one(clean_damage(r) for r in beq)} 行。'
            f'本体の集計は保留の分だけ正解と違う（合計が違う実行 {sum(1 for r in beq if not r["query"]["equal"])}）。'
            f'数字の文字列（"1200"）のように読める揺れも保留になった')
        sil = [r for r in nk if drift_primary(r) == 'silent']
        sum_ok = [r for r in sil if r['query']['ok'] and r['query']['equal']]
        add(s, '黙って落とす・既定値で埋める：止まらない。欠けたことに気づくのが遅れる', '一致' if sil else '再現せず',
            f'エラーなしで行や値が変わった実行は {len(sil)}（落ちた行のある実行 {sum(1 for r in sil if r["counts"]["drifted"]["row_missing"])}、'
            f'NULL {sum(1 for r in sil if r["counts"]["drifted"]["null"])}、0 で埋まる {sum(1 for r in sil if r["counts"]["drifted"]["defaulted"])}、'
            f'値が変わる {sum(1 for r in sil if r["counts"]["drifted"]["altered"])}）。このうち合計の集計が正解と同じ値を返したのは {len(sum_ok)} 実行で、'
            f'そのうち平均は違ったものが {sum(1 for r in sum_ok if not r["query_avg"]["equal"])} 実行')
        ps = [r for r in nk if r['kind'] == 'pass']
        add(s, 'そのまま通す：取り込みは止まらない。読む側のすべてが揺れを扱う', '一致' if ps and all(r['ingest']['ok'] for r in ps) else '一部だけ再現',
            f'形を決めずに持つ・読む作りの {len(ps)} 実行で、取り込みが止まったのは {sum(1 for r in ps if not r["ingest"]["ok"])}。'
            f'読む側では、その項目を読むとエラー {sum(1 for r in ps if drift_primary(r) == "unreadable")}、素直な集計がエラー {sum(1 for r in ps if not r["query"]["ok"])}、'
            f'エラーなしで違う値 {sum(1 for r in ps if r["query"]["ok"] and not r["query"]["equal"])}。'
            f'変換できない値を NULL にする寛容な集計は、{sum(1 for r in ps if r["query_tolerant"] and r["query_tolerant"]["ok"] and not r["query_tolerant"]["equal"])} 実行でエラーなしに違う値を返した')
        add(s, '「項目が揺れたときの失敗の出方を、題材 C で実測する」', '実測',
            f'3 製品 × {sum(len(v) for v in C2.PIPELINES.values())} の取り込みの作り × 8 ケース（{len(d.drift)} 実行）。揺れのある {len(nk)} 実行の内訳：'
            + '、'.join(f'{PRIMARY_JA[k]} {sum(1 for r in nk if drift_primary(r) == k)}' for k in PRIMARY_JA))

    s = '再実行しても同じ結果になるタスクが作り直しの前提'
    if d.c4:
        def w(p, st, sc, key='after_rerun'):
            rs = sel(d.c4, product=p, strategy=st, scenario=sc)
            return rs, (_jr(rs, key) if rs else NA)
        ins = w('pg', 'insert', 'same')
        ups, upc = w('pg', 'upsert', 'same'), w('pg', 'upsert', 'corrected')
        ok_same = ins[0] and ins[0][0]['after_rerun']['duplicates'] > 0 and ups[0][0]['after_rerun']['wrong'] == 0
        left = upc[0][0]['after_rerun']['wrong'] if upc[0] else 0
        v = '一致' if ok_same and not left else ('条件つき（同じ入力と値の訂正では UPSERT で合う。訂正で入力から消えた行は、UPSERT では表に残った）' if ok_same else '食い違い')
        add(s, '再実行で行が重複するので、INSERT ではなく UPSERT を使う（Airflow）', v,
            f'PostgreSQL。INSERT を同じ入力で 2 回：{ins[1]}。UPSERT を同じ入力で 2 回：{ups[1]}。UPSERT で 2 回目が訂正後の入力：{upc[1]}。'
            f'1 回目が半分で止まった後の流し直し：INSERT は {w("pg", "insert", "partial")[1]}、UPSERT は {w("pg", "upsert", "partial")[1]}')
        di = [r for r in d.c4 if r['strategy'] == 'delete_insert']
        add(s, '決まった範囲を、1 トランザクションで置き換える（DELETE して INSERT）', '一致' if di and all(r['after_rerun']['wrong'] == 0 and r['final']['wrong'] == 0 for r in di) else '食い違い',
            f'日の範囲を消して入れ直す作りの {len(di)} 実行（3 製品 × 3 場面）で、流し直した後の誤りは {one(r["after_rerun"]["wrong"] for r in di)} 件、3 日目を足した後も {one(r["final"]["wrong"] for r in di)} 件。'
            'ClickHouse は ALTER DELETE と INSERT が別の操作で、1 つのトランザクションではない（その間の読み取りは測っていない）')
        nw = sel(d.c4, strategy='now_window')
        if nw:
            add(s, '悪い例：範囲が「今」で決まり、再実行で行が重なる', '一致' if nw[0]['after_rerun']['duplicates'] and nw[0]['after_rerun']['extra'] else '一部だけ再現',
                f'「now の前 24 時間」を INSERT する処理を 6 時間後にもう一度流すと：{_j(nw[0]["after_rerun"])}（重なった 18 時間分が重複し、次の日の 6 時間分が余分に入った）')
        sw = [r for r in d.c4 if r['strategy'] in ('replace_partition', 'overwrite_file')]
        add(s, '一時的な場所に書き、検証してから適用する（SRE Workbook）', '一部だけ実測（置き換えだけ。間に検証の段は入れていない）',
            f'仮の表に入れて REPLACE PARTITION（ClickHouse）、日ごとのファイルを rename で置き換える（Parquet）の {len(sw)} 実行で、誤りは {one(r["final"]["wrong"] for r in sw)} 件')
        dup_runs = [r for r in d.c4 if r['after_rerun']['duplicates']]
        add(s, '「同じ範囲を 2 回流したときの重複の件数を、題材 C で実測する」', '実測',
            f'{len(d.c4)} 実行（流し方 {len({(r["product"], r["strategy"]) for r in d.c4})} 通り × 場面）。流し直した後に重複が残った流し方：'
            + '、'.join(sorted({C4.STRATEGY_JA[f'{r["product"]}:{r["strategy"]}'] + f'（{PROD_S[r["product"]]}）' for r in dup_runs})) + '。件数は C-4 の表')

    s = 'テレメトリは失いうる前提。監査の記録は経路を分ける'
    if d.c6:
        def cj(dz, sc, k):
            return one(r['judge'][k] for r in sel(d.c6, design=dz, scenario=sc))

        def lost(dz, sc):
            return max((r['judge']['lost_acked'] for r in sel(d.c6, design=dz, scenario=sc)), default=0)
        v = ('一致（クラッシュと再試行の時間切れは、受け付け済みのログを失った。あふれた分は捨てずに、送り手へ 503 で断った）'
             if lost('memory', 'crash') and lost('memory', 'retry_timeout') and not lost('memory', 'overflow') else '一部だけ再現')
        add(s, 'メモリのキュー（既定）：満杯で新しいデータを捨てる。まだ失う場面は、クラッシュ、キューあふれ、再試行の時間切れ', v,
            f'受け付けたのに届かなかったログ（2,000 件中）：下流の停止中に強制終了 {cj("memory", "crash", "lost_acked")}、再試行の時間切れ {cj("memory", "retry_timeout", "lost_acked")}、'
            f'あふれ {cj("memory", "overflow", "lost_acked")}（断ったログ {cj("memory", "overflow", "rejected")}）、短い停止 {cj("memory", "outage", "lost_acked")}')
        v = ('一致（再起動をまたいで残った。あふれと再試行の時間切れは、メモリのキューと同じ出方だった）'
             if not lost('file', 'crash') and lost('file', 'retry_timeout') else '一部だけ再現')
        add(s, 'ディスクの WAL：再起動をまたいで残る。まだ失う場面は、ディスクの障害と満杯', v + '。ディスクの障害と満杯は未検証',
            f'file_storage のキュー（fsync あり）：下流の停止中に強制終了 {cj("file", "crash", "lost_acked")}、再試行の時間切れ {cj("file", "retry_timeout", "lost_acked")}、'
            f'あふれ {cj("file", "overflow", "lost_acked")}（断ったログ {cj("file", "overflow", "rejected")}）')
        kc, kac = lost('kafka', 'crash'), lost('kafka_after', 'crash')
        v = ('条件つき（読む側の Collector が既定のままだと、Kafka から先に読み出して自分のメモリに持ち、強制終了で失った。'
             '処理の後で印を付ける設定では、強制終了でも失わなかった）' if kc and not kac else ('一致' if not kc else '一部だけ再現'))
        add(s, 'Kafka を間に置く：下流が止まると、下流が戻るまで Kafka にたまる', v,
            f'下流の停止中に、読む側の Collector を強制終了：既定のまま {cj("kafka", "crash", "lost_acked")}、処理の後で印を付ける設定 {cj("kafka_after", "crash", "lost_acked")}。'
            f'短い停止はどちらも {cj("kafka", "outage", "lost_acked")}・{cj("kafka_after", "outage", "lost_acked")}')
        add(s, 'Kafka を間に置いても、まだ失う場面は、Kafka の障害、保持期限の超過',
            '一致' if lost('kafka_after', 'retention') and lost('kafka', 'broker_down_crash') else '一部だけ再現',
            f'保持（2 秒）より長い停止：処理の後で印を付ける設定で {cj("kafka_after", "retention", "lost_acked")} 件、既定のままでは {cj("kafka", "retention", "lost_acked")} 件（先に読み出していたため）。'
            f'ブローカーの一時停止：戻せば {cj("kafka", "broker_down", "lost_acked")} 件（重複 {cj("kafka", "broker_down", "duplicates")} 件）、'
            f'停止中に送り側の Collector を強制終了すると {cj("kafka", "broker_down_crash", "lost_acked")} 件')
        add(s, '既定のキューは多くの場合 1000 バッチ。再試行は既定で 5 分まで', NA,
            'キューの大きさは既定のまま使ったが、あふれさせた場面は 10 要求に縮めた。再試行の上限は 120 秒と 3 秒に縮めて測り、既定の 5 分は待っていない'
            '（Collector 0.165.0 の文書の既定値は queue_size 1000、max_elapsed_time 300s）')
    add(s, '費用を決めるのは、保持、カーディナリティ、サンプリング', '一部だけ実測（保持を落とす操作の時間と容量だけ。カーディナリティとサンプリングは未検証）',
        '古い 10 日分を落とす操作は C-3 の表。時系列のデータベース、トレースのサンプリングは動かしていない')
    add(s, '診断のログと監査の記録は、経路を分ける。Cloudflare の事後報告の数値', NA, '監査の記録の経路は組んでいない。事後報告は文献で、実測の対象ではない')

    # ---- other slides that mention the case study / unverified points
    add('障害と機能不全を分け、耐える障害を先に決める', '「障害を注入して確かめる（Toxiproxy と Pumba。結果はケーススタディ）」', '実測（題材 C では別の入れ方）',
        '題材 C は Toxiproxy と Pumba を使っていない。障害は C-6 だけで、下流の 503 応答、docker kill、docker pause を、要求の件数を合図に入れた')
    add('負荷は、読み書きの比とファンアウトで表す', 'Krikorian「Timelines at Scale」の講演の数値は未確認', NA, '文献の確認で、題材 C の実測の対象ではない')
    add('共通基盤（認証・AI・セキュリティ）の各スライド', '認証・AI・セキュリティ基盤は実機にしていない（未検証）', NA, '題材 C でも組んでいない（設計のとおり）')
    add('このガイドの限界と、ケーススタディで確かめること', '題材 C は、行指向、列指向、ファイルで、容量と時間を比べる。数値は 1 台の Docker 環境の中での相対比較', '実測',
        f'C-1〜C-6 の {len(d.runs)} 実行。数値はこの 1 台（8 コア、WSL2）の中の相対比較')

    out = ['## ガイドの記述との照合\n',
           'ガイド `data-intensive-design-guide` の Part 2（deck/p02.js）、Part 4（p04.js）、Part 5（p05.js）、Part 13（p13.js・p13b.js）、Part 17（p17.js）の記述のうち、'
           '題材 C に当たる点と、スライドが「題材 C で実測する」「ケーススタディ」「未確認」「未検証」と書いた点を、下の結果と比べた。'
           '判定は結果のファイルの数から付けた（しきい値は `cs/c/agg_sections.py` にある）。「一部だけ再現」「条件つき」は、再現した点としなかった点が混じることを表し、中身は実測の列に書いた。\n',
           table(['スライドの題', 'ガイドの記述', '判定', '実測'], rows)]
    return out


# ================================================================= ablation
def ablation(d):
    out = ['## 構成 N・B・BE と除去実験\n',
           '題材 C の構成は、1 本の流れを組み替えたものではなく、実験ごとの選択の組み合わせである。下の表は、BE（と B）の要素を 1 つ外したときに戻る結果を、'
           '対応する実験の数字で並べたもの（新しく流した実行ではない）。\n']
    rows = []
    big = [s for s in C.SCALES if sel(d.c1, scale=s)][-1]

    def c1m(p, q):
        return median(qmed(r, q) for r in sel(d.c1, scale=big, product=p))
    pgs, chs = sel(d.c1, scale=big, product='pg')[0]['size'], sel(d.c1, scale=big, product='ch')[0]['size']
    rows.append(['BE − E-C1（列指向と圧縮）＝行指向の表のまま', 'C-1',
                 f'容量 {mb(chs["total_bytes"])} → {mb(pgs["total_bytes"])}。店舗ごとの集計 {fms(c1m("ch", "agg_store"))} → {fms(c1m("pg", "agg_store"))}、'
                 f'日ごとの集計 {fms(c1m("ch", "agg_day"))} → {fms(c1m("pg", "agg_day"))}',
                 f'逆に、キーで 1 行の検索は {fms(c1m("ch", "point_pk"))} → {fms(c1m("pg", "point_pk"))}、1 行ずつの書き込みも行指向のほうが短い（E-C1 の費用）'])
    nk = [r for r in d.drift if r['case'] != 'none']
    for p in PRODUCTS:
        be = [r for r in nk if r['product'] == p and r['pipeline'] == 'be']
        n = [r for r in nk if r['product'] == p and r['pipeline'] == 'typed']
        if not be or not n:
            continue

        def tally(rs):
            return '、'.join(f'{PRIMARY_JA[k]} {sum(1 for r in rs if drift_primary(r) == k)}' for k in PRIMARY_JA if any(drift_primary(r) == k for r in rs))
        rows.append([f'BE − E-C2（検査・未知のキーの列・保留）＝既定の型つきの取り込みに戻す：{PROD_S[p]}', 'C-2',
                     f'揺れのある 7 ケース：BE は {tally(be)} → 外すと {tally(n)}',
                     f'揺れていない行まで欠けた実行：{sum(1 for r in be if clean_damage(r))} → {sum(1 for r in n if clean_damage(r))}'])
    for p, a, b in (('pg', 'partition_drop', 'delete'), ('ch', 'drop_partition', 'alter_delete'), ('duck', 'files_per_day', 'single_file_rewrite')):
        ra, rb = sel(d.retain, product=p, method=a), sel(d.retain, product=p, method=b)
        if ra and rb:
            first = rb[0]['steps'][0]
            rows.append([f'BE − E-C3（パーティションの単位で落とす）＝{C3.RETAIN_JA[f"{p}:{b}"]}：{PROD_S[p]}', 'C-3',
                         f'落とす時間 {rng(r["total_ms"] for r in ra)} → {rng(r["total_ms"] for r in rb)}',
                         f'最初の段（{first["step"]}）の直後の容量：{mb(median(r["bytes_before"] for r in rb))} → {mb(median(r["steps"][0]["bytes_after"] for r in rb))}'
                         f'（パーティションの単位では {mb(median(r["bytes_before"] for r in ra))} → {mb(median(r["bytes_final"] for r in ra))}）'])
    for p, good, bad in (('pg', 'delete_insert', 'insert'), ('ch', 'replace_partition', 'insert'), ('duck', 'overwrite_file', 'append_file')):
        cells = []
        for sc in C4.SCENARIOS:
            rg, rb = sel(d.c4, product=p, strategy=good, scenario=sc), sel(d.c4, product=p, strategy=bad, scenario=sc)
            if rg and rb:
                cells.append(f'{C4.SCENARIO_JA[sc]}：誤り {one(r["after_rerun"]["wrong"] for r in rg)} → {one(r["after_rerun"]["wrong"] for r in rb)}')
        if cells:
            rows.append([f'B − 冪等な再実行（{C4.STRATEGY_JA[f"{p}:{good}"]}）＝{C4.STRATEGY_JA[f"{p}:{bad}"]}：{PROD_S[p]}', 'C-4', '。'.join(cells),
                         '誤り＝重複＋欠落＋古い値＋余分の行数（内訳は C-4 の表）'])
    out.append(table(['外す要素（外した後の作り）', '実験', '実測（入れた場合 → 外した場合）', '補足'], rows))
    return out


# =============================================================== normal ops
def normal_ops(d):
    out = ['## 正常な操作：基準と重点の費用\n']
    rows = []
    none = sel(d.drift, case='none')
    bad = [r for r in none if not r['ingest']['ok'] or clean_damage(r) or not r['query']['equal'] or r['quarantined']]
    rows.append(['揺れの無い入力の取り込み（C-2）', f'{len(none)} 実行（全製品・全部の取り込みの作り）',
                 f'失敗・欠けた行・保留・集計の不一致のあった実行 {len(bad)}'])
    be = [r for r in d.drift if r['kind'] == 'BE']
    for c in ('field_added', 'field_removed', 'num_as_string'):
        rs = [r for r in be if r['case'] == c]
        rows.append([f'BE の検査が、正規の変更を止めないか：{C2.CASES[c]["ja"]}', f'{len(rs)} 実行（3 製品）',
                     f'保留 {one(r["quarantined"] for r in rs)} 行、上流の意味どおりに読めた行 {one(r["counts"]["drifted"]["faithful"] for r in rs)}'])
    rows.append(['再実行の作りが、新しい正規のバッチ（3 日目）を拒まないか（C-4）', f'{len(d.c4)} 実行',
                 f'3 日目の行が欠けた実行 {sum(1 for r in d.c4 if r["day3_missing"])}、3 日目の取り込みがエラーになった実行 {sum(1 for r in d.c4 if r["run_errors"] and r["run_errors"][-1])}'])
    rows.append(['古いデータを落とした後、残すべき行が残っているか（C-3）', f'{len(d.retain)} 実行', f'行数か金額の合計が正解と違った実行 {sum(1 for r in d.retain if r["check"]["mismatch"])}'])
    perf = d.c1 + d.perf + d.compress + d.c5
    rows.append(['問い合わせの結果が正解と一致するか（C-1・C-2・C-3・C-5）', f'{len(perf)} 実行', f'不一致のあった実行 {sum(1 for r in perf if r["mismatch_total"])}'])
    pg_fail = sum(r['mixed'][k]['failed'] for r in sel(d.c1, product='pg') for k in ('0', '1', '4'))
    rows.append(['集計を並べた PostgreSQL で、検索と挿入が失敗しないか（C-1）', f'{len(sel(d.c1, product="pg"))} 実行 × 3 段', f'失敗 {pg_fail} 件（遅れは C-1 の表）'])
    nrm = sel(d.c6, scenario='normal')
    if nrm:
        rows.append(['障害の無いとき、Collector がログを失わないか（C-6）', f'{len(nrm)} 実行',
                     f'届かなかったログ {sum(r["judge"]["lost_acked"] for r in nrm)}、断ったログ {sum(r["judge"]["rejected"] for r in nrm)}、重複 {sum(r["judge"]["duplicates"] for r in nrm)}'])
        ka, km = sel(nrm, design='kafka_after'), sel(nrm, design='memory')
        if ka and km:
            rows.append(['失わない側に倒した設定の遅れ（C-6：処理の後で印を付け、キューを持たない受け側）', f'{len(ka)} 実行',
                         f'届くまでの遅れ p50 {rng(r["delay_normal_ms"]["p50_ms"] for r in ka)}（メモリのキューの設計は {rng(r["delay_normal_ms"]["p50_ms"] for r in km)}）'])
    out.append(table(['確かめたこと', '数', '結果'], rows))
    out.append('BE の検査は、数字の文字列（"1200"）のように値としては読める揺れも保留に回す。保留を見る人と手順が要る、という費用はこの形で出た。\n')
    return out


# ================================================================= findings
def findings(d):
    out = ['## 期待どおりにならなかった結果・気づいた点\n']
    b = []
    ch = sel(d.c1, product='ch')
    if ch:
        b.append(f'**ClickHouse 26.8 は、設定の表示では async_insert=1 が既定だった**（system.settings の値：{ch[0]["settings"].get("async_insert")}）。'
                 f'1 行ずつの INSERT は、既定のままだと 1 要求あたり約 {median(r["rowwrite"]["single_ms"] for r in ch) / ch[0]["rowwrite"]["rows"]:.0f} ms かかり'
                 f'（まとめ書きを待つ時間）、async_insert=0 では約 {median(r["rowwrite"]["single_sync_ms"] for r in ch) / ch[0]["rowwrite"]["rows"]:.1f} ms だった。'
                 f'wait_for_async_insert=0 では応答は早いが、応答の直後に読めた行は {one(r["rowwrite"]["single_nowait_rows_at_return"] for r in ch)} 行（{ch[0]["rowwrite"]["rows"]} 行中）で、'
                 '書き込みの完了より前に応答が返る。パーツの数は、測った時点ではマージが進んでいて 1 行 1 パーツの状態は見えなかった')
    scales = [s for s in C.SCALES if sel(d.c1, scale=s)]
    if scales:
        big = scales[-1]
        m = lambda p, q: median(qmed(r, q) for r in sel(d.c1, scale=big, product=p))  # noqa: E731
        b.append(f'**キーでない列の絞り込み（顧客 1 人、全列）は、列指向でも行指向の全走査と同じ桁だった**：{big} で PostgreSQL {fms(m("pg", "filter_nokey"))}、'
                 f'ClickHouse {fms(m("ch", "filter_nokey"))}、DuckDB＋Parquet {fms(m("duck", "filter_nokey"))}。集計ほどの差は出ていない。'
                 f'キーで 1 行の検索は、索引のある PostgreSQL が {fms(m("pg", "point_pk"))}、ClickHouse は {fms(m("ch", "point_pk"))}（event_id は並び順の 2 番目で、索引として効かない）')
    cj = sel(d.perf, variant='ch_json')
    if cj:
        b.append(f'**ClickHouse の JSON 型は、文字列で送った日時を {"・".join(cj[0]["size"]["ts_dynamic_types"])} として持った**。最初に書いた問い合わせ（`doc.ts.:String`）は、'
                 f'エラーにならずに全行 NULL を返し（読めた行 {cj[0]["size"]["ts_as_string_nonnull"]}）、日ごとの集計の照合で不一致になって気づいた。'
                 '型を指定した部分列（`.:型`）は、型が違う行を黙って NULL にする')
    tol = [r for r in d.drift if r['query_tolerant'] and r['query_tolerant']['ok'] and not r['query_tolerant']['equal']]
    if tol:
        b.append(f'**寛容な読み方は、エラーを消す代わりに、黙って違う値を返す**：変換できない値を NULL にする集計が、エラーなしで正解と違う値を返したのは {len(tol)} 実行'
                 f'（{"、".join(sorted({PROD_S[r["product"]] + "：" + r["pipeline"] for r in tol}))}）。数字の文字列（"1200"）に変わっただけでも、'
                 f'ClickHouse の `.:Int64` は {sum(1 for r in tol if r["product"] == "ch" and r["case"] == "num_as_string")} 実行で合計が減った')
    cjn = [r for r in d.drift if r['product'] == 'ch' and r['pipeline'] == 'json' and r['query']['ok'] and r['query']['equal']
           and r['query_avg']['ok'] and not r['query_avg']['equal']]
    if cjn:
        b.append(f'**ClickHouse の JSON 型の値を `::Float64` で変換すると、項目の無い行が 0 になった**：{len(cjn)} ケース（{"、".join(C2.CASES[r["case"]]["ja"] for r in cjn)}）で、'
                 f'合計は正解と同じだが、平均は {"、".join(f"{r["query_avg"]["value"]:.1f}（正解 {r["query_avg"]["truth"]:.1f}）" for r in cjn)} になった。行そのものは NULL として読める（表の ○）のに、変換した集計だけが変わる')
    chd = [r for r in d.drift if r['product'] == 'ch' and r['pipeline'] == 'typed' and r['counts']['drifted']['defaulted']]
    if chd:
        ok = [r for r in chd if r['query']['equal']]
        b.append(f'**ClickHouse の既定の型つきの取り込みは、欠けた項目と null を 0 で埋めた**（{len(chd)} ケース：{"、".join(C2.CASES[r["case"]]["ja"] for r in chd)}）。'
                 f'このうち {len(ok)} ケースは合計が正解と同じ値のままで、平均だけが違った（{"、".join(f"{r["query_avg"]["value"]:.1f}（正解 {r["query_avg"]["truth"]:.1f}）" for r in ok)}）。'
                 '列を Nullable にしていない、よくある定義での結果')
    dk = [r for r in d.drift if r['product'] == 'duck' and r['case'] == 'int_to_float' and r['counts']['drifted']['altered']]
    if dk:
        b.append(f'**DuckDB の read_json は、整数の列に来た小数をエラーにせず丸めた**（{"、".join(C2.PIPELINE_JA["duck:" + r["pipeline"]] for r in dk)}：'
                 f'{one(r["counts"]["drifted"]["altered"] for r in dk)} 行）。PostgreSQL の jsonb_populate_record と ClickHouse の JSONEachRow は、同じ入力で取り込みを止めた')
    au = [r for r in d.drift if r['product'] == 'duck' and r['pipeline'] == 'auto' and r['counts']['drifted']['unreadable']]
    af = [r for r in d.drift if r['product'] == 'duck' and r['pipeline'] == 'auto' and r['field_absent']]
    if au or af:
        b.append(f'**DuckDB の read_json の自動判定は、既定では先頭の行だけを見て型を決める**。揺れが {C2.DRIFT_FROM:,} 行目より後ろにあると、'
                 f'型の違う値で読み出しがエラーになり（{len(au)} ケース）、増えた項目は列として現れなかった（{len(af)} ケース）。sample_size=-1（全行から判定）では、'
                 '型の混じった列が JSON 型になり、素直な合計は関数の型のエラーになった')
    pgi = [r for r in d.drift if r['product'] == 'pg' and r['pipeline'] == 'csv_ignore' and r['case'] != 'none']
    if pgi:
        st = [r for r in pgi if not r['ingest']['ok']]
        dr = [r for r in pgi if r['ingest']['ok'] and r['counts']['drifted']['row_missing']]
        b.append(f'**PostgreSQL の COPY の ON_ERROR ignore が飛ばしたのは、型に変換できない値の行だけ**（{len(dr)} ケースで各 {one(r["counts"]["drifted"]["row_missing"] for r in dr) if dr else 0} 行を黙って落とす）。'
                 f'列の数が違う行と NOT NULL の違反では、指定していても取り込み全体が止まった（{len(st)} ケース）')
    rt = sel(d.retain, product='pg', method='delete')
    td = sel(d.retain, product='duck', method='table_delete')
    lw = sel(d.retain, product='ch', method='lightweight_delete')
    if rt:
        s = rt[0]['steps']
        b.append(f'**DELETE は容量を減らさない**。PostgreSQL は DELETE の後も VACUUM の後も {mb(s[0]["bytes_after"])}・{mb(s[1]["bytes_after"])}（前は {mb(rt[0]["bytes_before"])}）で、'
                 f'VACUUM FULL（{rng(r["steps"][2]["ms"] for r in rt)}。表を排他ロックして書き直す）で {mb(s[2]["bytes_after"])} になった。'
                 + (f'ClickHouse の軽量な削除も、直後は {mb(lw[0]["steps"][0]["bytes_after"])} のままで、OPTIMIZE FINAL の後に {mb(lw[0]["steps"][1]["bytes_after"])}。' if lw else '')
                 + (f'DuckDB の表は、DELETE と CHECKPOINT の後、ファイルが {mb(td[0]["bytes_before"])} → {mb(td[0]["bytes_final"])} と大きくなった' if td else ''))
    cr = [r for r in d.retain if r['product'] == 'ch']
    if cr:
        b.append(f'**ClickHouse は、落とした後も古いパーツをしばらくディスクに残す**：有効なパーツの容量は減ったが、使わなくなったパーツが {mb(min(r["inactive_bytes"] for r in cr))}〜{mb(max(r["inactive_bytes"] for r in cr))} 残っていた'
                 '（測定の時点。実際に空くまでの時間は測っていない）')
    rp = sel(d.c4, product='ch', strategy='replacing')
    if rp:
        same = sel(rp, scenario='same')
        cor = sel(rp, scenario='corrected')
        b.append(f'**ReplacingMergeTree は、そのまま読むと重複が見える**：同じ入力を 2 回流した後、ふつうの SELECT では重複 {one(r["after_rerun"]["duplicates"] for r in same)} 行、'
                 f'3 日目を足した後も {one(r["final"]["duplicates"] for r in same)} 行。FINAL を付けるか OPTIMIZE FINAL の後は {one(r["read_variants"]["final"]["wrong"] for r in same)} 件。'
                 f'訂正で入力から消えた行は、FINAL でも {one(r["read_variants"]["final"]["wrong"] for r in cor)} 行残った')
    dd = sel(d.c4, product='ch', strategy='insert_dedup')
    if dd:
        b.append(f'**ClickHouse のブロックの重複排除（non_replicated_deduplication_window）は、まったく同じブロックにしか効かない**：同じ入力の 2 回目は重複 {one(r["after_rerun"]["duplicates"] for r in sel(dd, scenario="same"))} 行、'
                 f'訂正後の入力では {one(r["after_rerun"]["duplicates"] for r in sel(dd, scenario="corrected"))} 行、半分で止まった後の流し直しでは {one(r["after_rerun"]["duplicates"] for r in sel(dd, scenario="partial"))} 行。'
                 f'既定（window=0）では同じ入力でも {one(r["after_rerun"]["duplicates"] for r in sel(d.c4, product="ch", strategy="insert", scenario="same"))} 行')
    ar = sel(d.c5, product='duck', variant='tbl_art')
    a0 = sel(d.c5, product='duck', variant='tbl0')
    if ar and a0:
        b.append(f'**DuckDB の索引は、この絞り込みでは使われなかった**：実行計画は索引なしと同じ {" → ".join(ar[0]["plans"]["filter_sum"])} で、ファイルだけが {mb(a0[0]["data_bytes"])} → {mb(ar[0]["data_bytes"])} に増えた')
    kd, ka = sel(d.c6, design='kafka', scenario='crash'), sel(d.c6, design='kafka', scenario='retention')
    kr = sel(d.c6, design='kafka_after', scenario='retention')
    if kd and ka:
        b.append(f'**Kafka を間に置いても、読む側の Collector が既定のままだと、Kafka は「下流が戻るまでためる場所」にならなかった**：読む側はメッセージを先に読み出して印を付け、'
                 f'自分のメモリのキューで再試行する。その Collector を強制終了すると {one(r["judge"]["lost_acked"] for r in kd)} 件を失い、'
                 f'保持（2 秒）を超える停止では逆に {one(r["judge"]["lost_acked"] for r in ka)} 件だった（もう Kafka の外にあったため）。'
                 + (f'処理の後で印を付ける設定にすると強制終了では失わないが、保持を超えた分は {one(r["judge"]["lost_acked"] for r in kr)} 件を失った' if kr else ''))
    of = sel(d.c6, scenario='overflow')
    if of:
        in_fault = (C6.FAULT_TO - C6.FAULT_FROM) * C6.PER_REQUEST
        later = sum(1 for r in of if r['judge']['rejected'] > in_fault)
        b.append(f'**Collector のキューがあふれると、新しい要求は HTTP 503 で断られた**（{one(r["judge"]["rejected"] for r in of)} 件。メモリでもディスクでも起きた）。'
                 f'受け付け済みで届かなかったログは {one(r["judge"]["lost_acked"] for r in of)} 件。失うかどうかは、断られた送り手が再送するかで決まる（この実験の送り手は再送しない）。'
                 f'障害の間に送った {in_fault} 件より多く断られた試行が {later}/{len(of)} あり、下流が戻った後の要求も、キューが空くまでは断られた')
    nm, nf, na = (sel(d.c6, design=x, scenario='normal') for x in ('memory', 'file', 'kafka_after'))
    if nm and nf and na:
        b.append(f'**正常時に届くまでの遅れ**：届くまでの p50 は、メモリのキュー {rng(r["delay_normal_ms"]["p50_ms"] for r in nm)}、'
                 f'ディスクのキュー {rng(r["delay_normal_ms"]["p50_ms"] for r in nf)}、Kafka を挟み処理の後で印を付ける設計 {rng(r["delay_normal_ms"]["p50_ms"] for r in na)}。'
                 'ディスクのキューは fsync を指定している。'
                 '差の内訳は分けて測っていない。受け口は同じ機械の上の実験用の HTTP サーバーで、ネットワークの遅れは無い')
    bd = sel(d.c6, design='kafka', scenario='broker_down')
    if bd and any(r['judge']['duplicates'] for r in bd):
        b.append(f'**ブローカーの一時停止の後、同じログが 2 回届いた**（{one(r["judge"]["duplicates"] for r in bd)} 件）。失ってはいないが、受け手に重複を捨てる仕組みが要る')
    for x in b:
        out.append(f'- {x}')
    out.append('')
    return out


def closing(d):
    out = ['## 見送り\n',
           '- **全文検索の専用ストア**（OpenSearch など）：入れていない。失うのは、ログの本文（message）を語で探す検索と、その索引。題材 C のログの問い合わせは、'
           '決まった列の絞り込みと集計だけにした。見直す条件：本文の語での検索が日常の問い合わせになり、列の絞り込みで絞ってからの走査では応答時間が足りなくなったとき。'
           '入れる場合は、転置索引の更新の費用（ガイド Part 5）と、導出ストアとしての作り直しの手順（Part 12）が加わる\n',
           '## 限界\n']
    csvs = sorted({r['csv_bytes'] for r in d.c1})
    pg_agg = [qmed(r, q) for r in sel(d.c1, product='pg') for q in ('agg_store', 'agg_day', 'topn')]
    hit = {r['scale'] for r in sel(d.c1, product='pg')
           if r['mixed']['4']['lookup']['p95_ms'] > 1.5 * r['mixed']['0']['lookup']['p95_ms']}
    mixed_hit = [s for s in C.SCALES if s in hit]
    lim = [
        f'1 台（8 コア、WSL2 の Docker）の上の、1 ノードの製品。データは {"・".join(f"{v:,}" for v in C.SCALES.values())} 行'
        f'（CSV で {mb(csvs[0]) if csvs else NA}〜{mb(csvs[-1]) if csvs else NA}）で、どの規模もメモリに収まる。'
        '時間と容量は、この機械・この大きさでの相対比較で、絶対値は一般化できない。メモリに収まらない規模、複数ノード、同時に多数の利用者がいる状態は測っていない',
        f'問い合わせは暖機 1 回のあとの {C.REPS} 回で、OS のページキャッシュは捨てていない（この環境ではキャッシュを捨てる操作をしない決まり）。冷えた状態の時間は測っていない',
        '時間はクライアントで測った。PostgreSQL は psycopg、ClickHouse は HTTP と JSON の応答、DuckDB は同じプロセスの中で、経路が違う。数 ms 以下の差は、経路の差と区別できない',
        '製品の設定は、メモリの上限のほかは既定のまま。ClickHouse と DuckDB は 8 スレッド、PostgreSQL の並列ワーカーは 1 つの問い合わせに 2 つまでで、使う CPU の数が違う。'
        '並び順・索引・型（ClickHouse の LowCardinality など）は、それぞれの製品でふつうに書く形を 1 つ選んだもので、選び方で結果は変わる',
        'データは生成器が作った一様に近い分布（店舗と商品だけ偏りを付けた）で、実際のログの偏りや長い本文は無い。圧縮の効き方はデータに強く左右される',
        '取り込みは、サーバーが手元のファイルを直接読む形。ネットワーク越しの取り込み、少しずつ届くデータの取り込みは、C-1 の 1 行ずつの書き込みと C-5 の 100 行ずつの挿入だけ',
        'C-2 の揺れは 8 ケースで、ネストした構造、配列、日時の形式の変化は入れていない。CSV で流したのは PostgreSQL だけ。BE の検査の決まり（必須の項目、整数だけを通す）は実験の前に決めた 1 つの形',
        'C-3 の保持は 1 回の操作の時間と容量だけ。落としている間のほかの読み書きへの影響、VACUUM FULL のロックの間の停止は測っていない',
        'C-4 は 1 つの書き手が順に流す形で、同時に 2 つの実行が走る場合、流し直しの途中で読む人が見るものは測っていない',
        'C-6 の時間は縮めた（再試行の上限 120 秒と 3 秒、保持 2 秒）。下流の「停止」は 503 の応答で、接続の拒否や無応答ではない。送り手は再送しない。Kafka の役は Redpanda の 1 ノードで、Apache Kafka では流していない',
        '「0 件」「失わなかった」は、この手順と試行回数で起きなかったという意味で、起きないことの証明ではない',
    ]
    out += [f'- {x}' for x in lim]
    out += ['', '## 未検証\n']
    un = [
        '公開のデータセット：DESIGN.md は公開の小さなデータセットも使うとしていたが、使っていない（正解を生成器から作れないため。生成器のログだけで測った）',
        'B ツリーと LSM ツリーの直接の比較（設計のとおり、対象外）',
        'メモリに収まらない規模での性能、冷えたキャッシュでの性能、複数ノードの構成、クラウドのマネージド製品、オブジェクトストレージの上の表形式（Iceberg など）',
        f'1 つの RDB で兼ねてよい規模の境目：3 つの規模では、行指向の集計 3 本は最大でも {fms(max(pg_agg)) if pg_agg else NA} で、'
        f'検索の p95 が 1.5 倍を超えた試行があったのは、集計を 4 本並べた {"・".join(mixed_hit) or "（なし）"} の規模。線を引ける規模までは測っていない',
        'スタースキーマと One Big Table の比較、JSON の文書への並行の更新とロックの競合',
        '索引を作り直している間の、問い合わせの遅さ',
        'ディスクの障害・満杯のときの、Collector のディスクのキュー。Collector の既定の時間（再試行 5 分）そのもの。メトリクスとトレース（ログだけを流した）',
        'カーディナリティとサンプリングの費用、監査の記録の経路',
        'ClickHouse の Nullable な列での型つきの取り込み、PostgreSQL と ClickHouse の外部の取り込みの道具（Kafka エンジン、COPY 以外のローダー）',
        '保持の TTL を now() で表した場合の動き（壁時計に頼らないよう、固定の境目の式で測った）',
    ]
    out += [f'- {x}' for x in un]
    out.append('')
    return out
