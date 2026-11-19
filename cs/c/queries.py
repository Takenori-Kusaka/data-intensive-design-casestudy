"""The table of events in the three products, the six queries, and the check of their
results against the truth made by the generator.

Queries (same meaning in every product):
  point_pk      one row by its key, all columns
  range_ts      four columns of the rows of a 10-minute window
  agg_store     count and sum(amount) per store (reads 2 of the 23 columns, all rows)
  agg_day       sum(quantity) of completed events per day (3 columns, all rows)
  topn          the 10 items with the largest sum(amount)
  filter_nokey  all columns of the rows of one customer (a column that is not a key)
"""
from . import gen

QUERIES = ['point_pk', 'range_ts', 'agg_store', 'agg_day', 'topn', 'filter_nokey']
QUERY_JA = {'point_pk': '点の検索（キーで 1 行、全列）', 'range_ts': '範囲（10 分の窓、4 列）',
            'agg_store': '集計（店舗ごとの件数と金額、2 列を全行）', 'agg_day': '集計（完了の数量を日ごと、3 列を全行）',
            'topn': '上位 N（金額の多い商品 10 件）', 'filter_nokey': 'キーでない列の絞り込み（顧客 1 人、全列）'}

_PG = {'int64': 'bigint', 'int32': 'integer', 'int16': 'smallint', 'ts': 'timestamp', 'lowcard': 'text', 'str': 'text'}
_CH = {'int64': 'Int64', 'int32': 'Int32', 'int16': 'Int16', 'ts': 'DateTime', 'lowcard': 'LowCardinality(String)',
       'str': 'String'}
_DUCK = {'int64': 'BIGINT', 'int32': 'INTEGER', 'int16': 'SMALLINT', 'ts': 'TIMESTAMP', 'lowcard': 'VARCHAR',
         'str': 'VARCHAR'}


def pg_columns():
    return ', '.join(f'{n} {_PG[k]}{"" if nullable else " NOT NULL"}' for n, k, nullable in gen.COLUMNS)


def ch_columns(codec=None):
    out = []
    for n, k, nullable in gen.COLUMNS:
        t = f'Nullable({_CH[k]})' if nullable else _CH[k]
        out.append(f'{n} {t}' + (f' CODEC({codec})' if codec else ''))
    return ', '.join(out)


def ch_structure():
    return ', '.join(f'{n} {"Nullable(" + _CH[k] + ")" if nullable else _CH[k]}' for n, k, nullable in gen.COLUMNS)


def duck_columns():
    return '{' + ', '.join(f"'{n}': '{_DUCK[k]}'" for n, k, _ in gen.COLUMNS) + '}'


def duck_ddl():
    return ', '.join(f'{n} {_DUCK[k]}' for n, k, _ in gen.COLUMNS)


def sql(product, q, table, prm, i):
    """SQL text of query ``q`` for repetition ``i`` (parameters come from the generator)."""
    day = {'pg': 'ts::date', 'ch': 'toDate(ts)', 'duck': 'CAST(ts AS DATE)'}[product]
    if q == 'point_pk':
        return f"SELECT * FROM {table} WHERE event_id = {prm['point_ids'][i]}"
    if q == 'range_ts':
        a, b = prm['windows'][i]
        return (f"SELECT event_id, store_id, amount, status FROM {table} "
                f"WHERE ts >= '{a}' AND ts < '{b}' ORDER BY event_id")
    if q == 'agg_store':
        return f"SELECT store_id, count(*), sum(amount) FROM {table} GROUP BY store_id ORDER BY store_id"
    if q == 'agg_day':
        return (f"SELECT {day} AS d, sum(quantity) FROM {table} WHERE status = 'completed' "
                f"GROUP BY d ORDER BY d")
    if q == 'topn':
        return f"SELECT item_id, sum(amount) AS s FROM {table} GROUP BY item_id ORDER BY s DESC, item_id LIMIT 10"
    if q == 'filter_nokey':
        return f"SELECT * FROM {table} WHERE customer_id = {prm['customers'][i]} ORDER BY event_id"
    raise KeyError(q)


def canon_row(r):
    out = []
    for v, (_n, kind, _nullable) in zip(r, gen.COLUMNS):
        if v is None:
            out.append(None)
        elif kind == 'ts':
            out.append(str(v)[:19])
        elif kind in ('str', 'lowcard'):
            out.append(str(v))
        else:
            out.append(int(v))
    return out


def canon(q, rows):
    """Product result -> the canonical form used by the truth."""
    if q in ('point_pk', 'filter_nokey'):
        return [canon_row(r) for r in rows]
    if q == 'range_ts':
        return [[int(r[0]), int(r[1]), int(r[2]), str(r[3])] for r in rows]
    if q == 'agg_store':
        return [[int(r[0]), int(r[1]), int(r[2])] for r in rows]
    if q == 'agg_day':
        return [[str(r[0])[:10], int(r[1])] for r in rows]
    if q == 'topn':
        return [[int(r[0]), int(r[1])] for r in rows]
    raise KeyError(q)


def expected(q, truth, i):
    prm = truth['params']
    if q == 'point_pk':
        return [truth['points'][str(prm['point_ids'][i])]]
    if q == 'filter_nokey':
        return truth['customers'][str(prm['customers'][i])]
    if q == 'range_ts':
        return truth['windows'][i]
    if q == 'agg_store':
        return truth['by_store']
    if q == 'agg_day':
        return truth['completed_qty_by_day']
    if q == 'topn':
        return truth['top_items']
    raise KeyError(q)


def result_diff(expected_rows, got_rows):
    """Judge: compare a canonical result with the truth. Order matters (all queries sort)."""
    exp = [tuple(r) for r in expected_rows]
    got = [tuple(r) for r in got_rows]
    se, sg = set(exp), set(got)
    d = {'rows_expected': len(exp), 'rows_got': len(got), 'missing': len(se - sg), 'extra': len(sg - se),
         'duplicates': len(got) - len(sg),
         'order': int(se == sg and len(exp) == len(got) and exp != got)}
    d['mismatch'] = d['missing'] + d['extra'] + d['duplicates'] + d['order']
    return d
