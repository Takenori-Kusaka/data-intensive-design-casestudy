"""Generator of the order-event log of the fictional delivery service, and the truth.

One row = one event of an order. The same rows are written as CSV (and optionally as
NDJSON); the expected answers of the queries are computed here, from the generated
arrays, never from the output of a product.

Rows are generated in fixed chunks of CHUNK rows, each from rng([seed, chunk]) so that
the content does not depend on how the work is split.
"""
import json
import os

import numpy as np
import pyarrow as pa
import pyarrow.csv as pacsv

from . import config as C

CHUNK = 100_000
BASE_TS = 1790812800          # 2026-10-01 00:00:00 UTC
DAYS = 30
STATUSES = ['accepted', 'cooking', 'delivering', 'completed', 'cancelled']
STATUS_P = [0.05, 0.05, 0.05, 0.75, 0.10]
PAYMENTS = ['card', 'cash', 'wallet', 'points']
PLATFORMS = ['ios', 'android', 'web']
VERSIONS = ['5.1.0', '5.1.1', '5.2.0', '5.2.1', '5.3.0', '6.0.0']
N_STORES, N_CUSTOMERS, N_ITEMS, N_COURIERS = 500, 200_000, 2000, 5000

# (name, type family, nullable)
COLUMNS = [
    ('event_id', 'int64', False), ('ts', 'ts', False), ('order_id', 'int64', False),
    ('store_id', 'int32', False), ('customer_id', 'int32', False), ('item_id', 'int32', False),
    ('status', 'lowcard', False), ('quantity', 'int16', False), ('amount', 'int32', False),
    ('delivery_fee', 'int32', False), ('distance_m', 'int32', False), ('prep_s', 'int32', False),
    ('deliver_s', 'int32', False), ('payment_method', 'lowcard', False), ('platform', 'lowcard', False),
    ('app_version', 'lowcard', False), ('region', 'int16', False), ('coupon_code', 'str', True),
    ('courier_id', 'int32', False), ('rating', 'int16', True), ('latency_ms', 'int32', False),
    ('trace_id', 'str', False), ('message', 'str', False),
]
NAMES = [c[0] for c in COLUMNS]


def ts_str(sec):
    return str(np.datetime64(int(sec), 's')).replace('T', ' ')


def ts_sec(s):
    return int(np.datetime64(s.replace(' ', 'T'), 's').astype(np.int64))


def chunk(seed, k, n_total):
    """Columns (dict name -> numpy array / list) of chunk k of a data set of n_total rows."""
    a, b = k * CHUNK, min((k + 1) * CHUNK, n_total)
    n = b - a
    rng = np.random.default_rng([seed, k])
    eid = np.arange(a + 1, b + 1, dtype=np.int64)
    ts = BASE_TS + ((eid - 1) * (DAYS * 86400) // n_total)
    store = (rng.random(n) ** 2 * N_STORES).astype(np.int32) + 1
    cust = rng.integers(1, N_CUSTOMERS + 1, n, dtype=np.int32)
    item = (rng.random(n) ** 1.5 * N_ITEMS).astype(np.int32) + 1
    st = rng.choice(len(STATUSES), n, p=STATUS_P)
    qty = rng.integers(1, 6, n, dtype=np.int16)
    amount = (qty.astype(np.int32) * (300 + (item * 37) % 2000)).astype(np.int32)
    fee = rng.choice(np.array([0, 150, 300, 450], dtype=np.int32), n)
    dist = rng.integers(200, 8001, n, dtype=np.int32)
    prep = rng.integers(300, 1801, n, dtype=np.int32)
    deliver = rng.integers(300, 2401, n, dtype=np.int32)
    pay = rng.integers(0, len(PAYMENTS), n)
    plat = rng.integers(0, len(PLATFORMS), n)
    ver = rng.integers(0, len(VERSIONS), n)
    region = rng.integers(1, 48, n, dtype=np.int16)
    has_coupon = rng.random(n) < 0.10
    coupon_no = rng.integers(0, 100, n)
    courier = rng.integers(1, N_COURIERS + 1, n, dtype=np.int32)
    has_rating = rng.random(n) < 0.30
    rating_v = rng.integers(1, 6, n, dtype=np.int16)
    latency = np.minimum(rng.lognormal(4.0, 0.8, n), 60000).astype(np.int32)
    trace = rng.integers(0, 2**63 - 1, n, dtype=np.int64)
    order = eid // 3 + 1_000_000
    status_s = [STATUSES[i] for i in st]
    return {
        'event_id': eid, 'ts': ts.astype(np.int64), 'order_id': order, 'store_id': store, 'customer_id': cust,
        'item_id': item, 'status': status_s, 'quantity': qty, 'amount': amount, 'delivery_fee': fee,
        'distance_m': dist, 'prep_s': prep, 'deliver_s': deliver,
        'payment_method': [PAYMENTS[i] for i in pay], 'platform': [PLATFORMS[i] for i in plat],
        'app_version': [VERSIONS[i] for i in ver], 'region': region,
        'coupon_code': [f'CP{c:04d}' if h else None for h, c in zip(has_coupon.tolist(), coupon_no.tolist())],
        'courier_id': courier,
        'rating': [int(r) if h else None for h, r in zip(has_rating.tolist(), rating_v.tolist())],
        'latency_ms': latency, 'trace_id': [f'{t:016x}' for t in trace.tolist()],
        'message': [f'order {o} {s} at store {p}' for o, s, p in zip(order.tolist(), status_s, store.tolist())],
        '_status_idx': st,
    }


def rows_of(cols, idx=None):
    """Canonical rows (lists, ts as 'YYYY-MM-DD HH:MM:SS', None for NULL) of a chunk."""
    idx = range(len(cols['event_id'])) if idx is None else idx
    out = []
    for i in idx:
        r = []
        for name, kind, _ in COLUMNS:
            v = cols[name][i]
            if v is None:
                r.append(None)
            elif kind == 'ts':
                r.append(ts_str(v))
            elif kind in ('str', 'lowcard'):
                r.append(v)
            else:
                r.append(int(v))
        out.append(r)
    return out


def all_rows(seed, n):
    """All canonical rows of a (small) data set."""
    out = []
    for k in range((n + CHUNK - 1) // CHUNK):
        out += rows_of(chunk(seed, k, n))
    return out


def _table(cols):
    arrs = []
    for name, kind, _ in COLUMNS:
        v = cols[name]
        if kind == 'ts':
            arrs.append(pa.array(v, pa.timestamp('s')))
        elif name == 'rating':
            arrs.append(pa.array(v, pa.int16()))
        else:
            arrs.append(pa.array(v))
    return pa.table(arrs, names=NAMES)


def pick_params(seed, n):
    """Query parameters, fixed by the seed (not by the data)."""
    rng = np.random.default_rng([seed, 999_999])
    return {
        'point_ids': sorted(int(x) for x in rng.integers(1, n + 1, C.REPS + 1)),
        'customers': sorted(int(x) for x in rng.choice(np.arange(1, N_CUSTOMERS + 1), C.REPS + 1, replace=False)),
        # 10-minute windows, one per repetition (+ warm-up), spread over the 30 days
        'windows': [[ts_str(BASE_TS + d * 86400 + 12 * 3600), ts_str(BASE_TS + d * 86400 + 12 * 3600 + 600)]
                    for d in (2, 6, 10, 13, 17, 21, 25, 28)][:C.REPS + 1],
    }


def paths(name):
    d = os.path.join(C.DATA, name)
    return {'dir': d, 'csv': os.path.join(d, 'events.csv'), 'ndjson': os.path.join(d, 'events.ndjson'),
            'truth': os.path.join(d, 'truth.json')}


def write_csv(path, rows, header=True):
    """Small helper for the small experiments: canonical rows -> CSV (empty = NULL)."""
    import csv
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.writer(f, lineterminator='\n')
        if header:
            w.writerow(NAMES)
        for r in rows:
            w.writerow(['' if v is None else v for v in r])


def ensure(name, n, seed, ndjson=False):
    """Generate data set ``name`` (n rows) unless it is already complete. Returns the truth."""
    p = paths(name)
    if os.path.exists(p['truth']):
        with open(p['truth'], encoding='utf-8') as f:
            t = json.load(f)
        if t['n'] == n and t['seed'] == seed and os.path.exists(p['csv']) and (not ndjson or os.path.exists(p['ndjson'])):
            return t
    os.makedirs(p['dir'], exist_ok=True)
    os.chmod(p['dir'], 0o755)
    for f in ('truth', 'csv', 'ndjson'):
        if os.path.exists(p[f]):
            os.remove(p[f])
    prm = pick_params(seed, n)
    win = [(ts_sec(a), ts_sec(b)) for a, b in prm['windows']]
    store_n = np.zeros(N_STORES + 1, dtype=np.int64)
    store_sum = np.zeros(N_STORES + 1, dtype=np.int64)
    item_sum = np.zeros(N_ITEMS + 1, dtype=np.int64)
    day_qty = np.zeros(DAYS, dtype=np.int64)
    day_n = np.zeros(DAYS, dtype=np.int64)
    day_amount = np.zeros(DAYS, dtype=np.int64)
    total_amount, coupon_nonnull, rating_sum = 0, 0, 0
    points, customers, windows = {}, {str(c): [] for c in prm['customers']}, [[] for _ in win]
    completed = STATUSES.index('completed')
    writer = None
    jf = open(p['ndjson'] + '.tmp', 'w', encoding='utf-8', newline='\n') if ndjson else None
    for k in range((n + CHUNK - 1) // CHUNK):
        cols = chunk(seed, k, n)
        tbl = _table(cols)
        if writer is None:
            writer = pacsv.CSVWriter(p['csv'] + '.tmp', tbl.schema, write_options=pacsv.WriteOptions(include_header=True))
        writer.write_table(tbl)
        if jf:
            for r in rows_of(cols):
                jf.write(json.dumps({nm: v for nm, v in zip(NAMES, r) if v is not None}, separators=(',', ':')) + '\n')
        amt = cols['amount'].astype(np.int64)
        store_n += np.bincount(cols['store_id'], minlength=N_STORES + 1)
        store_sum += np.bincount(cols['store_id'], weights=amt, minlength=N_STORES + 1).astype(np.int64)
        item_sum += np.bincount(cols['item_id'], weights=amt, minlength=N_ITEMS + 1).astype(np.int64)
        day = ((cols['ts'] - BASE_TS) // 86400).astype(np.int64)
        done = cols['_status_idx'] == completed
        day_qty += np.bincount(day[done], weights=cols['quantity'][done], minlength=DAYS).astype(np.int64)
        day_n += np.bincount(day, minlength=DAYS)
        day_amount += np.bincount(day, weights=amt, minlength=DAYS).astype(np.int64)
        total_amount += int(amt.sum())
        coupon_nonnull += sum(1 for c in cols['coupon_code'] if c is not None)
        rating_sum += sum(r for r in cols['rating'] if r is not None)
        a = k * CHUNK
        for pid in prm['point_ids']:
            if a < pid <= a + len(amt):
                points[str(pid)] = rows_of(cols, [pid - 1 - a])[0]
        for c in prm['customers']:
            hit = np.nonzero(cols['customer_id'] == c)[0]
            customers[str(c)] += rows_of(cols, hit.tolist())
        for wi, (lo, hi) in enumerate(win):
            hit = np.nonzero((cols['ts'] >= lo) & (cols['ts'] < hi))[0].tolist()
            windows[wi] += [[int(cols['event_id'][i]), int(cols['store_id'][i]), int(cols['amount'][i]), cols['status'][i]]
                            for i in hit]
    writer.close()
    os.replace(p['csv'] + '.tmp', p['csv'])
    os.chmod(p['csv'], 0o644)
    if jf:
        jf.close()
        os.replace(p['ndjson'] + '.tmp', p['ndjson'])
        os.chmod(p['ndjson'], 0o644)
    top = sorted(((int(s), i) for i, s in enumerate(item_sum.tolist()) if s), key=lambda x: (-x[0], x[1]))[:10]
    truth = {
        'name': name, 'n': n, 'seed': seed, 'params': prm, 'total_amount': total_amount,
        'coupon_nonnull': coupon_nonnull, 'rating_sum': int(rating_sum),
        'by_store': [[i, int(store_n[i]), int(store_sum[i])] for i in range(1, N_STORES + 1) if store_n[i]],
        'completed_qty_by_day': [[ts_str(BASE_TS + d * 86400)[:10], int(day_qty[d])] for d in range(DAYS) if day_qty[d]],
        'top_items': [[i, s] for s, i in top],
        'rows_by_day': day_n.tolist(), 'amount_by_day': day_amount.tolist(),
        'points': points, 'customers': customers, 'windows': windows,
        'csv_bytes': os.path.getsize(p['csv']),
        'ndjson_bytes': os.path.getsize(p['ndjson']) if ndjson else None,
    }
    with open(p['truth'], 'w', encoding='utf-8') as f:
        json.dump(truth, f)
    return truth
