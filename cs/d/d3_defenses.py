"""D-3: what each defence costs (PostgreSQL).

Two workloads, both generated from a seed:

inv  Stock reservation with shortage. K items, INITIAL in stock, DEMAND requests
     per item. Exactly INITIAL requests per item must be served and the rest
     rejected as out of stock. Counts lost updates, overselling, legitimate
     requests that failed, retries and latency.
ws   Write skew. Each area has two couriers on duty and both ask for a break at
     the same time; the rule is "at least one on duty". Exactly one request per
     area must be served.
"""
import random
import time

from ..common.record import lat_summary
from .db import DBError, connect, run_txn
from .judges import duty_judge, inventory_judge
from .pool import run_pool, tally

K_ITEMS, INITIAL, DEMAND = 8, 30, 50
AREAS = 80
CONCURRENCY = [1, 4, 16]
TRIALS = 5
SEED_BASE = 3000


# ---------------------------------------------------------------- inventory
def gen_inv_ops(seed):
    ops = [i for i in range(1, K_ITEMS + 1) for _ in range(DEMAND)]
    random.Random(seed).shuffle(ops)
    return ops


def setup_inv(check):
    c = connect('pg')
    try:
        c.q('drop table if exists stock')
        c.q('create table stock (id int primary key, qty int not null, ver int not null default 0'
            + (', constraint stock_qty_nonneg check (qty >= 0)' if check else '') + ')')
        for i in range(1, K_ITEMS + 1):
            c.q('insert into stock (id, qty) values (%s, %s)', (i, INITIAL))
    finally:
        c.close()


def _rmw(for_update=False):
    def body_for(item):
        def body(c):
            qty = c.q('select qty from stock where id = %s' + (' for update' if for_update else ''), (item,))[0][0]
            if qty < 1:
                return 'rejected'
            c.q('update stock set qty = %s where id = %s', (qty - 1, item))
            return 'served'
        return body
    return body_for


def _atomic_guard(item):
    def body(c):
        n = c.q('update stock set qty = qty - 1 where id = %s and qty >= 1', (item,))
        return 'served' if n == 1 else 'rejected'
    return body


def _atomic_plain(item):
    def body(c):
        c.q('update stock set qty = qty - 1 where id = %s', (item,))
        return 'served'
    return body


def _version(item):
    def body(c):
        qty, ver = c.q('select qty, ver from stock where id = %s', (item,))[0]
        if qty < 1:
            return 'rejected'
        n = c.q('update stock set qty = %s, ver = ver + 1 where id = %s and ver = %s', (qty - 1, item, ver))
        if n != 1:
            raise DBError('app:version', 'row changed since it was read')
        return 'served'
    return body


# name -> (isolation level, CHECK constraint, body factory, max attempts)
INV_STRATEGIES = {
    'naive_rc': ('rc', False, _rmw(), 1),
    'check_only': ('rc', True, _rmw(), 1),
    'atomic': ('rc', False, _atomic_guard, 1),
    'atomic_check': ('rc', True, _atomic_plain, 1),
    'for_update': ('rc', False, _rmw(for_update=True), 1),
    'version': ('rc', False, _version, 50),
    'rr_retry': ('rr', False, _rmw(), 50),
    'ser_retry': ('ser', False, _rmw(), 50),
    'ser_noretry': ('ser', False, _rmw(), 1),
}
INV_JA = {
    'naive_rc': '素朴（Read Committed で読んで書く）',
    'check_only': '素朴＋CHECK 制約',
    'atomic': 'アトミックな更新（条件つき UPDATE）',
    'atomic_check': 'アトミックな更新＋CHECK 制約',
    'for_update': 'SELECT FOR UPDATE',
    'version': 'バージョン列（楽観ロック＋再試行）',
    'rr_retry': 'Repeatable Read＋再試行',
    'ser_retry': 'Serializable＋再試行',
    'ser_noretry': 'Serializable（再試行なし）',
}


def _op(body_for, attempts):
    def op(conn, item):
        try:
            return run_txn(conn, body_for(item), max_attempts=attempts)
        except DBError as e:
            if e.code == '23514':            # CHECK violated: the request is out of stock
                return 'rejected', getattr(e, 'retries', 0), getattr(e, 'codes', [e.code])
            raise
    return op


def run_inv(strategy, conc, seed):
    level, check, body_for, attempts = INV_STRATEGIES[strategy]
    ops = gen_inv_ops(seed)
    setup_inv(check)
    results, dur = run_pool('pg', level, ops, conc, _op(body_for, attempts))
    c = connect('pg')
    try:
        final = {r[0]: r[1] for r in c.q('select id, qty from stock order by id')}
    finally:
        c.close()
    served = {}
    for op, r in zip(ops, results):
        if r['outcome'] == 'served':
            served[op] = served.get(op, 0) + 1
    initial = {i: INITIAL for i in range(1, K_ITEMS + 1)}
    demand = {i: DEMAND for i in range(1, K_ITEMS + 1)}
    j = inventory_judge(initial, demand, served, final)
    return {'n_ops': len(ops), **tally(results), **j,
            'latency': lat_summary([r['ms'] for r in results]), 'duration_s': round(dur, 3),
            'final': {str(k): v for k, v in final.items()}}


# ---------------------------------------------------------------- write skew
def gen_ws_ops(seed):
    areas = list(range(1, AREAS + 1))
    random.Random(seed).shuffle(areas)
    return [(a, cid) for a in areas for cid in (1, 2)]   # the two requests of an area are adjacent


def setup_ws():
    c = connect('pg')
    try:
        c.q('drop table if exists courier')
        c.q('create table courier (area int not null, cid int not null, on_duty boolean not null, primary key (area, cid))')
        c.q('insert into courier select a, c, true from generate_series(1, %s) a, generate_series(1, 2) c', (AREAS,))
        c.q('analyze courier')
    finally:
        c.close()


def _break(lock_premise):
    def body_for(op):
        area, cid = op

        def body(c):
            if lock_premise:
                n = len(c.q('select cid from courier where area = %s and on_duty for update', (area,)))
            else:
                n = c.q('select count(*) from courier where area = %s and on_duty', (area,))[0][0]
            if n < 2:
                return 'rejected'
            c.q('update courier set on_duty = false where area = %s and cid = %s', (area, cid))
            return 'served'
        return body
    return body_for


WS_STRATEGIES = {
    'rc_naive': ('rc', _break(False), 1),
    'rr_naive': ('rr', _break(False), 50),
    'lock_premise': ('rc', _break(True), 1),
    'ser_retry': ('ser', _break(False), 50),
    'ser_noretry': ('ser', _break(False), 1),
}
WS_JA = {
    'rc_naive': '素朴（Read Committed）',
    'rr_naive': 'Repeatable Read（スナップショット分離）＋再試行',
    'lock_premise': '前提にした行を SELECT FOR UPDATE',
    'ser_retry': 'Serializable＋再試行',
    'ser_noretry': 'Serializable（再試行なし）',
}


def run_ws(strategy, conc, seed):
    level, body_for, attempts = WS_STRATEGIES[strategy]
    ops = gen_ws_ops(seed)
    setup_ws()
    results, dur = run_pool('pg', level, ops, conc, lambda conn, op: run_txn(conn, body_for(op), max_attempts=attempts))
    c = connect('pg')
    try:
        final = {r[0]: r[1] for r in c.q(
            'select area, count(*) filter (where on_duty) from courier group by area order by area')}
    finally:
        c.close()
    served = {}
    for (area, _), r in zip(ops, results):
        if r['outcome'] == 'served':
            served[area] = served.get(area, 0) + 1
    j = duty_judge(final, served)
    j = {('ws_' + k if k in ('served', 'false_rejects') else k): v for k, v in j.items()}
    return {'n_ops': len(ops), 'areas': AREAS, **tally(results), **j,
            'latency': lat_summary([r['ms'] for r in results]), 'duration_s': round(dur, 3)}


def run(res, version, log=print):
    for wl, strategies, fn in (('inv', INV_STRATEGIES, run_inv), ('ws', WS_STRATEGIES, run_ws)):
        for strategy in strategies:
            for conc in CONCURRENCY:
                for trial in range(1, TRIALS + 1):
                    rid = f'd3/{wl}/{strategy}/c{conc}/t{trial}'
                    if res.has(rid):
                        continue
                    seed = SEED_BASE + trial
                    r = fn(strategy, conc, seed)
                    res.append({'run_id': rid, 'exp': 'd3', 'workload': wl, 'product': 'pg',
                                'strategy': strategy, 'concurrency': conc, 'trial': trial, 'seed': seed,
                                'version': version, **r})
            log(f'  d3 {wl} {strategy}: done')
