"""D-2: how often a read-compute-write reservation loses updates under load.

Workload (generator, seeded): N_OPS reservations spread over K_ITEMS hot items.
Each reservation is one transaction: read qty, compute qty - 1 in the client,
write it back. Serialization failures and deadlocks are retried (whole
transaction). Stock is large, so no request is ever out of stock: every
acknowledged reservation must lower the stock by exactly one.
"""
import random
import time

from ..common.record import lat_summary
from .db import connect, run_txn
from .judges import inventory_judge
from .pool import run_pool, tally

N_OPS = 400
K_ITEMS = 4
INITIAL = 100000
CONCURRENCY = [1, 4, 16]
TRIALS = 5
SEED_BASE = 2000
D2_LEVELS = {'pg': ['rc', 'rr', 'ser'], 'mysql': ['ru', 'rc', 'rr', 'ser'], 'crdb': ['rc', 'rr', 'ser']}


def gen_ops(seed):
    rnd = random.Random(seed)
    return [rnd.randrange(1, K_ITEMS + 1) for _ in range(N_OPS)]


def setup(product):
    c = connect(product)
    try:
        c.q('drop table if exists stock')
        c.q('create table stock (id int primary key, qty int not null)' + (' engine=innodb' if product == 'mysql' else ''))
        for i in range(1, K_ITEMS + 1):
            c.q('insert into stock (id, qty) values (%s, %s)', (i, INITIAL))
    finally:
        c.close()


def _reserve(conn, item):
    def body(c):
        qty = c.q('select qty from stock where id = %s', (item,))[0][0]
        c.q('update stock set qty = %s where id = %s', (qty - 1, item))
        return 'served'
    return run_txn(conn, body, max_attempts=50)


def run_one(product, level, conc, seed):
    ops = gen_ops(seed)
    setup(product)
    results, dur = run_pool(product, level, ops, conc, _reserve)
    c = connect(product)
    try:
        final = {r[0]: r[1] for r in c.q('select id, qty from stock order by id')}
    finally:
        c.close()
    served = {}
    demand = {}
    for op, r in zip(ops, results):
        demand[op] = demand.get(op, 0) + 1
        if r['outcome'] == 'served':
            served[op] = served.get(op, 0) + 1
    initial = {i: INITIAL for i in range(1, K_ITEMS + 1)}
    j = inventory_judge(initial, demand, served, final)
    return {'n_ops': N_OPS, 'k_items': K_ITEMS, **tally(results), **j,
            'latency': lat_summary([r['ms'] for r in results]), 'duration_s': round(dur, 3),
            'final': {str(k): v for k, v in final.items()}}


def run(product, res, version, log=print):
    for level in D2_LEVELS[product]:
        for conc in CONCURRENCY:
            for trial in range(1, TRIALS + 1):
                rid = f'd2/{product}/{level}/c{conc}/t{trial}'
                if res.has(rid):
                    continue
                seed = SEED_BASE + trial
                t0 = time.monotonic()
                r = run_one(product, level, conc, seed)
                res.append({'run_id': rid, 'exp': 'd2', 'product': product, 'level': level,
                            'concurrency': conc, 'trial': trial, 'seed': seed, 'version': version,
                            'wall_s': round(time.monotonic() - t0, 2), **r})
            log(f'  d2 {product} {level} c{conc}: done')
