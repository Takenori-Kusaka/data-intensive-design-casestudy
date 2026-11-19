"""B-7: what a "materialized view" follows, product by product (guide Part 12).

  pg:refresh / pg:refresh_concurrently
      PostgreSQL 18: change the base table, count the groups of the view that are stale,
      then REFRESH (plain or CONCURRENTLY) while 4 readers query the view; how long the
      reads stop, and whether the view matches afterwards
  ch:mv
      ClickHouse: an incremental materialized view (insert trigger), one with a join, and a
      refreshable one. After each step (insert, update of source rows, delete of source
      rows, change of the right side of the join, drop of a partition, refresh) the views
      are compared with the same query run on the source tables
"""
import random
import time

from . import config as C, infra
from .load import Load, LockSampler, window

PG_ROWS, PG_GROUPS = 600_000, 50_000
CH_ROWS, CH_STORES = 100_000, 200
TRIALS = 3
CASES = [('pg', 'refresh'), ('pg', 'refresh_concurrently'), ('ch', 'mv')]
CASE_JA = {'refresh': 'REFRESH MATERIALIZED VIEW', 'refresh_concurrently': 'REFRESH MATERIALIZED VIEW CONCURRENTLY',
           'mv': 'ClickHouse のマテリアライズドビュー'}


def plan():
    return [{'exp': 'b7', 'product': p, 'case': c, 'trial': t, 'seed': C.SEED0 + t}
            for p, c in CASES for t in range(1, TRIALS + 1)]


def run_id_of(s):
    return f'b7:{s["product"]}:{s["case"]}:t{s["trial"]}'


def diff_counts(expected, got):
    """Compare a view with the query on the source: {key: tuple of values}."""
    return {'keys_expected': len(expected), 'missing': len(set(expected) - set(got)),
            'extra': len(set(got) - set(expected)),
            'value_mismatch': sum(1 for k, v in expected.items() if k in got and tuple(got[k]) != tuple(v))}


def n_diff(d):
    return d['missing'] + d['extra'] + d['value_mismatch']


def audit_cases():
    exp = {1: (2, 10), 2: (3, 30), 3: (1, 5)}
    return [
        ('diff_clean', lambda: diff_counts(exp, dict(exp)), {'keys_expected': 3}),
        ('diff_planted', lambda: diff_counts(exp, {1: (2, 11), 3: (1, 5), 9: (1, 1)}),
         {'keys_expected': 3, 'missing': 1, 'extra': 1, 'value_mismatch': 1}),
    ]


# ------------------------------------------------------------------ PostgreSQL
def _pg_view():
    return {r[0]: (r[1], int(r[2])) for r in infra.q('pg', 'select store, n, total from mv_sales')}


def _pg_source():
    return {r[0]: (r[1], int(r[2])) for r in infra.q('pg', 'select store, count(*), sum(amount) from sales group by store')}


def run_pg(spec):
    conc = spec['case'] == 'refresh_concurrently'
    infra.pg_up('pg')
    with infra.pg('pg') as c:
        c.execute('create table sales (id int primary key, store int not null, amount int not null)')
        c.execute('insert into sales select g, g %% %s, g %% 1000 from generate_series(1, %s) g', (PG_GROUPS, PG_ROWS))
        c.execute('create materialized view mv_sales as select store, count(*) as n, sum(amount) as total '
                  'from sales group by store')
        c.execute('create unique index mv_sales_store on mv_sales (store)')
        c.execute('vacuum analyze')
    out = {'rows': PG_ROWS, 'groups': PG_GROUPS, 'after_create': diff_counts(_pg_source(), _pg_view())}
    with infra.pg('pg') as c:      # changes of the base table: update, delete, insert
        c.execute('update sales set amount = amount + 1 where id <= 5000')
        c.execute('delete from sales where id between 100001 and 101000')
        c.execute('insert into sales select g, g %% %s, 1 from generate_series(%s, %s) g',
                  (PG_GROUPS, PG_ROWS + 1, PG_ROWS + 1000))
    out['after_base_change'] = diff_counts(_pg_source(), _pg_view())

    def request(_kind, conn, ctx):
        rng = ctx.setdefault('rng', random.Random(f'{spec["seed"]}:{ctx["thread"]}'))
        conn.execute('select n, total from mv_sales where store = %s', (rng.randrange(PG_GROUPS),)).fetchone()
    ld = Load(lambda _ctx: infra.pg('pg'), request, n_writers=0, n_readers=4, timeout_ms=10000).start()
    time.sleep(1.0)
    ddl = infra.pg('pg')
    sampler = LockSampler(lambda: infra.pg('pg'))
    sampler.pid = ddl.execute('select pg_backend_pid()').fetchone()[0]
    sampler.start()
    sql = 'refresh materialized view ' + ('concurrently ' if conc else '') + 'mv_sales'
    t_a = time.monotonic()
    ddl.execute(sql)
    t_b = time.monotonic()
    ddl.close()
    time.sleep(0.5)
    waits = sampler.stop()
    ops = ld.stop()
    r = window(ops, 'r', t_a, t_b + 0.2)
    out.update({'sql': sql, 'refresh_ms': round((t_b - t_a) * 1000, 1), 'read': r,
                'baseline_read': window(ops, 'r', t_a - 0.8, t_a), 'lock_waits': waits,
                'locks_sampled': sorted({(rel, m) for rel, m, _g in sampler.modes if rel == 'mv_sales'}),
                'after_refresh': diff_counts(_pg_source(), _pg_view()),
                'versions': {'pg': infra.q('pg', 'show server_version')[0][0]}})
    out['brief'] = (f'stale_before={n_diff(out["after_base_change"])} refresh={out["refresh_ms"]}ms '
                    f'rgap={r["max_gap_ms"]} rmax={r["max_ms"]} after={n_diff(out["after_refresh"])} locks={out["locks_sampled"]}')
    return out


# ------------------------------------------------------------------ ClickHouse
def _ch_map(sql):
    return {int(r[0]) if r[0].lstrip('-').isdigit() else r[0]: tuple(int(x) for x in r[1:]) for r in infra.ch(sql)}


SRC_BY_STORE = 'select store, count(), sum(amount) from orders_src group by store'
SRC_BY_REGION = ('select d.region, sum(o.amount) from orders_src o join stores_dim d on o.store = d.store '
                 'group by d.region')


def _ch_compare():
    return {
        'incremental': diff_counts(_ch_map(SRC_BY_STORE), _ch_map('select store, sum(n), sum(total) from agg_inc group by store')),
        'incremental_join': diff_counts(_ch_map(SRC_BY_REGION), _ch_map('select region, sum(total) from agg_join group by region')),
        'refreshable': diff_counts(_ch_map(SRC_BY_STORE), _ch_map('select store, n, total from mv_ref')),
    }


def run_ch(spec):
    rng = random.Random(spec['seed'])
    infra.ch_up()
    sync = {'mutations_sync': '2'}
    for sql in [
        'create table orders_src (order_id UInt32, store UInt32, amount Int64) engine = MergeTree '
        'partition by intDiv(order_id, 50000) order by order_id',
        'create table stores_dim (store UInt32, region String) engine = MergeTree order by store',
        f"insert into stores_dim select number, concat('r', toString(number % 8)) from numbers({CH_STORES})",
        'create table agg_inc (store UInt32, n UInt64, total Int64) engine = SummingMergeTree order by store',
        'create materialized view mv_inc to agg_inc as select store, count() as n, sum(amount) as total '
        'from orders_src group by store',
        'create table agg_join (region String, total Int64) engine = SummingMergeTree order by region',
        'create materialized view mv_join to agg_join as select d.region as region, sum(o.amount) as total '
        'from orders_src o join stores_dim d on o.store = d.store group by d.region',
        'create materialized view mv_ref refresh every 1 hour engine = MergeTree order by store as '
        'select store, count() as n, sum(amount) as total from orders_src group by store',
    ]:
        infra.ch(sql)
    steps = []

    def step(name, ja):
        steps.append({'step': name, 'label': ja, **_ch_compare()})
    block = CH_ROWS // 10
    for b in range(10):       # ten insert blocks; values from the seed
        salt = rng.randrange(1, 1000)
        infra.ch(f'insert into orders_src select number + 1 + {b * block}, (number * 7 + {salt}) % {CH_STORES}, '
                 f'(number * 13 + {salt}) % 5000 from numbers({block})')
    infra.ch('system refresh view mv_ref')
    infra.ch('system wait view mv_ref')
    step('insert', '元の表に 10 回に分けて挿入（refreshable は挿入の後に 1 回更新）')
    infra.ch("alter table stores_dim update region = 'moved' where store < 20", sync)
    step('change_join_right', '結合の右側の表（店の属性）を更新')
    infra.ch('alter table orders_src update amount = amount + 1000 where order_id % 10 = 0', sync)
    step('update_source', '元の行を更新（ALTER TABLE … UPDATE）')
    infra.ch('alter table orders_src delete where order_id % 7 = 0', sync)
    step('delete_source', '元の行を削除（ALTER TABLE … DELETE）')
    infra.ch('delete from orders_src where order_id % 11 = 0')
    step('lightweight_delete', '元の行を削除（DELETE FROM：論理削除）')
    infra.ch('alter table orders_src drop partition 0')
    step('drop_partition', '元の表のパーティションを削除')
    infra.ch(f'insert into orders_src select number + 1 + {CH_ROWS}, number % {CH_STORES}, 1 from numbers(1000)')
    step('insert_again', 'さらに 1000 行を挿入')
    t0 = time.monotonic()
    infra.ch('system refresh view mv_ref')
    infra.ch('system wait view mv_ref')
    refresh_ms = round((time.monotonic() - t0) * 1000, 1)
    step('refresh', 'refreshable を更新（SYSTEM REFRESH VIEW）')
    ver = infra.ch('select version()')[0][0]
    brief = ' '.join(f'{s["step"]}={n_diff(s["incremental"])}/{n_diff(s["incremental_join"])}/{n_diff(s["refreshable"])}'
                     for s in steps)
    return {'rows': CH_ROWS, 'stores': CH_STORES, 'steps': steps, 'refresh_ms': refresh_ms,
            'versions': {'clickhouse': ver}, 'brief': brief}


def run_one(spec, log=print):
    t0 = time.monotonic()
    infra.down()
    body = run_pg(spec) if spec['product'] == 'pg' else run_ch(spec)
    row = {'run_id': run_id_of(spec), **spec, **body, 'container_memory': infra.mem()}
    infra.down()
    row['seconds'] = round(time.monotonic() - t0, 1)
    return row
