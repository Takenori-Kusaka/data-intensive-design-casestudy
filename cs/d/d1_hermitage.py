"""D-1: anomalies per isolation level, with a fixed statement order across sessions.

The scenarios follow the method of Hermitage (https://github.com/ept/hermitage,
Martin Kleppmann, CC BY 4.0): two or three sessions, a fixed interleaving, and a
check of what each session saw. The statement lists are re-implemented here and
extended (P2, P3, P3W, MAVW are additions of this case study). Verdicts are
computed from the rows each session read and from the final table contents,
not from messages.

Every scenario starts from: test(id int primary key, value int) = (1,10), (2,20).
"""
import itertools
import queue
import threading
import time

from . import config as C
from .db import DBError, connect

BLOCK_T = {'pg': 0.6, 'mysql': 0.6, 'crdb': 1.2}   # seconds without a reply -> "blocked"
LOCK_TIMEOUT_S = 6
SEL_ALL = 'select id, value from test order by id'


def _sel(where):
    return f'select id, value from test where {where} order by id'


# (session, sql, tag)
TESTS = [
    {'id': 'G0', 'ja': 'ダーティライト（G0）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T1', 'update test set value = 21 where id = 2'),
        ('T1', 'COMMIT'),
        ('T2', 'update test set value = 22 where id = 2'),
        ('T2', 'COMMIT')]},
    {'id': 'G1a', 'ja': 'ダーティリード：取り消された値を読む（G1a）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = 101 where id = 1'),
        ('T2', SEL_ALL, 'r1'),
        ('T1', 'ROLLBACK'),
        ('T2', SEL_ALL, 'r2'),
        ('T2', 'COMMIT')]},
    {'id': 'G1b', 'ja': 'ダーティリード：途中の値を読む（G1b）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = 101 where id = 1'),
        ('T2', SEL_ALL, 'r1'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T1', 'COMMIT'),
        ('T2', SEL_ALL, 'r2'),
        ('T2', 'COMMIT')]},
    {'id': 'G1c', 'ja': '未コミットの値を互いに読む（G1c）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T2', 'update test set value = 22 where id = 2'),
        ('T1', _sel('id = 2'), 'r1'),
        ('T2', _sel('id = 1'), 'r2'),
        ('T1', 'COMMIT'), ('T2', 'COMMIT')]},
    {'id': 'OTV', 'ja': '見えた更新が消える（OTV）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'), ('T3', 'BEGIN'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T1', 'update test set value = 19 where id = 2'),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T1', 'COMMIT'),
        ('T3', SEL_ALL, 'r1'),
        ('T2', 'update test set value = 18 where id = 2'),
        ('T3', SEL_ALL, 'r2'),
        ('T2', 'COMMIT'),
        ('T3', SEL_ALL, 'r3'),
        ('T3', 'COMMIT')]},
    {'id': 'P2', 'ja': 'ノンリピータブルリード：同じ行を 2 回読む（P2）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id = 1'), 'r1'),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T2', 'COMMIT'),
        ('T1', _sel('id = 1'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'P3', 'ja': 'ファントム：同じ条件で 2 回読む（P3）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('value >= 10'), 'r1'),
        ('T2', 'insert into test (id, value) values (3, 30)'),
        ('T2', 'COMMIT'),
        ('T1', _sel('value >= 10'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'PMP', 'ja': 'ファントム：別の条件で読む（PMP）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('value = 30'), 'r1'),
        ('T2', 'insert into test (id, value) values (3, 30)'),
        ('T2', 'COMMIT'),
        ('T1', _sel('value % 3 = 0'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'PMPW', 'ja': 'ファントム：書き込みの条件（PMP・書き込み）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = value + 10'),
        ('T2', 'delete from test where value = 20'),
        ('T1', 'COMMIT'),
        ('T2', SEL_ALL, 'r1'),
        ('T2', 'COMMIT')]},
    {'id': 'PMPW2', 'ja': 'ファントム：読んでから条件で消す（PMP・書き込み 2）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', 'update test set value = value + 10'),
        ('T2', _sel('value = 20'), 'r0'),
        ('T2', 'delete from test where value = 20'),
        ('T1', 'COMMIT'),
        ('T2', SEL_ALL, 'r1'),
        ('T2', 'COMMIT')]},
    {'id': 'P3W', 'ja': 'ファントム：自分の更新の後に見える行が増える（P3W）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', SEL_ALL, 'r1'),
        ('T2', 'insert into test (id, value) values (3, 30)'),
        ('T2', 'COMMIT'),
        ('T1', 'update test set value = value + 1 where value >= 10'),
        ('T1', SEL_ALL, 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'P4', 'ja': 'ロストアップデート（P4）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id = 1'), 'r1'),
        ('T2', _sel('id = 1'), 'r2'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T2', 'update test set value = 11 where id = 1'),
        ('T1', 'COMMIT'), ('T2', 'COMMIT')]},
    {'id': 'GS', 'ja': 'リードスキュー：読み取りだけ（G-single）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id = 1'), 'r1'),
        ('T2', _sel('id = 1')),
        ('T2', _sel('id = 2')),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T2', 'update test set value = 18 where id = 2'),
        ('T2', 'COMMIT'),
        ('T1', _sel('id = 2'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'GSD', 'ja': 'リードスキュー：条件で読む（G-single・述語）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('value % 5 = 0'), 'r1'),
        ('T2', 'update test set value = 12 where value = 10'),
        ('T2', 'COMMIT'),
        ('T1', _sel('value % 3 = 0'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'GSW', 'ja': 'リードスキュー：読んだ後に書く（G-single・書き込み）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id = 1'), 'r1'),
        ('T2', SEL_ALL),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T2', 'update test set value = 18 where id = 2'),
        ('T2', 'COMMIT'),
        ('T1', 'delete from test where value = 20'),
        ('T1', _sel('id = 2'), 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'MAVW', 'ja': '自分の更新の後、1 回の読み取りに新旧が混ざる（MAVW）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id = 1'), 'r1'),
        ('T2', 'update test set value = 12 where id = 1'),
        ('T2', 'update test set value = 18 where id = 2'),
        ('T2', 'COMMIT'),
        ('T1', 'update test set value = value + 1 where id = 2'),
        ('T1', SEL_ALL, 'r2'),
        ('T1', 'COMMIT')]},
    {'id': 'G2I', 'ja': 'ライトスキュー（G2-item）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('id in (1,2)'), 'r1'),
        ('T2', _sel('id in (1,2)'), 'r2'),
        ('T1', 'update test set value = 11 where id = 1'),
        ('T2', 'update test set value = 21 where id = 2'),
        ('T1', 'COMMIT'), ('T2', 'COMMIT')]},
    {'id': 'G2', 'ja': 'ライトスキュー：条件で読んで挿入（G2）', 'steps': [
        ('T1', 'BEGIN'), ('T2', 'BEGIN'),
        ('T1', _sel('value % 3 = 0'), 'r1'),
        ('T2', _sel('value % 3 = 0'), 'r2'),
        ('T1', 'insert into test (id, value) values (3, 30)'),
        ('T2', 'insert into test (id, value) values (4, 42)'),
        ('T1', 'COMMIT'), ('T2', 'COMMIT')]},
    {'id': 'G2E', 'ja': '3 つのトランザクションの循環（G2・2 辺）', 'steps': [
        ('T1', 'BEGIN'),
        ('T1', SEL_ALL, 'r1'),
        ('T2', 'BEGIN'),
        ('T2', 'update test set value = value + 5 where id = 2'),
        ('T2', 'COMMIT'),
        ('T3', 'BEGIN'),
        ('T3', SEL_ALL, 'r3'),
        ('T3', 'COMMIT'),
        ('T1', 'update test set value = 0 where id = 1'),
        ('T1', 'COMMIT')]},
]
TEST_IDS = [t['id'] for t in TESTS]
TEST_JA = {t['id']: t['ja'] for t in TESTS}


# --------------------------------------------------------------------------
# judges: obs = {'reads': {tag: {id: value} | None}, 'committed': {sess: bool}, 'final': {id: value}}
# return 'anomaly' | 'prevented' | 'inconclusive'
# --------------------------------------------------------------------------
def judge(test_id, obs):
    R, CM, F = obs['reads'], obs['committed'], obs['final']
    both = CM.get('T1') and CM.get('T2')

    def vals(*tags):
        return [R.get(t) for t in tags if R.get(t) is not None]

    if test_id == 'G0':
        return 'anomaly' if (F.get(1), F.get(2)) in {(11, 22), (12, 21)} else 'prevented'
    if test_id in ('G1a', 'G1b'):
        seen = vals('r1', 'r2')
        if not seen:
            return 'inconclusive'
        return 'anomaly' if any(r.get(1) == 101 for r in seen) else 'prevented'
    if test_id == 'G1c':
        return 'anomaly' if R.get('r1') == {2: 22} and R.get('r2') == {1: 11} else 'prevented'
    if test_id == 'OTV':
        order = [(10, 20), (11, 19), (12, 18)]
        seen = [(r.get(1), r.get(2)) for r in vals('r1', 'r2', 'r3')]
        if not seen:
            return 'inconclusive'
        if any(s not in order for s in seen):
            return 'anomaly'
        idx = [order.index(s) for s in seen]
        return 'anomaly' if idx != sorted(idx) else 'prevented'
    if test_id in ('P2', 'P3'):
        if R.get('r1') is None or R.get('r2') is None:
            return 'prevented' if not CM.get('T1') or not CM.get('T2') else 'inconclusive'
        return 'anomaly' if R['r1'] != R['r2'] else 'prevented'
    if test_id == 'PMP':
        if R.get('r2') is None:
            return 'prevented' if not both else 'inconclusive'
        return 'anomaly' if 3 in R['r2'] else 'prevented'
    if test_id == 'PMPW':
        if not both:
            return 'prevented'
        return 'prevented' if F in ({2: 30}, {1: 20}) else 'anomaly'
    if test_id == 'PMPW2':
        if not both:
            return 'prevented'
        # T2 deleted "value = 20"; it must not still see a row with value 20 afterwards
        if R.get('r1') is not None and 20 in R['r1'].values():
            return 'anomaly'
        return 'prevented' if F in ({2: 30}, {1: 20}) else 'anomaly'
    if test_id == 'P3W':
        if not CM.get('T1') or R.get('r2') is None:
            return 'prevented'
        return 'anomaly' if 3 in R['r2'] and 3 not in (R.get('r1') or {}) else 'prevented'
    if test_id == 'P4':
        return 'anomaly' if both and R.get('r1') == {1: 10} and R.get('r2') == {1: 10} else 'prevented'
    if test_id == 'GS':
        if R.get('r1') is None or R.get('r2') is None:
            return 'prevented' if not both else 'inconclusive'
        return 'anomaly' if R['r1'] == {1: 10} and R['r2'] == {2: 18} else 'prevented'
    if test_id == 'GSD':
        if R.get('r2') is None:
            return 'prevented' if not both else 'inconclusive'
        return 'anomaly' if R.get('r1') == {1: 10, 2: 20} and R['r2'] == {1: 12} else 'prevented'
    if test_id == 'GSW':
        if not both:
            return 'prevented'
        # T1 read id=1 before T2. Only "T1 then T2" is a serial order: T1 deletes id=2,
        # then T2 updates id=1 -> {1: 12}. Anything else means T1 acted on a mixed view.
        return 'prevented' if F == {1: 12} else 'anomaly'
    if test_id == 'MAVW':
        if not CM.get('T1') or R.get('r2') is None:
            return 'prevented'
        return 'anomaly' if R['r2'] == {1: 10, 2: 19} else 'prevented'
    if test_id in ('G2I', 'G2'):
        return 'anomaly' if both else 'prevented'
    if test_id == 'G2E':
        allc = both and CM.get('T3')
        return 'anomaly' if allc and R.get('r1') == {1: 10, 2: 20} and R.get('r3') == {1: 10, 2: 25} else 'prevented'
    raise ValueError(test_id)


def mechanism(verdict, steps):
    errs = sorted({s['err'] for s in steps if s.get('err')})
    blocked = any(s.get('blocked') for s in steps)
    parts = []
    if blocked:
        parts.append('wait')
    if errs:
        parts.append('error:' + ','.join(errs))
    if not parts:
        parts.append('none')
    return '+'.join(parts)


# --------------------------------------------------------------------------
# engine
# --------------------------------------------------------------------------
class _Sess(threading.Thread):
    def __init__(self, name, product, level, counter):
        super().__init__(daemon=True)
        self.sname, self.product, self.level = name, product, level
        self.inbox = queue.Queue()
        self.aborted = False
        self.committed = None
        self.ready = threading.Event()
        self.actual = None
        self.counter = counter
        self.fatal = None

    def run(self):
        try:
            self.conn = connect(self.product, lock_timeout_s=LOCK_TIMEOUT_S)
            self.conn.set_level(self.level)
            self.conn.begin()
            self.actual = self.conn.actual_level()
            self.conn.rollback()
        except Exception as e:  # noqa: BLE001
            self.fatal = repr(e)
            self.ready.set()
            return
        self.ready.set()
        while True:
            st = self.inbox.get()
            if st is None:
                break
            self._exec(st)
            st['seq'] = next(self.counter)
            st['_done'].set()
        self.conn.close()

    def _exec(self, st):
        sql = st['sql']
        t0 = time.monotonic()
        try:
            if self.aborted:
                st['status'] = 'skipped'
            elif sql == 'BEGIN':
                self.conn.begin()
                st['status'] = 'ok'
            elif sql == 'COMMIT':
                self.conn.q('COMMIT')
                self.committed = True
                st['status'] = 'ok'
            elif sql == 'ROLLBACK':
                self.conn.q('ROLLBACK')
                self.committed = False
                st['status'] = 'ok'
            else:
                r = self.conn.q(sql)
                st['status'] = 'ok'
                if isinstance(r, list):
                    st['rows'] = [list(x) for x in r]
                else:
                    st['rowcount'] = r
        except DBError as e:
            st['status'] = 'error'
            st['err'] = e.code
            st['msg'] = e.msg
            self.conn.rollback()          # what an application would do
            self.aborted = True
            self.committed = False
        st['ms'] = round((time.monotonic() - t0) * 1000, 1)


def reset_table(product):
    c = connect(product)
    try:
        if product == 'mysql':
            c.q('create table if not exists test (id int primary key, value int) engine=innodb')
        else:
            c.q('create table if not exists test (id int primary key, value int)')
        c.q('delete from test')
        c.q('insert into test (id, value) values (1, 10), (2, 20)')
    finally:
        c.close()


def run_scenario(product, level, test):
    reset_table(product)
    counter = itertools.count(1)
    names = sorted({s[0] for s in test['steps']})
    S = {n: _Sess(n, product, level, counter) for n in names}
    for s in S.values():
        s.start()
    for s in S.values():
        s.ready.wait(20)
        if s.fatal:
            raise RuntimeError(f'session start failed: {s.fatal}')
    steps = []
    for i, raw in enumerate(test['steps']):
        st = {'i': i, 's': raw[0], 'sql': raw[1], 'tag': raw[2] if len(raw) > 2 else None,
              '_done': threading.Event()}
        steps.append(st)
        sess = S[st['s']]
        queued = any(p['s'] == st['s'] and not p['_done'].is_set() for p in steps[:-1])
        sess.inbox.put(st)
        if queued:
            st['queued'] = True          # released while an earlier statement of the session was still waiting
            continue
        if not st['_done'].wait(BLOCK_T[product]):
            st['blocked'] = True
    deadline = time.monotonic() + 45
    for st in steps:
        st['_done'].wait(max(0.1, deadline - time.monotonic()))
    hung = [st['i'] for st in steps if not st['_done'].is_set()]
    for s in S.values():
        s.inbox.put(None)
    for s in S.values():
        s.join(5)
    c = connect(product)
    try:
        final = {r[0]: r[1] for r in c.q(SEL_ALL)}
    finally:
        c.close()
    reads = {}
    for st in steps:
        if st['tag']:
            reads[st['tag']] = {r[0]: r[1] for r in st['rows']} if st.get('status') == 'ok' and 'rows' in st else None
    committed = {n: bool(S[n].committed) for n in names}
    obs = {'reads': reads, 'committed': committed, 'final': final}
    verdict = 'inconclusive' if hung else judge(test['id'], obs)
    clean = [{k: v for k, v in st.items() if not k.startswith('_')} for st in steps]
    return {
        'verdict': verdict, 'mechanism': mechanism(verdict, clean),
        'actual_level': sorted({S[n].actual for n in names}),
        'reads': {k: (None if v is None else {str(a): b for a, b in v.items()}) for k, v in reads.items()},
        'committed': committed, 'final': {str(k): v for k, v in final.items()},
        'steps': clean, 'hung_steps': hung,
    }


def probe_levels(product):
    """What the server reports for each requested level, plus the default."""
    out = {}
    c = connect(product)
    try:
        if product == 'mysql':
            out['default'] = c.q('SELECT @@global.transaction_isolation')[0][0].lower().replace('-', ' ')
        else:
            out['default'] = c.q('SHOW default_transaction_isolation')[0][0].lower()
    finally:
        c.close()
    for lv in C.PRODUCT_LEVELS[product]:
        c = connect(product)
        try:
            c.set_level(lv)
            c.begin()
            out[lv] = c.actual_level()
            c.rollback()
        except DBError as e:
            out[lv] = f'error {e.code}: {e.msg}'
        finally:
            c.close()
    return out


CRDB_SETTINGS = ['sql.txn.read_committed_isolation.enabled', 'sql.txn.repeatable_read_isolation.enabled']


def crdb_settings(enable=None):
    """Read (and optionally set) the cluster settings that gate the weaker levels."""
    out = {}
    c = connect('crdb')
    try:
        for name in CRDB_SETTINGS:
            try:
                if enable is not None:
                    c.q(f'SET CLUSTER SETTING {name} = {"true" if enable else "false"}')
                out[name] = str(c.q(f'SHOW CLUSTER SETTING {name}')[0][0])
            except DBError as e:
                out[name] = f'error {e.code}: {e.msg}'
    finally:
        c.close()
    if enable is not None:
        time.sleep(1.0)
    return out


def run(product, res, version, trials=3, log=print):
    """Run all scenarios for one product (the container must be up)."""
    probes = {}
    if product == 'crdb':
        probes['settings_default'] = crdb_settings()
        probes['levels_default'] = probe_levels(product)
        probes['settings_enabled'] = crdb_settings(enable=True)
        probes['levels_enabled'] = probe_levels(product)
    else:
        probes['levels_default'] = probe_levels(product)
    rid = f'd1/probe/{product}'
    if not res.has(rid):
        res.append({'run_id': rid, 'exp': 'd1_probe', 'product': product, 'version': version, **probes})
    for level in C.PRODUCT_LEVELS[product]:
        for test in TESTS:
            for trial in range(1, trials + 1):
                rid = f'd1/{product}/{level}/{test["id"]}/t{trial}'
                if res.has(rid):
                    continue
                t0 = time.monotonic()
                r = run_scenario(product, level, test)
                res.append({'run_id': rid, 'exp': 'd1', 'product': product, 'level': level,
                            'test': test['id'], 'trial': trial, 'version': version,
                            'duration_s': round(time.monotonic() - t0, 2), **r})
            log(f'  d1 {product} {level} {test["id"]}: done')
