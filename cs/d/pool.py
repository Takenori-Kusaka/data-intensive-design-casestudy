"""Run a fixed list of operations on N concurrent connections.

Operation i goes to worker i % N, so the split is fixed by the generator and
not by timing. Each worker runs its operations in order."""
import threading
import time

from .db import DBError, connect


def run_pool(product, level, ops, conc, op_fn, lock_timeout_s=20):
    """op_fn(conn, op) -> (outcome, retries, codes). Returns per-op dicts in op order."""
    results = [None] * len(ops)
    barrier = threading.Barrier(conc)
    errors = []

    def worker(w):
        try:
            conn = connect(product, lock_timeout_s=lock_timeout_s)
            conn.set_level(level)
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))
            barrier.abort()
            return
        try:
            barrier.wait(60)
        except threading.BrokenBarrierError:
            conn.close()
            return
        for idx in range(w, len(ops), conc):
            t0 = time.monotonic()
            try:
                outcome, retries, codes = op_fn(conn, ops[idx])
            except DBError as e:
                outcome, retries, codes = 'failed', getattr(e, 'retries', 0), getattr(e, 'codes', [e.code])
            results[idx] = {'outcome': outcome, 'retries': retries, 'codes': codes,
                            'ms': (time.monotonic() - t0) * 1000}
        conn.close()

    t0 = time.monotonic()
    th = [threading.Thread(target=worker, args=(w,), daemon=True) for w in range(conc)]
    for t in th:
        t.start()
    for t in th:
        t.join()
    if errors:
        raise RuntimeError(f'worker start failed: {errors[:2]}')
    return results, time.monotonic() - t0


def tally(results):
    codes = {}
    for r in results:
        for c in r['codes']:
            codes[c] = codes.get(c, 0) + 1
    out = {'ops': len(results), 'retries': sum(r['retries'] for r in results), 'error_codes': codes}
    for k in ('served', 'rejected', 'failed'):
        out[k] = sum(1 for r in results if r['outcome'] == k)
    return out
