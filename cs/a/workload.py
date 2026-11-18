"""Deterministic workload generator for topic A.

A run has ``concurrency`` writers (w00, w01, ...). Each writer issues a fixed
list of operations; operation ``seq`` of writer ``w`` sets one order to one
status and has the event id ``<w>-<seq>``. The lists depend only on the seed,
so the expected set of events comes from here and never from experiment output.

Writers are spread over at most two order-service instances (processes): two
writers updating the same order may therefore sit in the same process or in
different processes.
"""
import random

from . import config as C


def writers(concurrency):
    return [f'w{i:02d}' for i in range(concurrency)]


def instances(concurrency):
    """[[writer ids of instance 0], [writer ids of instance 1]] (one list when concurrency is 1)."""
    ws = writers(concurrency)
    n = min(2, concurrency)
    return [ws[i::n] for i in range(n)]


def ops_for(writer, seed, n_orders, n_ops_total, concurrency):
    """[(seq, order_id, status)] for one writer; seq starts at 1."""
    per = n_ops_total // concurrency
    rnd = random.Random(f'{seed}:{writer}')
    return [(s, rnd.randint(1, n_orders), rnd.choice(C.STATUSES)) for s in range(1, per + 1)]


def event_id(writer, seq):
    return f'{writer}-{seq}'


def expected(seed, n_orders, n_ops_total, concurrency, first_seq=1, id_prefix=''):
    """{event_id: (order_id, status, writer, seq)} for the whole run."""
    out = {}
    for w in writers(concurrency):
        for s, o, st in ops_for(w, seed, n_orders, n_ops_total, concurrency):
            out[id_prefix + event_id(w, s)] = (o, st, w, s)
    return out
