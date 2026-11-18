"""Judges for topic D. Pure functions: they take the expected values from the
generator, the set of acknowledged operations and the final DB state, and
count anomalies. Audited by audit.py with planted errors."""


def inventory_judge(initial, demand, served, final):
    """In-stock reservation. All arguments are dicts keyed by item.
    initial: stock at start (generator); demand: number of requests (generator);
    served: requests acknowledged as reserved; final: stock read at the end."""
    out = {'lost_updates': 0, 'unacked_decrements': 0, 'oversold': 0, 'negative_items': 0,
           'false_rejects': 0, 'served': 0, 'expected_served': 0}
    for item, init in initial.items():
        s, f, dmd = served.get(item, 0), final[item], demand.get(item, 0)
        expect_final = init - s
        if f > expect_final:
            out['lost_updates'] += f - expect_final          # acknowledged decrement missing
        elif f < expect_final:
            out['unacked_decrements'] += expect_final - f     # decrement without acknowledgement
        out['oversold'] += max(0, s - init)
        out['negative_items'] += 1 if f < 0 else 0
        should = min(init, dmd)
        out['false_rejects'] += max(0, should - s)            # legitimate requests not served
        out['served'] += s
        out['expected_served'] += should
    return out


def duty_judge(final_on_duty, served, initial_on_duty=2):
    """Write skew workload: every area must keep at least one courier on duty.
    final_on_duty / served: dicts keyed by area."""
    out = {'violations': 0, 'false_rejects': 0, 'served': 0, 'state_mismatch': 0}
    for area, n in final_on_duty.items():
        s = served.get(area, 0)
        if n < 1:
            out['violations'] += 1
        if s == 0:
            out['false_rejects'] += 1        # one break request per area was legitimate
        if n != initial_on_duty - s:
            out['state_mismatch'] += 1
        out['served'] += s
    return out


def repl_loss_judge(acked, present):
    """acked: ids whose commit was acknowledged to the client;
    present: ids found on the node that serves after failover."""
    acked, present = set(map(tuple, acked)), set(map(tuple, present))
    return {'acked': len(acked), 'present': len(present),
            'lost_acked': len(acked - present), 'unacked_present': len(present - acked)}


def split_judge(old_rows, new_rows):
    """Rows are (id, writer, seq) read from the old and the new primary.
    Counts rows that exist on only one side and ids used for different rows."""
    old = {r[0]: tuple(r[1:]) for r in old_rows}
    new = {r[0]: tuple(r[1:]) for r in new_rows}
    old_keys, new_keys = set(old.values()), set(new.values())
    return {'only_old': len(old_keys - new_keys), 'only_new': len(new_keys - old_keys),
            'common': len(old_keys & new_keys),
            'id_collisions': sum(1 for i in old if i in new and old[i] != new[i])}


def stale_judge(visible_flags):
    """visible_flags[i] is True when write i was visible on the standby right
    after its commit was acknowledged."""
    n = len(visible_flags)
    stale = sum(1 for v in visible_flags if not v)
    return {'n': n, 'stale': stale, 'stale_ratio': round(stale / n, 4) if n else None}
