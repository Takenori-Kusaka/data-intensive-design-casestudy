"""Judges of topic C (pure functions; audited by audit.py with planted errors).

  result_diff      (queries.py) a query result against the truth of the generator
  classify_drift   what happened to each row when the upstream schema drifted
  judge_sum        the reader's aggregate against the truth
  judge_table      rows of a table after re-runs / retention: duplicates, missing, stale, extra
"""
import math


def _num(v):
    """Number carried by an observed value (text, possibly a quoted JSON string) or None."""
    if v is None:
        return None
    s = str(v).strip()
    if len(s) >= 2 and s[0] == '"' and s[-1] == '"':
        s = s[1:-1]
    try:
        x = float(s)
    except ValueError:
        return None
    return None if math.isnan(x) else x


CLASSES = ['faithful', 'null', 'defaulted', 'altered', 'raw_text', 'row_missing', 'quarantined', 'unreadable']


def classify_value(faithful, observed):
    """faithful: the number the upstream meant (None = the field is absent / null).
    observed: what a reader gets (text or None)."""
    x = _num(observed)
    if faithful is None:
        if observed is None:
            return 'faithful'
        return 'defaulted' if x == 0 or observed == '' else 'altered'
    if observed is None:
        return 'null'
    if x is None:
        return 'raw_text'               # kept as text that is not a number (e.g. "1200 JPY")
    if x == faithful:
        return 'faithful'
    return 'defaulted' if x == 0 else 'altered'


def classify_drift(truth, observed, quarantined=()):
    """truth: {id: {'drifted': bool, 'faithful': number|None}}
    observed: {id: text|None} of the rows a reader can see, or None when nothing can be read.
    Returns counts per class, separately for drifted and clean (not drifted) rows."""
    out = {'drifted': dict.fromkeys(CLASSES, 0), 'clean': dict.fromkeys(CLASSES, 0),
           'unexpected_rows': 0, 'both_loaded_and_quarantined': 0}
    q = set(quarantined)
    for i, t in truth.items():
        g = out['drifted' if t['drifted'] else 'clean']
        if observed is None:
            g['unreadable'] += 1
        elif i in q:
            g['quarantined'] += 1
            out['both_loaded_and_quarantined'] += int(i in observed)
        elif i not in observed:
            g['row_missing'] += 1
        else:
            g[classify_value(t['faithful'], observed[i])] += 1
    if observed is not None:
        out['unexpected_rows'] = sum(1 for i in observed if i not in truth)
    return out


def judge_sum(truth_sum, got):
    """The reader's sum against the truth. got: number / None (SQL NULL)."""
    if got is None:
        return {'equal': truth_sum == 0, 'diff': None if truth_sum else 0}
    d = float(got) - float(truth_sum)
    return {'equal': abs(d) < 1e-6, 'diff': round(d, 3)}


def judge_table(expected, got_rows):
    """expected: {key: value} the table should hold. got_rows: list of (key, value) read back.
    duplicates: rows beyond the first of a key. stale: key present with another value."""
    seen = {}
    dup = 0
    for k, v in got_rows:
        if k in seen:
            dup += 1
        else:
            seen[k] = v
    missing = sum(1 for k in expected if k not in seen)
    extra = sum(1 for k in seen if k not in expected)
    stale = sum(1 for k, v in seen.items() if k in expected and expected[k] != v)
    return {'rows_expected': len(expected), 'rows_got': len(got_rows), 'duplicates': dup, 'missing': missing,
            'extra': extra, 'stale': stale, 'wrong': dup + missing + extra + stale}
