# -*- coding: utf-8 -*-
"""Check the catalogue before the xlsx is built. Writes decisions/validation.json and exits 1 on any problem.
  - every row has a decision record; IDs are unique
  - every cell outside the entry columns is filled; the entry columns are empty
  - closed vocabularies (step, target, when, properties, basis kind, treatment, origin, platform marks)
  - every guide slide named by a row exists (by title) in the current deck; every summary heading exists
  - the verdict cell equals, word for word, the verdict of the linked rows of 「ガイドの記述との照合」
  - what Part 17 says about a question (property, moment) is reflected in the row
  - every number in 「実測の結果」 occurs in the linked reconcile rows or in the summary sections the row names
    (this catches a link that points at the wrong row); numbers in the treatment cells occur in the summaries
  - a zero in 「実測の結果」 comes with the number of trials or runs in the same sentence
  - a platform is marked only with a guide slide as its basis; a question whose 「対象」 is one platform is marked for it
  python catalog/scripts/validate.py"""
import collections
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import checks  # noqa: E402
import view as V  # noqa: E402

v = V.View()
problems = []
ids = [r['id'] for r in v.rows]
for i, n in collections.Counter(ids).items():
    if n > 1:
        problems.append((i, 'duplicate id'))
for k in sorted(set(v.dec) - set(r['key'] for r in v.rows)):
    problems.append((k, 'decision for a row that does not exist'))
corpus = V.corpus_tokens()
sections = {t: checks.sections_of(t) for t in 'ABCD'}
blank = 0
table = []
for r in v.rows:
    if r['key'] not in v.dec:
        problems.append((r['id'], 'no decision record'))
    c, link = v.cells(r)
    table.append(c)
    for key, label, _, group in V.COLUMNS:
        val = c.get(key, '')
        if key in V.ENTRY:
            if val != '':
                problems.append((r['id'], 'entry column not empty: ' + label))
            continue
        if val is None or str(val).strip() == '':
            blank += 1
            problems.append((r['id'], 'blank: ' + label))
    if c['step'] not in V.STEPS:
        problems.append((r['id'], 'step %r' % c['step']))
    if c['target'] not in V.TARGETS:
        problems.append((r['id'], 'target %r' % c['target']))
    for t in c['when'].split('・'):
        if t not in V.WHEN:
            problems.append((r['id'], 'when %r' % t))
    names = []
    if not c['properties'].startswith('該当なし（'):
        m = re.match(r'^([^：]+)：(.+)$', c['properties'] or '')
        names = m.group(1).split('・') if m else []
        if not m or any(n not in V.PROPS for n in names):
            problems.append((r['id'], 'properties %r' % c['properties'][:20]))
    for p in r['p17_property']:
        if p not in names:
            problems.append((r['id'], 'Part 17 names the property %s, the row does not' % p))
    if r['p17_moment'] and V.MOMENT[r['p17_moment']] not in c['when'].split('・'):
        problems.append((r['id'], 'Part 17 names the moment %s, the row does not' % r['p17_moment']))
    for k in c['basis_kind'].split('／'):
        if k not in V.BASIS_KINDS:
            problems.append((r['id'], 'basis kind %r' % k))
    if c['judge'] not in V.JUDGES:
        problems.append((r['id'], 'judge %r' % c['judge']))
    if c['origin'] not in V.ORIGINS:
        problems.append((r['id'], 'origin %r' % c['origin']))
    marked = [lab for k, lab in V.PLATFORMS if c[k] == V.YES]
    for k, lab in V.PLATFORMS:
        if c[k] not in (V.YES, V.NO):
            problems.append((r['id'], 'platform mark %r' % c[k]))
    for lab in marked:
        if lab + '＝スライド ' not in c['named_basis']:
            problems.append((r['id'], 'platform marked without a slide: ' + lab))
    if c['target'] in [lab for _, lab in V.PLATFORMS] and c['target'] not in marked:
        problems.append((r['id'], 'target is a platform that the row does not mark'))
    d = v.dec.get(r['key']) or {}
    meas, case = d.get('meas') or {}, d.get('case') or {}
    keys = meas.get('links') or []
    if keys:
        expect = '／'.join('%s：%s' % (k, v.rec[k]['verdict'].replace('**', '').strip()) for k in keys if k in v.rec)
        if c['verdict'] != expect:
            problems.append((r['id'], 'verdict differs from the summary'))
        if c['judge'] == '対象外' and c['verdict_class'] != '未検証':
            problems.append((r['id'], 'treated as out of scope but linked to a measured row'))
        own = ' '.join(' '.join(v.rec[k]['cells']) for k in keys if k in v.rec)
        for t, secs in (meas.get('sections') or {}).items():
            own += ' ' + ' '.join(sections[t].get(s, '') for s in secs)
        own += ' ' + ' '.join(n['evidence'] for n in r['casestudy'])
        for tok in sorted(checks.numbers(meas.get('result', '')) - checks.numbers(own, strip_ids=False)):
            problems.append((r['id'], 'number %s in the result is not in the linked rows or named sections' % tok))
        for s in checks.zeros_without_count(meas.get('result', '')):
            problems.append((r['id'], 'zero without the number of trials: ' + s[:60]))
    else:
        if not meas.get('none'):
            problems.append((r['id'], 'neither links nor a reason for no measurement'))
        if c['judge'] in ('題材の重点に含めた', '実験で比べた') and 'RQ-' not in c['reason']:
            problems.append((r['id'], 'judged as measured but no reconcile row is linked'))
    if c['judge'] in ('測っていない', '対象外') and not c['cost'].startswith('測っていない'):
        problems.append((r['id'], 'not measured, but a cost is written'))
    for field in ('reason', 'cost'):
        for tok in sorted(V.number_tokens(case.get(field) or '') - corpus):
            problems.append((r['id'], 'number %s in %s is not in the summaries' % (tok, field)))
    for m in re.findall(r'RQ-[A-Z0-9]+-\d+', ' '.join(str(x) for x in c.values())):
        if m not in ids:
            problems.append((r['id'], 'points at a row that does not exist: ' + m))
problems += v.problems

by = lambda k: dict(collections.Counter(c[k] for c in table).most_common())
plat = {}
for k, lab in V.PLATFORMS:
    named = sum(1 for c in table if c[k] == V.YES)
    plat[lab] = {'named': named, 'named_or_all_platforms': sum(1 for c in table if c[k] == V.YES or c['target'] == '共通基盤全般')}
summary = {'rows': len(v.rows), 'columns': len(V.COLUMNS), 'blank_cells_outside_entry_columns': blank, 'problems': len(problems),
           'by_step': by('step'), 'by_target': by('target'), 'by_origin': by('origin'), 'by_verdict_class': by('verdict_class'), 'by_judge': by('judge'),
           'by_part': dict(collections.Counter('ケーススタディ' if r['part'] is None else 'Part %d' % r['part'] for r in v.rows)),
           'by_platform': plat, 'rows_naming_a_platform': sum(1 for c in table if any(c[k] == V.YES for k, _ in V.PLATFORMS)),
           'rows_with_properties': sum(1 for c in table if not c['properties'].startswith('該当なし')),
           'rows_with_supplied_cells': sum(1 for r in v.rows if r['supplied']),
           'supplied_by_origin': dict(collections.Counter(('ケーススタディ' if r['part'] is None else 'ガイド') + '：' + '・'.join(r['supplied']) for r in v.rows if r['supplied'])),
           'rows_linked_to_reconcile': sum(1 for c in table if c['verdict_class'] != V.NO_MEAS),
           'reconcile_rows_linked': len(set(k for d in v.dec.values() for k in (d.get('meas') or {}).get('links') or [])), 'reconcile_rows': len(v.rec)}
json.dump({'summary': summary, 'problems': problems}, open(os.path.join(V.CAT, 'decisions', 'validation.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
print(json.dumps(summary, ensure_ascii=False, indent=1))
for p in problems[:120]:
    print('  ', *p)
sys.exit(1 if problems else 0)
