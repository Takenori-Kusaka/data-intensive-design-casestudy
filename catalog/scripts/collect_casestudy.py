# -*- coding: utf-8 -*-
"""Pick the take-home review questions out of the case-study deck (catalog/raw/casestudy_slides.json, written
by extract_guide.js with the case-study repository as its argument) and write catalog/raw/casestudy.json.
One record per question as written on the slide. Which guide question each one equals is decided by hand in
catalog/raw/casestudy_map.json (see SCHEMA.md 「まとめ方」).
  python catalog/scripts/collect_casestudy.py"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = os.path.dirname(HERE)
RAW = os.path.join(CAT, 'raw')

# (key prefix, topic, slide title, kind, columns)
SPEC = [
    ('CA', 'A', '設計レビューに持ち帰る問い：題材 A から', 'table', dict(q=0, ev=1, caveat=2)),
    ('CB', 'B', '設計レビューに持ち帰る 6 つの問い', 'cards', {}),
    ('CC', 'C', '設計レビューに持ち帰る問い：ログ・分析の基盤', 'table', dict(q=0, ev=1)),
    ('CD', 'D', 'レビューでは、水準の名前でなく防ぐ異常と費用を問う', 'table', dict(q=0, ev=1)),
    ('CS', 'まとめ', '設計レビューに持ち帰る 8 つの問い', 'table', dict(q=0, ev=1)),
]


def clean(s):
    return re.sub(r'\s*\n\s*', '', s or '').strip()


def main():
    data = json.load(open(os.path.join(RAW, 'casestudy_slides.json'), encoding='utf-8'))
    by_title = {clean(s['title']): s for s in data['slides']}
    out = []
    for prefix, topic, title, kind, o in SPEC:
        s = by_title.get(title)
        if not s:
            raise SystemExit('case-study slide not found: ' + title)
        m = re.match(r'Part\s?(\d+)', s['section'] or '')
        base = dict(topic=topic, slide_title=title, slide_no=s['n'], part=int(m.group(1)) if m else -1, section=s['section'], refs=s['refs'])
        rows = []
        if kind == 'table':
            for r in s['tables'][0][1:]:
                r = [clean(c) for c in r]
                rows.append(dict(question=r[o['q']], evidence=r[o['ev']], caveat=r[o['caveat']] if 'caveat' in o else ''))
        else:
            for c in s['cards']:
                rows.append(dict(question=clean(c['title']), evidence='。'.join(clean(b) for b in c['body']), caveat=''))
        for i, r in enumerate(rows, 1):
            out.append(dict(base, key='%s-%02d' % (prefix, i), **r))
        print(prefix, len(rows), title)
    json.dump(out, open(os.path.join(RAW, 'casestudy.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    print('picked', len(out), 'take-home questions from', len(SPEC), 'slides')


if __name__ == '__main__':
    main()
