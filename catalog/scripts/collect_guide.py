# -*- coding: utf-8 -*-
"""Pick the review questions out of the guide's slides (catalog/raw/guide_slides.json, written by
extract_guide.js) and write one file per Part: catalog/raw/guide_partNN.json (one record per question as
written on the slide; nothing is reworded). Slides are addressed by title, not by number.
Rules for what counts as a question are in catalog/SCHEMA.md (section 「行にするもの」).
  python catalog/scripts/collect_guide.py"""
import json
import os
import re

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = os.path.dirname(HERE)
RAW = os.path.join(CAT, 'raw')

# (slide title, kind, options). kind:
#   table  : a table with a question column. q/why/sign/ctx/hint = column indexes; t = table index
#   cards  : each card title is a question, the body is the reason
#   plus   : rows of a table whose first cell starts with '＋' (the guide's three additions to an ADR)
#   quote  : a question quoted in the slide text (pattern)
#   multi  : a cell that holds several questions, each followed by （Part N）
#   boxlines: a note box labelled 「レビューで問うこと」; each line is a question
#   boxquote: a question quoted inside a note box (pattern)
SPEC = [
    ('設計レビューでは、性質を数字と手順で問う', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：要件と負荷', 'table', dict(q=0, why=1, sign=2)),
    ('分ける理由 6 つと、まとめる理由 2 つを並べる', 'table', dict(t=0, q=1, ctx=0)),
    ('分ける理由 6 つと、まとめる理由 2 つを並べる', 'table', dict(t=1, q=1, ctx=0)),
    ('設計レビューで問うこと：正と所有', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：データモデル', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：ストレージと索引', 'table', dict(q=0, why=1, sign=2)),
    ('データの流れについて、設計レビューで問うこと', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：契約と変更', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：複製と分割', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：正しさの水準', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：頼り先が止まったら', 'table', dict(q=0, why=1, sign=2)),
    ('サーガは 3 つの軸で整理し、調整役の得失で選ぶ', 'table', dict(t=0, q=2, ctx=0, hint=1)),
    ('設計レビューで問うこと：サービスをまたぐ整合性', 'table', dict(q=0, why=1, sign=2)),
    ('導出データにも出力の検査が要る：照合と系譜', 'table', dict(q=1, ctx=0, hint=2)),
    ('設計レビューで問うこと：導出データ', 'table', dict(q=0, why=1, sign=2)),
    ('共通基盤は内部の利用者に出す製品。作る前に問う', 'table', dict(q=0, why=1, sign=2)),
    ('どの基盤も、同じ 4 つの問いで設計する', 'cards', {}),
    ('設計レビューで問うこと：7 つの基盤に 1 問ずつ', 'table', dict(q=1, why=2, sign=3, ctx=0)),
    ('被害を広げた設計は、8 つの型にまとまる', 'table', dict(q=3, ctx=0, short_of='設計レビューで問うこと：型ごとに 1 問')),
    ('設計レビューで問うこと：型ごとに 1 問', 'table', dict(q=1, why=2, sign=3, ctx=0)),
    ('段階配布は万能ではない。何を見て進むかを問う', 'boxlines', dict(label='レビューで問うこと')),
    ('型②⑧：1 つの基盤、1 つの共有が全体に届く', 'boxquote', dict(pattern=r'(「止まってもよいはず」の依存は、実際に止めて確かめたか)を問う')),
    ('設計レビューで問うこと：入れ替える前・やめる前', 'table', dict(q=0, why=1, sign=2)),
    ('設計レビューで問うこと：法・プライバシー', 'table', dict(q=0, why=1, sign=2)),
    ('9 つのステップで、全体像を順に押さえる', 'table', dict(q=2, ctx=0)),
    ('9 つのステップは、レビューで問う順番でもある', 'table', dict(q=1, ctx=0, hint=2)),
    ('ステップ 1〜3：数字と正と問い合わせを先に問う', 'table', dict(q=1, sign=2, ctx=0)),
    ('ステップ 4〜6：書く箇所、互換の向き、置く台数を問う', 'table', dict(q=1, sign=2, ctx=0)),
    ('ステップ 7〜9：範囲、作り直し、止まり方を問う', 'table', dict(q=1, sign=2, ctx=0)),
    ('4 つの性質から、効くステップと問いを引く', 'multi', dict(q=2, ctx=0)),
    ('基盤は、作る前・入れ替える前・やめる前に問う', 'table', dict(q=1, sign=2, ctx=0)),
    ('決定は ADR に残し、データの 3 項目を足す', 'plus', dict(hint=1)),
    ('トレードオフは、3 つの手順で自分で分析する', 'quote', dict(pattern=r'「(X を変えたら、Y も変える必要があるか)」')),
]


def clean(s):
    return re.sub(r'\s*\n\s*', '', s or '').strip()


def part_of(slide):
    m = re.match(r'Part\s?(\d+)', slide['section'] or '')
    return int(m.group(1)) if m else -1


def main():
    data = json.load(open(os.path.join(RAW, 'guide_slides.json'), encoding='utf-8'))
    by_title = {}
    for s in data['slides']:
        by_title.setdefault(clean(s['title']), []).append(s)
    out = {}
    for title, kind, o in SPEC:
        ss = by_title.get(title) or []
        if len(ss) != 1:
            raise SystemExit('slide not found or not unique: %s (%d)' % (title, len(ss)))
        s = ss[0]
        recs = []
        base = dict(slide_title=title, slide_no=s['n'], part=part_of(s), section=s['section'], tag=s['tag'],
                    stages=s['stages'], refs=s['refs'], lead=clean(s['lead']), kind=kind)
        if kind == 'table':
            t = s['tables'][o.get('t', 0)]
            head = [clean(c) for c in t[0]]
            for r in t[1:]:
                r = [clean(c) for c in r]
                recs.append(dict(base, question=r[o['q']], why=r[o['why']] if 'why' in o else '', sign=r[o['sign']] if 'sign' in o else '',
                                 context=r[o['ctx']] if 'ctx' in o else '', hint=r[o['hint']] if 'hint' in o else '',
                                 header='｜'.join(head), short_of=o.get('short_of', '')))
        elif kind == 'cards':
            for c in s['cards']:
                recs.append(dict(base, question=re.sub(r'^[①-⑨]\s*', '', clean(c['title'])), why='。'.join(clean(b) for b in c['body']), sign='',
                                 context=clean(c['title'])[:1], hint='', header='カードの題｜カードの本文', short_of=''))
        elif kind == 'plus':
            t = s['tables'][0]
            head = [clean(c) for c in t[0]]
            for r in t[1:]:
                r = [clean(c) for c in r]
                if r[0].startswith('＋'):
                    recs.append(dict(base, question=re.sub(r'^＋\s*', '', r[0]), why='', sign='', context='ADR に足す項目',
                                     hint=r[1] + '。例（架空の宅配注文サービス）：' + r[2], header='｜'.join(head), short_of=''))
        elif kind == 'quote':
            text = ' '.join(s['texts'])
            m = re.search(o['pattern'], text)
            if not m:
                raise SystemExit('quoted question not found: ' + title)
            recs.append(dict(base, question=m.group(1), why='', sign='', context='図「3 つの手順」の 1 つ目（絡み合う部分を見つける）', hint='', header='スライドの図の文', short_of=''))
        elif kind == 'boxlines':
            box = [b for b in s['boxes'] if b['label'] == o['label']]
            if len(box) != 1:
                raise SystemExit('box not found: ' + title)
            for line in box[0]['text'].split('\n'):
                if line.strip():
                    recs.append(dict(base, question=line.strip(), why='', sign='', context='注記「%s」' % o['label'], hint='', header='注記の行', short_of=''))
        elif kind == 'boxquote':
            m = re.search(o['pattern'], ' '.join(b['text'] for b in s['boxes']))
            if not m:
                raise SystemExit('quoted question not found: ' + title)
            recs.append(dict(base, question=m.group(1), why='', sign='', context='注記（リーダーの視点）', hint='', header='注記の文', short_of=''))
        elif kind == 'multi':
            t = s['tables'][0]
            head = [clean(c) for c in t[0]]
            for r in t[1:]:
                for q, p in re.findall(r'(.+?)（Part (\d+)）', clean(r[o['q']])):
                    recs.append(dict(base, question=q.strip(), why='', sign='', context=clean(r[o['ctx']]), hint='', header='｜'.join(head), short_of='', cites_part=int(p)))
        out.setdefault(base['part'], []).extend(recs)
        print('Part %2d  %-3d %s' % (base['part'], len(recs), title))
    total = 0
    for part, recs in sorted(out.items()):
        for i, r in enumerate(recs, 1):
            r['seq'] = i
        json.dump(recs, open(os.path.join(RAW, 'guide_part%02d.json' % part), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
        total += len(recs)
    print('picked', total, 'question records from', len(SPEC), 'tables or slides;', ', '.join('Part %d: %d' % (p, len(r)) for p, r in sorted(out.items())))


if __name__ == '__main__':
    main()
