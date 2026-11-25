# -*- coding: utf-8 -*-
"""Merge catalog/raw/guide_partNN.json and catalog/raw/casestudy.json into catalog/catalog.json:
one row per question, with a stable ID (RQ-P<Part>-<order on the slides>, RQ-C<topic>-<order> for questions
that only the case study has). Same question = same row (rules in SCHEMA.md 「まとめ方」):
  1. Part 0 and Part 17 quote the questions of Parts 1-16 word for word -> added as further sources.
  2. Part 14's overview table gives a short form of the eight per-type questions -> added as a further source.
  3. A case-study take-home question that asks about the same decision as a guide question -> joined to that
     row by hand (catalog/raw/casestudy_map.json); the rest become rows of their own.
Then the hand-written definition fields (catalog/decisions/*.json, object "def") are attached.
  python catalog/scripts/merge.py"""
import glob
import json
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = os.path.dirname(HERE)
RAW = os.path.join(CAT, 'raw')
sys.path.insert(0, HERE)
import links  # noqa: E402

STEP_BY_TAG = {'ステップ 1': '1 要件', 'ステップ 2': '2 所有', 'ステップ 3': '3 モデル', 'ステップ 4': '4 流れ', 'ステップ 5': '5 契約',
               'ステップ 6': '6 複製', 'ステップ 7': '7 正しさ', 'ステップ 8': '8 導出', 'ステップ 9': '9 基盤', '基礎': '基礎',
               '置換と更新': '置換と更新', '責任': '責任', '考え方': '全体', 'まとめ': '全体', '導入': '全体'}
TARGET_BY_CTX = {'データ': 'データ基盤', 'ETL': 'ETL 基盤', 'ログ': 'ログ基盤', '認証': '認証基盤', 'AI': 'AI 基盤', '分析': '分析基盤', 'セキュリティ': 'セキュリティ基盤'}
KIND_LABEL = [('spec', '仕様・文書'), ('inst', '指針・報告'), ('book', '書籍')]


def norm(s):
    return re.sub(r'[\s　]+', '', s or '')


def source_of(r):
    return dict(part=r['part'], slide_title=r['slide_title'], slide_no=r['slide_no'], context=r.get('context', ''))


def load_guide():
    recs = []
    for p in sorted(glob.glob(os.path.join(RAW, 'guide_part*.json'))):
        recs += json.load(open(p, encoding='utf-8'))
    return recs


def main():
    recs = load_guide()
    rows, by_text, log = [], {}, []

    def new_row(r):
        row = dict(key='', part=r['part'], question=r['question'], why=r['why'], sign=r['sign'], context=r.get('context', ''), hint=r.get('hint', ''),
                   tag=r['tag'], section=r['section'], refs=r['refs'], lead=r.get('lead', ''), header=r.get('header', ''),
                   sources=[source_of(r)], first_question='', p17_property=[], p17_moment='', origin='ガイドの問い', casestudy=[])
        rows.append(row)
        by_text.setdefault(norm(r['question']), row)
        return row

    # pass 1: Parts 1-16, own questions (short forms wait)
    shorts = []
    for r in recs:
        if 1 <= r['part'] <= 16:
            if r.get('short_of'):
                shorts.append(r)
                continue
            hit = by_text.get(norm(r['question']))
            if hit:
                hit['sources'].append(source_of(r))
                log.append(('same text inside Parts 1-16', r['slide_title'], r['question']))
            else:
                new_row(r)
    # pass 2: Part 14's short forms -> the per-type row with the same circled number
    for r in shorts:
        num = r['context'][:1]
        hit = [x for x in rows if x['sources'][0]['slide_title'] == r['short_of'] and x['context'][:1] == num]
        if len(hit) != 1:
            raise SystemExit('short form has no home row: ' + r['question'])
        hit[0]['sources'].append(dict(source_of(r), short_form=r['question']))
        log.append(('short form', r['slide_title'], r['question']))
    # pass 3: Part 17, then Part 0 (both quote; what is left is Part 17's own)
    for r in [x for x in recs if x['part'] == 17] + [x for x in recs if x['part'] == 0]:
        hit = by_text.get(norm(r['question']))
        if hit is None:
            hit = new_row(r) if r['part'] == 17 else None
            if hit is None:
                raise SystemExit('Part 0 question without a Part 17 row: ' + r['question'])
            continue
        hit['sources'].append(source_of(r))
        log.append(('quoted', r['slide_title'], r['question']))
        if 'まず問う' in r.get('header', ''):
            hit['first_question'] = r['slide_title']
        if r['slide_title'].startswith('4 つの性質'):
            hit['p17_property'].append(r['context'])
        if r['slide_title'].startswith('基盤は、作る前'):
            hit['p17_moment'] = r['context']
        if r.get('hint') and not hit.get('hint'):
            hit['hint'] = r['hint']
    # the nine step questions: both warning signs of the step, copied from Part 17's 「まず問うこと」 tables
    for row in rows:
        if row['part'] == 17 and row['context'][:1].isdigit():
            signs = [r['sign'] for r in recs if r['part'] == 17 and 'まず問う' in r.get('header', '') and r['context'] == row['context'] and r['sign']]
            if signs:
                row['sign'] = '／'.join(signs)
                row['sign_from'] = 'Part 17 の「まず問うこと」の表の、同じステップの行'
    # keys: Part and order of first appearance
    count = {}
    for row in rows:
        count[row['part']] = count.get(row['part'], 0) + 1
        row['key'] = 'P%02d-%02d' % (row['part'], count[row['part']])
    by_key = {r['key']: r for r in rows}

    # case-study take-home questions
    cs = json.load(open(os.path.join(RAW, 'casestudy.json'), encoding='utf-8'))
    cmap = json.load(open(os.path.join(RAW, 'casestudy_map.json'), encoding='utf-8'))
    cs_new = {}
    for c in cs:
        m = cmap.get(c['key'])
        if not m:
            raise SystemExit('casestudy_map.json has no entry for ' + c['key'])
        note = dict(key=c['key'], topic=c['topic'], part=c['part'], slide_title=c['slide_title'], slide_no=c['slide_no'], question=c['question'],
                    evidence=c['evidence'], caveat=c['caveat'], why_same=m.get('why', ''))
        if m['same_as'] == 'new':
            row = dict(key=c['key'], part=None, question=c['question'], why='', sign='', context='', hint='', tag='題材 ' + c['topic'], section=c['section'],
                       refs={}, lead='', header='', sources=[], first_question='', p17_property=[], p17_moment='', origin='ケーススタディから持ち帰る問い',
                       casestudy=[note])
            rows.append(row)
            cs_new[c['key']] = row
            by_key[c['key']] = row
        else:
            tgt = by_key.get(m['same_as'])
            if tgt is None:
                raise SystemExit('%s points at an unknown row %s' % (c['key'], m['same_as']))
            tgt['casestudy'].append(note)
            if tgt['origin'] == 'ガイドの問い':
                tgt['origin'] = '両方（ガイドの問い＋ケーススタディから持ち帰る問い）'

    # hand-written definition fields
    defs = {}
    for p in sorted(glob.glob(os.path.join(CAT, 'decisions', '*.json'))):
        if os.path.basename(p) == 'validation.json':
            continue
        for k, v in json.load(open(p, encoding='utf-8')).items():
            if k in defs:
                raise SystemExit('decided twice: ' + k)
            defs[k] = v
    named = json.load(open(os.path.join(RAW, 'named_platforms.json'), encoding='utf-8'))
    related = json.load(open(os.path.join(RAW, 'related_slides.json'), encoding='utf-8'))
    for k in list(named) + list(related):
        if k not in by_key:
            raise SystemExit('named_platforms/related_slides point at an unknown row ' + k)
    out = []
    for n, row in enumerate(rows, 1):
        d = (defs.get(row['key']) or {}).get('def') or {}
        tag = row['tag']
        step = d.get('step') or STEP_BY_TAG.get(tag) or ''
        if row['part'] == 17 and row['context'][:1].isdigit():
            step = row['context']            # the nine step questions carry their own step
        target = d.get('target') or TARGET_BY_CTX.get(row['context']) or ('共通基盤全般' if row['part'] in (13, 14) else 'サービス全般')
        refs = dict(row['refs'] or {})
        book_note = ''
        if refs.get('book') and '『' not in refs['book'] and 'DDIA' not in refs['book']:
            book_note = refs.pop('book')      # the line explains the guide's own structure and names no book
        kinds = [lab for k, lab in KIND_LABEL if refs.get(k)]
        cs_only = row['part'] is None
        if cs_only:
            # the reason comes from the case-study slide (its evidence column); only the warning sign is written here
            row['why'] = row['casestudy'][0]['evidence']
        supplied = [x for x in ('why', 'sign') if not row[x]]
        copied = row.get('sign_from', '')
        why = row['why'] or d.get('why', '')
        sign = row['sign'] or d.get('sign', '')
        if cs_only:
            kinds = ['ケーススタディの実測']
        elif supplied or not kinds:
            kinds.append('本ガイドの考え')
        out.append(dict(
            no=n, id='RQ-' + row['key'], key=row['key'], part=row['part'], step=step, target=target, question=row['question'], why=why, good=d.get('good', ''),
            sign=sign, when=d.get('when', ''), first_question=row['first_question'], properties=d.get('properties', ''),
            basis_kind='／'.join(kinds), basis='／'.join(['%s：%s' % (lab, refs[k]) for k, lab in KIND_LABEL if refs.get(k)] + (['本ガイドの考え：' + book_note] if book_note else [])),
            book=refs.get('book', ''), sources=row['sources'], supplied=supplied, guide_hint=row['hint'], guide_context=row['context'],
            p17_property=row['p17_property'], p17_moment=row['p17_moment'], casestudy=row['casestudy'], origin=row['origin'],
            note=d.get('note', ''), checked_on=links.CHECKED_ON, sign_from=copied,
            named=named.get(row['key'], {}), related=related.get(row['key'], [])))
    json.dump(out, open(os.path.join(CAT, 'catalog.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    json.dump(log, open(os.path.join(RAW, 'merge_log.json'), 'w', encoding='utf-8'), ensure_ascii=False, indent=1)
    guide_rows = [r for r in out if r['part'] is not None]
    print('guide question records', len(recs), '-> rows', len(guide_rows), '(joined', len(recs) - len(guide_rows), ')')
    print('case-study take-home questions', len(cs), '-> joined to guide rows', len(cs) - len(cs_new), ', rows of their own', len(cs_new))
    print('rows', len(out), '; with definition fields', sum(1 for r in out if r['good']))


if __name__ == '__main__':
    main()
