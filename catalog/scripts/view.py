# -*- coding: utf-8 -*-
"""Build the cells of the sheet 「一覧」 from catalog.json, decisions/*.json, raw/reconcile.json and the slide
dumps. Shared by validate.py (checks the cells) and build_xlsx.py (writes them). Everything that can be
derived (slide links, verdicts copied from the summaries, notes about joined questions) is derived here, so
the hand-written files hold only what a person decided."""
import glob
import json
import os
import re

import links

HERE = os.path.dirname(os.path.abspath(__file__))
CAT = os.path.dirname(HERE)
RAW = os.path.join(CAT, 'raw')

# key, label, width, group
PLATFORMS = [('nm_data', 'データ基盤'), ('nm_etl', 'ETL 基盤'), ('nm_log', 'ログ基盤'), ('nm_auth', '認証基盤'), ('nm_ai', 'AI 基盤'), ('nm_bi', '分析基盤'), ('nm_sec', 'セキュリティ基盤')]
G_DEF, G_ENTRY, G_NAMED, G_BASIS, G_MEAS, G_CASE, G_NOTE = '定義', '記入欄（自分のシステム）', 'ガイドが名指しした基盤', '根拠', '実測', '架空の宅配注文サービスでの扱い', '備考'
COLUMNS = [
    ('no', 'No', 5, G_DEF), ('id', 'ID', 12, G_DEF), ('step', 'ステップ', 11, G_DEF), ('target', '対象', 14, G_DEF),
    ('question', '問い', 40, G_DEF), ('why', 'なぜ問うか', 36, G_DEF), ('good', 'よい答えの例', 46, G_DEF), ('sign', '危ない兆候', 34, G_DEF),
    ('when', 'いつ問うか', 14, G_DEF), ('first', 'まず問う問いか（Part 17）', 16, G_DEF), ('properties', '4 つの性質のどれに効くか', 30, G_DEF),
    ('my_answer', '自分のシステムでの現状の答え', 40, G_ENTRY), ('my_judge', '判定', 10, G_ENTRY),
    ('my_owner', '担当', 10, G_ENTRY), ('my_due', '期限', 11, G_ENTRY), ('my_memo', 'メモ', 30, G_ENTRY),
] + [(k, lab, 7, G_NAMED) for k, lab in PLATFORMS] + [
    ('named_basis', '名指しの根拠（ガイドのスライド）', 44, G_NAMED),
    ('basis_kind', '根拠の種別', 16, G_BASIS), ('basis', '根拠（ガイドのスライドの根拠の行）', 44, G_BASIS), ('book', '書籍（書名・章節・印刷ページ）', 34, G_BASIS),
    ('guide_slide', 'ガイドのスライド', 36, G_BASIS),
    ('exp', '題材と実験', 26, G_MEAS), ('result', '実測の結果', 60, G_MEAS), ('verdict_class', '実測の判定（区分）', 16, G_MEAS),
    ('verdict', '実測の判定（照合の表のまま）', 36, G_MEAS), ('result_link', '結果へのリンク', 30, G_MEAS), ('cs_slide', 'ケーススタディのスライド', 36, G_MEAS),
    ('judge', '扱いの判定', 16, G_CASE), ('reason', '理由（防いだこと／失うこと）', 50, G_CASE),
    ('cost', '正常な操作への費用', 40, G_CASE), ('revisit', '見直し条件', 32, G_CASE),
    ('note', '備考', 44, G_NOTE), ('origin', '出どころの種別', 20, G_NOTE),
]
ENTRY = ('my_answer', 'my_judge', 'my_owner', 'my_due', 'my_memo')

STEPS = ['全体', '1 要件', '2 所有', '3 モデル', '基礎', '4 流れ', '5 契約', '6 複製', '7 正しさ', '8 導出', '9 基盤', '置換と更新', '責任']
TARGETS = ['サービス全般', '共通基盤全般', 'データ基盤', 'ETL 基盤', 'ログ基盤', '認証基盤', 'AI 基盤', '分析基盤', 'セキュリティ基盤']
WHEN = ['作る前', '動かす前', '変更の前', '入れ替えの前', 'やめる前', '定期']
PROPS = ['柔軟性', '拡張性', '置換性', '更新性']
BASIS_KINDS = ['仕様・文書', '指針・報告', '書籍', '本ガイドの考え', 'ケーススタディの実測']
JUDGES = ['共通の基準に含めた', '題材の重点に含めた', '実験で比べた', '見送った', '測っていない', '対象外']
MY_JUDGES = ['問題なし', '要対応', '対象外']
ORIGINS = ['ガイドの問い', 'ケーススタディから持ち帰る問い', '両方（ガイドの問い＋ケーススタディから持ち帰る問い）']
NO_MEAS = '照合の行なし（実測していない）'
YES, NO = '○', '—'
MIXED = '複数の判定（右の列）'
MOMENT = {'作る前': '作る前', '動かす前': '動かす前', '入れ替える前': '入れ替えの前', 'やめる前': 'やめる前'}


def clean(s):
    return re.sub(r'\s*\n\s*', '', s or '').strip()


def verdict_text(v):
    return v.replace('**', '').strip()


def verdict_head(v):
    """The class of a verdict: its text before the first parenthesis, as the summary wrote it."""
    return re.split(r'[（(]', verdict_text(v), maxsplit=1)[0].strip().rstrip('。')


def load():
    rows = json.load(open(os.path.join(CAT, 'catalog.json'), encoding='utf-8'))
    dec = {}
    for p in sorted(glob.glob(os.path.join(CAT, 'decisions', 'd*.json'))):
        for k, v in json.load(open(p, encoding='utf-8')).items():
            dec[k] = v
    rec = json.load(open(os.path.join(RAW, 'reconcile.json'), encoding='utf-8'))
    guide = json.load(open(os.path.join(RAW, 'guide_slides.json'), encoding='utf-8'))['slides']
    cs = json.load(open(os.path.join(RAW, 'casestudy_slides.json'), encoding='utf-8'))['slides']
    return rows, dec, rec, guide, cs


def part_no(section):
    m = re.match(r'Part\s?(\d+)', section or '')
    return int(m.group(1)) if m else -1


class View:
    def __init__(self):
        self.rows, self.dec, rec, guide, cs = load()
        self.rec = {r['key']: r for r in rec['rows']}
        self.headings = rec['headings']
        self.guide_by_title = {}
        for s in guide:
            self.guide_by_title.setdefault(clean(s['title']), s)
        self.cs_slides = cs
        self.problems = []

    def guide_ref(self, title):
        s = self.guide_by_title.get(clean(title))
        if not s:
            return None
        return 'Part %d「%s」（スライド %d）' % (part_no(s['section']), clean(s['title']), s['n']), links.guide_slide(s['n'])

    def reconcile_titles(self, keys):
        out = []
        for k in keys:
            r = self.rec.get(k)
            if r:
                for t in r['slide'].split('／'):
                    if t.strip() and t.strip() not in out:
                        out.append(t.strip())
        return out

    def cs_citing(self, titles):
        """Case-study slides whose evidence line names one of these guide slides."""
        out = []
        for s in self.cs_slides:
            line = ' '.join(str(v) for v in (s.get('refs') or {}).values())
            if any(t in line for t in titles) and s['kind'] == 'content':
                out.append('Part %d「%s」（スライド %d）' % (part_no(s['section']), clean(s['title']), s['n']))
        return out

    def cells(self, r):
        """-> (dict of column key -> text, dict of column key -> hyperlink)"""
        d = self.dec.get(r['key']) or {}
        meas, case = d.get('meas') or {}, d.get('case') or {}
        c, link = {}, {}
        for k in ('no', 'id', 'step', 'target', 'question', 'why', 'good', 'sign', 'when', 'properties', 'basis_kind', 'origin'):
            c[k] = r.get(k, '')
        c['first'] = ('はい（Part 17「%s」）' % r['first_question']) if r['first_question'] else 'いいえ（Part 17 の「まず問うこと」の表に無い）'
        # --- platforms the guide names for this question
        named = r.get('named') or {}
        parts = []
        for k, lab in PLATFORMS:
            c[k] = YES if lab in named else NO
            if lab in named:
                refs = []
                for title, why in named[lab]:
                    g = self.guide_ref(title)
                    if not g:
                        self.problems.append((r['id'], 'guide slide not found (named platform): ' + title))
                        continue
                    refs.append('スライド %s「%s」：%s' % (g[0].split('（スライド ')[1].rstrip('）'), clean(title), why))
                    link.setdefault('named_basis', g[1])
                parts.append('%s＝%s' % (lab, '、'.join(refs)))
        for lab in named:
            if lab not in [x[1] for x in PLATFORMS]:
                self.problems.append((r['id'], 'unknown platform ' + lab))
        c['named_basis'] = '／'.join(parts) if parts else '名指しなし（ガイドは、この問いを特定の基盤の名前では書いていない。%sとして問う）' % r['target']
        keys = meas.get('links') or []
        rec_titles = self.reconcile_titles(keys)
        cs_only = r['part'] is None
        # --- basis
        if cs_only:
            n = r['casestudy'][0]
            c['basis'] = 'ケーススタディ Part %d「%s」の、実測の裏づけの列（%s）' % (n['part'], n['slide_title'], links.summary_path(n['topic']))
            c['book'] = '該当なし（ケーススタディから持ち帰る問いで、ガイドのスライドの根拠の行が無い）'
        else:
            c['basis'] = r['basis'] or '根拠の行なし（このスライドに根拠の行が無い）'
            c['book'] = r['book'] or '該当なし（ガイドのスライドに書籍の根拠なし）'
        # --- guide slide
        refs = []
        for s in r['sources']:
            g = self.guide_ref(s['slide_title'])
            if not g:
                self.problems.append((r['id'], 'guide slide not found: ' + s['slide_title']))
                continue
            if g not in refs:
                refs.append(g)
        if cs_only:
            rel = [self.guide_ref(t) for t in rec_titles]
            for t, g in zip(rec_titles, rel):
                if not g:
                    self.problems.append((r['id'], 'guide slide named by the summary not found: ' + t))
            rel = [g for g in rel if g]
            c['guide_slide'] = 'ガイドの問いの表には無い。関連するスライド：' + '／'.join(g[0] for g in rel) if rel else 'ガイドの問いの表には無い（関連するスライドも照合の表に無い）'
            if rel:
                link['guide_slide'] = rel[0][1]
        else:
            c['guide_slide'] = '／'.join(g[0] for g in refs)
            if refs:
                link['guide_slide'] = refs[0][1]
        # --- measurement
        if keys:
            topics = []
            for k in keys:
                if k not in self.rec:
                    self.problems.append((r['id'], 'unknown reconcile row ' + k))
                elif k[0] not in topics:
                    topics.append(k[0])
            for t in meas.get('sections') or {}:
                if t not in topics:
                    topics.append(t)
            good = [k for k in keys if k in self.rec]
            c['exp'] = meas.get('exp', '')
            c['result'] = meas.get('result', '')
            c['verdict'] = '／'.join('%s：%s' % (k, verdict_text(self.rec[k]['verdict'])) for k in good)
            heads = []
            for k in good:
                h = verdict_head(self.rec[k]['verdict'])
                if h not in heads:
                    heads.append(h)
            c['verdict_class'] = heads[0] if len(heads) == 1 else MIXED
            parts = []
            for t in topics:
                secs = [links.RECONCILE] if any(k[0] == t for k in keys) else []
                secs += [s for s in (meas.get('sections') or {}).get(t, [])]
                for s in secs:
                    if s not in self.headings[t.lower()]:
                        self.problems.append((r['id'], 'no such heading in %s: %s' % (links.summary_path(t), s)))
                parts.append(links.summary_path(t) + ''.join('「%s」' % s for s in secs))
            c['result_link'] = '／'.join(parts) + '（照合の行：' + '、'.join(keys) + '）'
            first_sec = links.RECONCILE if any(k[0] == topics[0] for k in keys) else (meas.get('sections') or {}).get(topics[0], [None])[0]
            link['result_link'] = links.summary_link(topics[0], first_sec)
        else:
            why = meas.get('none', '')
            c['exp'] = '該当なし（%s）' % why if why else ''
            c['result'] = '実測なし（%s）' % why if why else ''
            c['verdict'] = NO_MEAS
            c['verdict_class'] = NO_MEAS
            c['result_link'] = '該当なし（実測が無いので、指す結果が無い）'
        took = ['Part %d「%s」（スライド %d）の問い「%s」' % (n['part'], n['slide_title'], n['slide_no'], n['question']) for n in r['casestudy']]
        cited = [x for x in self.cs_citing(rec_titles) if not any(x.split('の問い')[0] == t.split('の問い')[0] for t in took)][:3]
        if took or cited:
            c['cs_slide'] = '／'.join(took + (['実測のスライド：' + '、'.join(cited)] if cited else []))
        else:
            c['cs_slide'] = '該当なし（持ち帰る問いのスライドに無く、この問いの実測を示すスライドも無い）'
        # --- treatment in the fictional service
        for k in ('judge', 'reason', 'cost', 'revisit'):
            c[k] = case.get(k, '')
        # --- entry columns stay empty
        for k in ENTRY:
            c[k] = ''
        # --- note
        notes = []
        if r.get('note'):
            notes.append(r['note'])
        if r['guide_context'] and not cs_only:
            notes.append('スライドでの見出し・位置：' + r['guide_context'])
        quoted = [s for s in r['sources'][1:] if 'short_form' not in s]
        if quoted:
            notes.append('同じ文言の問いを 1 行にまとめた（出どころ %d 枚）' % (1 + len(quoted)))
        for s in r['sources']:
            if 'short_form' in s:
                notes.append('Part 14「%s」の短い形「%s」をこの行にまとめた' % (s['slide_title'], s['short_form']))
        rel = []
        for t in r.get('related') or []:
            g = self.guide_ref(t)
            if not g:
                self.problems.append((r['id'], 'guide slide not found (related): ' + t))
            else:
                rel.append(g[0])
        if rel:
            notes.append('問いの表の外で、同じ点を「問う」と書いているスライド：' + '、'.join(rel))
        if r.get('sign_from'):
            notes.append('「危ない兆候」は、%sから写した' % r['sign_from'])
        if r['p17_property']:
            notes.append('Part 17 が性質（%s）の問いに選んだ' % '・'.join(r['p17_property']))
        if r['p17_moment']:
            notes.append('Part 17 が節目「%s」の問いに選んだ' % r['p17_moment'])
        if not cs_only:
            for n in r['casestudy']:
                notes.append('ケーススタディの持ち帰る問い「%s」をこの行にまとめた（%s）' % (n['question'], n['why_same']))
        else:
            for n in r['casestudy'][:1]:
                notes.append('ガイドの問いと別の行にした理由：' + n['why_same'])
            for n in r['casestudy'][1:]:
                notes.append('ケーススタディのまとめの問い「%s」も同じ問い' % n['question'])
            if r['casestudy'][0].get('caveat'):
                notes.append('スライドの但し書き：' + r['casestudy'][0]['caveat'])
        sup = {'why': 'なぜ問うか', 'sign': '危ない兆候'}
        if r['supplied']:
            src = '実測で壊れた作りから' if cs_only else '同じスライドの文から'
            notes.append('%sは、スライドの表に欄が無いので、%s一覧表の作成時に補った' % ('・'.join('「%s」' % sup[x] for x in r['supplied']), src))
        c['note'] = '。'.join(n.rstrip('。') for n in notes) if notes else '特記なし（まとめた問いも、補った欄も無い）'
        return c, link


def number_tokens(text):
    t = re.sub(r'RQ-[A-Z0-9]+-\d+|\b[A-E]-[A-Z]?\d+\b|E-[A-D]\d|B[1-6]\b|Part \d+|v\d|\^31', ' ', text)
    return set(x.replace(',', '') for x in re.findall(r'\d+(?:[.,]\d+)*', t))


def corpus_tokens():
    root = os.path.dirname(CAT)
    text = ''
    for p in ['results/a/summary.md', 'results/b/summary.md', 'results/c/summary.md', 'results/d/summary.md', 'results/RERUN.md', 'research/DESIGN.md']:
        text += open(os.path.join(root, p), encoding='utf-8').read() + '\n'
    return set(x.replace(',', '') for x in re.findall(r'\d+(?:[.,]\d+)*', text))
