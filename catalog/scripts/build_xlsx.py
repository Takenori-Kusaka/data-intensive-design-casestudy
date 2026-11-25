# -*- coding: utf-8 -*-
"""Build catalog/data-intensive-design-review.xlsx from catalog.json, decisions/*.json and raw/*.json.
Run validate.py first; this script refuses to build while validation reports problems.
  python catalog/scripts/build_xlsx.py"""
import collections
import datetime
import json
import math
import os
import re
import sys
import unicodedata

from openpyxl import Workbook
from openpyxl.formatting.rule import FormulaRule
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import links  # noqa: E402
import view as V  # noqa: E402

OUT = os.path.join(V.CAT, 'data-intensive-design-review.xlsx')
ROOT = os.path.dirname(V.CAT)
FONT = 'Meiryo'
MAIN = '一覧'
GROUP_FILL = {V.G_DEF: '13324A', V.G_ENTRY: 'B7791F', V.G_NAMED: '2F6F8F', V.G_BASIS: '4E6A86', V.G_MEAS: '0B6E78', V.G_CASE: '7A5BA8', V.G_NOTE: '5F6B76'}
ENTRY_FILL = PatternFill('solid', fgColor='FFF7DB')
JFILL = {'共通の基準に含めた': '1F6F4A', '題材の重点に含めた': '3B8458', '実験で比べた': '4E6A86', '見送った': 'D18A0B', '測っていない': '8A949C', '対象外': 'C9CED2'}
THIN = Side(style='thin', color='D3DCDF')
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)
LINK = '0B6E78'

# the words the summaries use as verdicts, as this catalogue reads them (the summaries have no glossary)
VERDICT_WORDS = [
    ('一致', 'ガイドの記述と、実測の結果が合った'),
    ('実測', 'ガイドが「題材で測る」と書いた点を測った。合った・合わないの判定ではない（結果は「実測の結果」の列）'),
    ('未確認だった点の実測／未検証だった点の実測', 'ガイドが「未確認」「未検証」と書いていた点を測った。かっこに（一致）とあれば、ガイドの見込みと合った'),
    ('ガイドに無い点の実測', 'ガイドに記述の無い点を、ついでに測った'),
    ('一部だけ実測／一部だけ再現', 'ガイドの記述の一部だけを測った、または一部だけがそのとおりになった。かっこの中に、どこまでかが書いてある'),
    ('条件つき／条件による／条件つきで食い違い', '条件によって結果が変わった。「食い違い」は、測った条件ではガイドの記述と逆になったもの。かっこの中の条件を必ず読む'),
    ('実行によって変わった', '同じコードで 2 回流して、判定が変わった。どちらの回も「この条件で起きうる／起きないこともある」と読む'),
    ('未検証', 'summary が、この点は測っていないと書いた（照合の行はあるが、実測は無い）'),
    (V.MIXED, 'この問いに結びつけた照合の行が 2 つ以上あり、判定の語が 1 種類でない。右の列に、行ごとの判定がある'),
    (V.NO_MEAS, 'この問いに当たる照合の行が無い。ケーススタディで測っていない'),
]


def f(bold=False, color='17202B', size=10):
    return Font(name=FONT, bold=bold, color=color, size=size)


def width_of(text):
    return sum(2 if unicodedata.east_asian_width(ch) in 'WFA' else 1 for ch in str(text))


def lines_of(text, width):
    return sum(max(1, math.ceil(width_of(p) / max(1.0, (width - 1.5) * 1.05))) for p in str(text).split('\n'))


def head(ws, row, col, text, fill='13324A'):
    c = ws.cell(row=row, column=col, value=text)
    c.font = f(True, 'FFFFFF')
    c.fill = PatternFill('solid', fgColor=fill)
    c.alignment = Alignment(wrap_text=True, vertical='center')
    c.border = BORDER
    return c


def body(ws, row, col, text, width=None, link=None, bold=False):
    c = ws.cell(row=row, column=col, value=text if text != '' else None)
    c.font = f(bold=bold, color=LINK if link else '17202B')
    c.alignment = Alignment(wrap_text=True, vertical='top')
    c.border = BORDER
    if link:
        c.hyperlink = link
    return c


def table_sheet(ws, heads, rows, freeze='A2', links_col=None):
    """heads: [(label, width)], rows: list of lists; links_col: {column index: [url per row]}"""
    for i, (h, w) in enumerate(heads, 1):
        head(ws, 1, i, h)
        ws.column_dimensions[get_column_letter(i)].width = w
    for n, vals in enumerate(rows, 2):
        lines = 1
        for i, val in enumerate(vals, 1):
            url = (links_col or {}).get(i, [None] * len(rows))[n - 2]
            body(ws, n, i, val, link=url)
            lines = max(lines, lines_of(val, heads[i - 1][1]))
        ws.row_dimensions[n].height = min(409, max(18, lines * 14.5 + 4))
    ws.freeze_panes = freeze
    ws.auto_filter.ref = 'A1:%s%d' % (get_column_letter(len(heads)), len(rows) + 1)


def sheet_main(wb, table):
    ws = wb.create_sheet(MAIN)
    cols = V.COLUMNS
    start = 1
    for group in dict.fromkeys(c[3] for c in cols):
        n = sum(1 for c in cols if c[3] == group)
        ws.merge_cells(start_row=1, start_column=start, end_row=1, end_column=start + n - 1)
        for col in range(start, start + n):
            ws.cell(row=1, column=col).fill = PatternFill('solid', fgColor=GROUP_FILL[group])
        c = ws.cell(row=1, column=start, value=group)
        c.font = f(True, 'FFFFFF', 11)
        c.alignment = Alignment(horizontal='center', vertical='center')
        start += n
    for i, (key, label, w, group) in enumerate(cols, 1):
        head(ws, 2, i, label, GROUP_FILL[group])
        L = get_column_letter(i)
        ws.column_dimensions[L].width = w
        if group in (V.G_NAMED, V.G_BASIS, V.G_MEAS, V.G_CASE):
            ws.column_dimensions[L].outlineLevel = 1    # can be folded with the outline buttons; shown by default
    marks = [k for k, _ in V.PLATFORMS]
    for n, (c, link) in enumerate(table, 3):
        lines = 1
        for i, (key, label, w, group) in enumerate(cols, 1):
            cell = body(ws, n, i, c[key], link=link.get(key), bold=key in ('id', 'question'))
            if key in V.ENTRY:
                cell.fill = ENTRY_FILL
            if key in marks:
                cell.alignment = Alignment(horizontal='center', vertical='top')
            lines = max(lines, lines_of(c[key], w))
        ws.row_dimensions[n].height = min(409, max(30, lines * 14.5 + 6))
    last = len(table) + 2
    ws.freeze_panes = 'F3'
    ws.auto_filter.ref = 'A2:%s%d' % (get_column_letter(len(cols)), last)
    ws.row_dimensions[1].height = 22
    ws.row_dimensions[2].height = 44
    keys = [c[0] for c in cols]
    L = lambda k: get_column_letter(keys.index(k) + 1)
    dv = DataValidation(type='list', formula1='=入力規則!$A$2:$A$%d' % (len(V.MY_JUDGES) + 1), allow_blank=True)
    ws.add_data_validation(dv)
    dv.add('%s3:%s%d' % (L('my_judge'), L('my_judge'), last))
    J = L('judge')
    for j, col in JFILL.items():
        ws.conditional_formatting.add('%s3:%s%d' % (J, J, last), FormulaRule(formula=['%s3="%s"' % (J, j)], fill=PatternFill('solid', fgColor=col),
                                      font=Font(name=FONT, color='17202B' if j == '対象外' else 'FFFFFF', bold=True)))
    M = L('my_judge')
    ws.conditional_formatting.add('%s3:%s%d' % (M, M, last), FormulaRule(formula=['%s3="要対応"' % M], fill=PatternFill('solid', fgColor='C2413B'), font=Font(name=FONT, color='FFFFFF', bold=True)))
    a, b = L(marks[0]), L(marks[-1])
    ws.conditional_formatting.add('%s3:%s%d' % (a, b, last), FormulaRule(formula=['%s3="%s"' % (a, V.YES)], fill=PatternFill('solid', fgColor='CFE6EE'), font=Font(name=FONT, bold=True)))
    ws.sheet_view.zoomScale = 90
    return L, last


def sheet_summary(wb, L, last, table):
    ws = wb.create_sheet('集計')
    ws['A1'] = '集計（数式。「一覧」を絞り込んでも、全体の行数を数える）'
    ws['A1'].font = f(True, size=12)
    rng = lambda k: "'%s'!$%s$3:$%s$%d" % (MAIN, L(k), L(k), last)
    row = 3
    ws.cell(row=row, column=1, value='行の数').font = f(True)
    ws.cell(row=row, column=2, value='=COUNTA(%s)' % rng('id')).font = f(True)
    row += 2

    def block(title, key, values, row):
        head(ws, row, 1, title)
        head(ws, row, 2, '行数')
        a = row + 1
        for i, val in enumerate(values, a):
            ws.cell(row=i, column=1, value=val).font = f()
            ws.cell(row=i, column=2, value='=COUNTIF(%s,$A%d)' % (rng(key), i)).font = f()
        end = a + len(values)
        ws.cell(row=end, column=1, value='合計').font = f(True)
        ws.cell(row=end, column=2, value='=SUM(B%d:B%d)' % (a, end - 1)).font = f(True)
        return end + 2

    seen = lambda k, order=None: [x for x in (order or []) if any(c[k] == x for c, _ in table)] + sorted(set(c[k] for c, _ in table) - set(order or []))
    row = block('ステップ', 'step', seen('step', V.STEPS), row)
    row = block('対象', 'target', seen('target', V.TARGETS), row)
    head(ws, row, 1, '基盤ごと（ガイドが名指しした行）')
    head(ws, row, 2, '名指しの行')
    head(ws, row, 3, '＋対象が共通基盤全般の行')
    for i, (k, lab) in enumerate(V.PLATFORMS, row + 1):
        ws.cell(row=i, column=1, value=lab).font = f()
        ws.cell(row=i, column=2, value='=COUNTIF(%s,"%s")' % (rng(k), V.YES)).font = f()
        ws.cell(row=i, column=3, value='=B%d+COUNTIFS(%s,"%s",%s,"共通基盤全般")' % (i, rng(k), V.NO, rng('target'))).font = f()
    row += len(V.PLATFORMS) + 2
    row = block('実測の判定（区分）', 'verdict_class', seen('verdict_class', [V.NO_MEAS, V.MIXED]), row)
    row = block('架空の宅配注文サービスでの扱い', 'judge', V.JUDGES, row)
    row = block('出どころの種別', 'origin', V.ORIGINS, row)
    head(ws, row, 1, '記入欄の進み具合（自分のシステム）')
    head(ws, row, 2, '行数')
    for i, val in enumerate(V.MY_JUDGES, row + 1):
        ws.cell(row=i, column=1, value=val).font = f()
        ws.cell(row=i, column=2, value='=COUNTIF(%s,$A%d)' % (rng('my_judge'), i)).font = f()
    i = row + 1 + len(V.MY_JUDGES)
    ws.cell(row=i, column=1, value='未記入').font = f()
    ws.cell(row=i, column=2, value='=COUNTBLANK(%s)' % rng('my_judge')).font = f()
    ws.column_dimensions['A'].width = 52
    ws.column_dimensions['B'].width = 12
    ws.column_dimensions['C'].width = 26


def sheet_reconcile(wb, v):
    ws = wb.create_sheet('照合の行')
    used = collections.defaultdict(list)
    for r in v.rows:
        for k in ((v.dec.get(r['key']) or {}).get('meas') or {}).get('links') or []:
            used[k].append(r['id'])
    heads = [('照合の行', 9), ('題材', 26), ('ガイドのスライドの題', 40), ('ガイドの記述', 60), ('判定（summary のまま）', 40), ('結びつけた問い（ID）', 30), ('結果のファイル', 28)]
    rows, urls = [], []
    for r in v.rec.values():
        t = r['topic']
        rows.append([r['key'], '題材 %s：%s' % (t, links.TOPIC_NAME[t]), r['slide'], r['cells'][1], V.verdict_text(r['verdict']),
                     '、'.join(used[r['key']]) or 'なし（スライドの事実の照合などで、どの問いにも結びつけていない）', links.summary_path(t)])
        urls.append(links.summary_link(t, links.RECONCILE))
    table_sheet(ws, heads, rows, freeze='B2', links_col={7: urls})


def sheet_platforms(wb, v, table):
    """Part 13's table 「7 基盤 × 4 つの問い」, cell for cell, with the first question, the detail slides and what was measured."""
    ws = wb.create_sheet('基盤ごとの答え')
    src = v.guide_by_title['7 基盤 × 4 つの問い：答えは基盤ごとに違う']
    t = src['tables'][0]
    note = ' '.join(b['text'] for b in src['boxes'])
    detail = json.load(open(os.path.join(V.RAW, 'platform_slides.json'), encoding='utf-8'))
    short = {'データ': 'データ基盤', 'ETL': 'ETL 基盤', 'ログ': 'ログ基盤', '認証': '認証基盤', 'AI': 'AI 基盤', '分析': '分析基盤', 'セキュリティ': 'セキュリティ基盤'}
    key_of = {lab: k for k, lab in V.PLATFORMS}
    by_id = {c['id']: c for c, _ in table}
    heads = [('基盤', 14), ('最初に問う 1 問（ID と問い）', 40)] + [(V.clean(h), 38) for h in t[0][1:]] + [('一次資料の無いセル（※）', 30), ('詳しいスライド', 40), ('実測の有無（この基盤を名指しした行）', 50)]
    rows, urls = [], []
    for r in t[1:]:
        lab = short[V.clean(r[0])]
        first = [x for x in v.rows if x['guide_context'] == V.clean(r[0]) and x['sources'][0]['slide_title'].startswith('設計レビューで問うこと：7 つの基盤')][0]
        cells = [V.clean(x) for x in r[1:]]
        marked = [h for h, x in zip(t[0][1:], cells) if '※' in x]
        slides = [v.guide_ref(s) for s in detail[lab]]
        meas, none = [], 0
        for c, _ in table:
            if c[key_of[lab]] == V.YES:
                if c['verdict_class'] in (V.NO_MEAS, '未検証'):
                    none += 1
                else:
                    meas.append(c['id'])
        fc = by_id[first['id']]
        state = ('実測と結びついた行：%s。' % '、'.join(meas) if meas else '実測と結びついた行は無い。') + '実測の無い行（未検証を含む）：%d。最初の 1 問（%s）の判定：%s' % (none, first['id'], fc['verdict_class'])
        rows.append([lab, '%s %s' % (first['id'], first['question'])] + cells +
                    [('※のあるセル：%s。その点を直接述べる一次資料が見つからなかった（本ガイドの考え）' % '、'.join(V.clean(m) for m in marked)) if marked else '※のあるセルは無い（ほかのセルにも、一次資料を組み合わせた整理を含む）',
                     '、'.join(s[0] for s in slides), state])
        urls.append(slides[0][1])
    table_sheet(ws, heads, rows, freeze='B2', links_col={len(heads) - 1: urls})
    n = len(rows) + 3
    g = v.guide_ref(src['title'])
    for a, b, url in [('出どころ', 'ガイド %s の表を、セルの文のまま写した。' % g[0], g[1]), ('スライドの注記', note, None),
                      ('読み方', '問いは 4 つとも同じで（一覧の RQ-P13-04〜RQ-P13-07）、答えが基盤ごとに違う。ここにあるのはガイドの答えで、自分のシステムの答えは一覧の記入欄に書く。「一覧」で基盤を 1 つ見るときは、その基盤の列を「○」で絞る（使い方 2）。', None)]:
        ws.cell(row=n, column=1, value=a).font = f(True)
        c = ws.cell(row=n, column=2, value=b)
        c.font = f(color=LINK if url else '17202B')
        if url:
            c.hyperlink = url
        n += 1


def design_tables():
    """Rows of the two tables of research/DESIGN.md: the common baseline B1-B6 and the per-topic emphasis E."""
    text = open(os.path.join(ROOT, 'research', 'DESIGN.md'), encoding='utf-8').read()
    out = []
    for m in re.finditer(r'^\| (B[1-6]) \| (.+?) \| (.+?) \| (.+?) \|$', text, re.M):
        out.append((m.group(1), '共通の基準', m.group(2), '効く題材：%s。ガイドの Part %s' % (m.group(3), m.group(4))))
    for m in re.finditer(r'^\| ([A-D]) \| (E-.+?) \| (.+?) \|$', text, re.M):
        for item in re.split(r'、(?=E-[A-D]\d)', m.group(2)):
            k, _, rest = item.partition(' ')
            out.append((k, '題材 %s の重点' % m.group(1), rest, '見送りの候補：' + m.group(3)))
    return out


def experiments():
    out = []
    for t in 'abcd':
        lines = open(os.path.join(ROOT, 'results', t, 'summary.md'), encoding='utf-8').read().split('\n')
        seen = set()
        for l in lines:
            m = re.match(r'^\| (%s-[A-Z]?\d[^|]*?) \| ([^|]+?) \| ([^|]+?) \| [^|]+ \|$' % t.upper(), l) if t == 'a' else None
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                out.append((m.group(1), '題材 A の実験（ケース）', m.group(2), m.group(3)))
            m = re.match(r'^## (%s-\d+) (.+)$' % t.upper(), l)
            if m and t != 'a' and m.group(1) not in seen:
                seen.add(m.group(1))
                out.append((m.group(1), '題材 %s の実験' % t.upper(), m.group(2), links.summary_path(t.upper()) + ' の同じ名前の節'))
    return out


def sheet_legend(wb):
    ws = wb.create_sheet('凡例')
    rows = [
        ('題材 A', '題材', links.TOPIC_NAME['A'], '注文の状態の変化を、検索の索引と通知へ届ける。結果は results/a/summary.md'),
        ('題材 B', '題材', links.TOPIC_NAME['B'], 'スキーマを変え、導出ストアと記録のシステムを入れ替え、ログから作り直す。結果は results/b/summary.md'),
        ('題材 C', '題材', links.TOPIC_NAME['C'], 'ログ・分析の基盤で、持ち方ごとの費用を比べる。結果は results/c/summary.md'),
        ('題材 D', '題材', links.TOPIC_NAME['D'], '分離レベルと複製の設定ごとに、異常と失うものを数える。結果は results/d/summary.md'),
        ('N', '構成', '素朴（よくある作り）', '基準を入れない構成。題材 A では二重書き込み'),
        ('B', '構成', '共通の基準', 'B1〜B6 を入れた構成。題材 A ではアウトボックス＋ポーリング。題材 C の B は「再実行しても同じ結果になる取り込み」で、B1〜B6 とは別'),
        ('BE', '構成', '基準＋題材ごとの重点', 'B に、その題材の重点 E を足した構成。題材 A ではアウトボックス＋CDC'),
        ('除去実験', '言葉', 'B・BE から要素を 1 つずつ外して、何が再び壊れるかを見る実験', '「理由」の列の「外すと…」は、この実験の結果'),
        ('正常な操作', '言葉', '基準を入れたせいで、正規の処理が止まる・遅くなる・失敗すること', '「正常な操作への費用」の列は、この数'),
    ]
    rows += design_tables()
    rows += experiments()
    rows += [
        ('1 回目 a・2 回目 b', '数字の書き方', '同じコードで最初から 2 回流した、それぞれの値', '「76・85」のように数字を「・」で並べたものも、1 回目・2 回目の順（ケーススタディのスライドの文をそのまま写した「なぜ問うか」に多い）。2 回とも同じ値は 1 つだけ書く'),
        ('n/m 試行', '数字の書き方', 'm 回の試行のうち、n 回で起きた', '「0（5 試行）」は、5 回の試行で 1 度も起きなかった、の意味。起きないことの証明ではない'),
        ('A-P1', '数字の書き方', '題材 A の追加実験。1 回だけ流した', '2 回目の値は無い。「追加実験（1 回だけ）」と書いてある'),
    ]
    rows += [(w, '実測の判定の語', d, 'summary に語の定義の節は無く、この表の作成時の読み方') for w, d in VERDICT_WORDS]
    rows += [(j, '扱いの判定', d, 'DECISION_POLICY.md') for j, d in [
        ('共通の基準に含めた', '問いの答えが、共通の基準 B1〜B6（題材 C は構成 B）の要素'), ('題材の重点に含めた', '問いの答えが、題材ごとの重点 E か、題材 B の記録のシステムの入れ替え（B-6）の BE の要素'),
        ('実験で比べた', '基準にも重点にも入れていないが、実験で比べた数字がある'), ('見送った', '構成として取らなかった。理由の列に、失うもの'),
        ('測っていない', 'サービスの範囲には入るが、実験にしていない'), ('対象外', '組んでいない基盤、組織・法・言葉の定義など、実験で確かめる対象でない')]]
    table_sheet(ws, [('記号・語', 22), ('種類', 16), ('意味', 60), ('補足', 70)], rows, freeze='A2')


def sheet_lists(wb):
    ws = wb.create_sheet('入力規則')
    lists = collections.OrderedDict([('記入欄の判定', V.MY_JUDGES), ('ステップ', V.STEPS), ('対象', V.TARGETS), ('いつ問うか', V.WHEN), ('4 つの性質', V.PROPS),
                                     ('根拠の種別', V.BASIS_KINDS), ('扱いの判定', V.JUDGES), ('出どころの種別', V.ORIGINS), ('基盤の名指し', [V.YES, V.NO])])
    for i, (k, vs) in enumerate(lists.items(), 1):
        head(ws, 1, i, k)
        ws.column_dimensions[get_column_letter(i)].width = 30
        for j, val in enumerate(vs, 2):
            ws.cell(row=j, column=i, value=val).font = f()


def sheet_readme(wb, v, table):
    ws = wb.active
    ws.title = '使い方'
    ws.column_dimensions['A'].width = 26
    ws.column_dimensions['B'].width = 130
    n_rows = len(table)
    n_guide = sum(1 for r in v.rows if r['part'] is not None)
    n_meas = sum(1 for c, _ in table if c['verdict_class'] != V.NO_MEAS)
    n_case = sum(1 for c, _ in table if c['judge'] not in ('測っていない', '対象外'))
    n_named = sum(1 for c, _ in table if any(c[k] == V.YES for k, _ in V.PLATFORMS))
    n_all = sum(1 for c, _ in table if c['target'] == '共通基盤全般')
    lines = [
        ('設計レビューの問いの一覧', ''),
        ('この表は何か', 'ガイド「%s」と、そのケーススタディ（4 つの題材を実機で 2 回測った）に書いてある、設計レビューで問うことを、1 行 1 問で並べた表。新任のテックリーダーが、設計レビューの場で使う。' % links.GUIDE_TITLE),
        ('行の数', '%d 行（ガイドの問い %d 行、ケーススタディだけにある問い %d 行）。照合の行（実測）と結びついた行は %d 行。件数は「集計」のシートが数式で数える。' % (n_rows, n_guide, n_rows - n_guide, n_meas)),
        ('行にしたもの', 'ガイドのスライドに問いの形で書いてあるものだけ：各 Part の「設計レビューで問うこと」の表、分ける理由・まとめる理由の問い（Part 3）、サーガの 3 つの軸の問い（Part 11）、照合と系譜の 4 つの道具が答える問い（Part 12）、基盤を作る前の問いと 4 つの問い（Part 13）、段階配布の「レビューで問うこと」と「止まってもよいはず」の依存の問い（Part 14）、Part 17 の 9 つのステップの問い・ADR に足す 3 項目・トレードオフの問い。ケーススタディの「設計レビューに持ち帰る問い」。各 Part の扉の「答えられるようになる問い」は、読者が学ぶ問いなので入れていない。ステップ × 基盤の掛け合わせで行を作ることはしていない。'),
        ('まとめ方', '同じ文言の問い（Part 17 と Part 0 は、各 Part の問いを文言のまま引いている）は 1 行にまとめ、出どころを全部書いた。ケーススタディの持ち帰る問いは、ガイドの問いと同じ決定を問うものはその行にまとめ（備考に理由）、無いものだけ別の行にした。'),
        ('使い方 1：ステップで見る', '「一覧」を、ステップで絞り込む。初めてなら「まず問う問いか」を「はい」で絞る（Part 17 がステップごとに最初に問うと選んだ問い）。'),
        ('使い方 2：基盤を 1 つ見る', '基盤の名前の列（データ基盤〜セキュリティ基盤）は、ガイドがその基盤の名前を挙げて同じ点を書いている問いに「○」が付く（根拠のスライドは右隣の列）。○ のある行は %d 行。基盤を 1 つ見るときは、(1) その基盤の列を「○」で絞って読み、(2) 絞り込みを外して「対象」を「共通基盤全般」で絞った %d 行（どの基盤にも向ける問い：作る前の問い、4 つの問い、全体を止める型の問い）も読む。4 つの問いへのガイドの答えは、シート「基盤ごとの答え」に 7 基盤分ある。○ が無いことは、その基盤に関係が無いという意味ではなく、ガイドが名指ししていないという意味。' % (n_named, n_all)),
        ('使い方 3：場で問う', '問いを読み上げ、返ってきた答えを「危ない兆候」「よい答えの例」と比べる。なぜ問うのかを聞かれたら「なぜ問うか」と根拠の列を見せる。'),
        ('使い方 4：書き込む', '黄色の列（記入欄）に、自分のシステムでの現状の答え、判定（問題なし・要対応・対象外）、担当、期限を書く。記入欄は空のまま配っている。進み具合は「集計」の最後の表に出る。'),
        ('使い方 5：実測を読む', '実測と、架空のサービスでの扱いを読むなら、「扱いの判定」で「測っていない」「対象外」を外す（残るのは %d 行）。照合の行の番号（A-01 など）は summary には書かれていない（表の上からの順）ので、シート「照合の行」で引く。記号（N・B・BE、B1〜B6、E-A1 など）と判定の語は、シート「凡例」。' % n_case),
        ('列の説明：定義', 'ID は安定（RQ-P＜Part＞-＜スライドでの順＞。ケーススタディだけの問いは RQ-C＜題材＞-＜順＞）。「よい答えの例」は、ガイドの記述（その Part と、問いが指す Part）を、この表の作成時に 1〜2 文にまとめたもの（スライドの文そのものではない）。「いつ問うか」と「4 つの性質のどれに効くか」も、Part 17 が挙げた問いを除いて、この表の作成時に付けた。性質は、ガイドの定義（Part 1。拡張性は負荷への対応だけ）に当たるものだけに付け、当たらない行は「該当なし（理由）」。'),
        ('列の説明：対象と基盤', '「対象」は、問いの表が基盤を名指ししているときだけその基盤（Part 13 の「7 つの基盤に 1 問ずつ」の 7 行）。基盤や利用側に向ける問いは「共通基盤全般」、ほかは「サービス全般」。主題からの対応は「ガイドが名指しした基盤」の列に、根拠のスライドつきで持つ。'),
        ('列の説明：根拠', '「根拠」は、問いが載っているスライドの根拠の行（仕様・文書／指針・報告／書籍）を写したもので、問い 1 つずつの出典ではない。「書籍」は書名・章節・印刷ページだけで、本文は引用していない。スライドの表に「なぜ問うか」「危ない兆候」の欄が無く、この表で補った行は、根拠の種別に「本ガイドの考え」が入り、備考に補ったことを書いた。'),
        ('列の説明：実測', '「実測の判定（照合の表のまま）」は、ケーススタディの results/＜題材＞/summary.md の「ガイドの記述との照合」の行の判定を、そのまま写したもの（行の番号つき）。「区分」は、その判定のかっこの前の語。語の意味は「凡例」。結びつく行が無ければ「%s」。数字は summary から写し、2 回の実行で違えば「1 回目 a・2 回目 b」と並べた。' % V.NO_MEAS),
        ('列の説明：扱い', 'ケーススタディの架空の宅配注文サービスで、その問いをどう扱ったか。値の意味は「凡例」。理由は、入れたものは外したときに再び壊れた数（除去実験）、見送ったものは失うもの。'),
        ('限界 1', '実測は 1 台（WSL2 の Docker）の上の小さな規模で、数字は同じ環境の中の相対比較。「0 件」は、この手順と試行の数で起きなかったという意味で、起きないことの証明ではない。'),
        ('限界 2', '「扱い」の判定は、架空のサービスに対するもので、自分のシステムへの推奨ではない。認証・AI・セキュリティの各基盤、クラウドのマネージド製品、法に関わる点は実機で確かめていない。'),
        ('限界 3', '問いは、ガイドとケーススタディに書いてあるものだけ。設計レビューで問うべきことを網羅した表ではない。法律に関わる行は助言ではない。'),
        ('照合の行', '「照合の行」のシートは、4 つの summary の照合の表の全 %d 行と、それぞれを結びつけた問いの ID。どの問いにも結びつけていない行もある（スライドの事実の確認で、問いに当たらないもの）。' % len(v.rec)),
        ('ガイド', links.GUIDE_REPO + ' ／ Web 版 ' + links.GUIDE_WEB),
        ('ケーススタディ', links.CASE_REPO),
        ('確認日', '%s（ガイドのスライドと、ケーススタディの summary を、この日の版で読み直した。全行に共通）' % links.CHECKED_ON),
        ('ライセンスと作り方', 'CC BY 4.0。本表は生成AI（Claude）を使って作成し、作成者が確認して公開している。作り方は catalog/README.md。'),
    ]
    for i, (a, b) in enumerate(lines, 1):
        ca = ws.cell(row=i, column=1, value=a)
        cb = ws.cell(row=i, column=2, value=b or None)
        ca.font = f(True, size=14 if i == 1 else 10)
        cb.font = f()
        cb.alignment = Alignment(wrap_text=True, vertical='top')
        ca.alignment = Alignment(vertical='top')
        ws.row_dimensions[i].height = max(18, math.ceil(width_of(b) / 250.0) * 16 + 4)
        if b.startswith('http'):
            cb.hyperlink = b.split(' ')[0]
            cb.font = f(color=LINK)
    leg = len(lines) + 2
    ws.cell(row=leg, column=1, value='扱いの判定の色').font = f(True)
    for i, (j, col) in enumerate(JFILL.items(), leg + 1):
        c = ws.cell(row=i, column=1, value=j)
        c.fill = PatternFill('solid', fgColor=col)
        c.font = Font(name=FONT, bold=True, color='17202B' if j == '対象外' else 'FFFFFF')
    c = ws.cell(row=leg + len(JFILL) + 1, column=1, value='記入欄')
    c.fill = ENTRY_FILL
    c.font = f(True)


def main():
    val = os.path.join(V.CAT, 'decisions', 'validation.json')
    if not os.path.exists(val) or json.load(open(val, encoding='utf-8'))['summary']['problems']:
        raise SystemExit('run validate.py first and fix what it reports')
    v = V.View()
    table = [v.cells(r) for r in v.rows]
    wb = Workbook()
    sheet_readme(wb, v, table)
    sheet_legend(wb)
    L, last = sheet_main(wb, table)
    sheet_platforms(wb, v, table)
    sheet_summary(wb, L, last, table)
    sheet_reconcile(wb, v)
    sheet_lists(wb)
    p = wb.properties                      # no personal data in the workbook properties
    p.creator = ''
    p.lastModifiedBy = ''
    p.title = '設計レビューの問いの一覧'
    stamp = datetime.datetime.strptime(links.CHECKED_ON, '%Y-%m-%d')
    p.created = stamp
    p.modified = stamp
    wb.save(OUT)
    print('wrote', os.path.basename(OUT), len(table), 'rows,', len(V.COLUMNS), 'columns')


if __name__ == '__main__':
    main()
