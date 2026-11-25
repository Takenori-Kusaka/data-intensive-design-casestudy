# -*- coding: utf-8 -*-
"""Build catalog/data-intensive-design-review.xlsx from catalog.json, decisions/*.json and raw/reconcile.json.
Run validate.py first; this script refuses to build while validation reports problems.
  python catalog/scripts/build_xlsx.py"""
import collections
import datetime
import json
import math
import os
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
FONT = 'Meiryo'
MAIN = '一覧'
GROUP_FILL = {'定義': '13324A', '根拠': '4E6A86', '実測': '0B6E78', '架空の宅配注文サービスでの扱い': '7A5BA8', '記入欄（自分のシステム）': 'B7791F', '備考と確認': '5F6B76'}
ENTRY_FILL = PatternFill('solid', fgColor='FFF7DB')
JFILL = {'共通の基準に含めた': '1F6F4A', '題材の重点に含めた': '3B8458', '実験で比べた': '4E6A86', '見送った': 'D18A0B', '測っていない': '8A949C', '対象外': 'C9CED2'}
THIN = Side(style='thin', color='D3DCDF')
BORDER = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


def f(bold=False, color='17202B', size=10):
    return Font(name=FONT, bold=bold, color=color, size=size)


def width_of(text):
    return sum(2 if unicodedata.east_asian_width(ch) in 'WFA' else 1 for ch in str(text))


def head(ws, row, col, text, fill='13324A'):
    c = ws.cell(row=row, column=col, value=text)
    c.font = f(True, 'FFFFFF')
    c.fill = PatternFill('solid', fgColor=fill)
    c.alignment = Alignment(wrap_text=True, vertical='center')
    c.border = BORDER
    return c


def sheet_main(wb, v, table):
    ws = wb.create_sheet(MAIN)
    cols = V.COLUMNS
    start = 1
    for group, n in [(g, len(list(1 for c in cols if c[3] == g))) for g in dict.fromkeys(c[3] for c in cols)]:
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
        if group in ('根拠', '実測'):
            ws.column_dimensions[L].outlineLevel = 1    # collapsible with the outline buttons; shown by default
    for n, (c, link) in enumerate(table, 3):
        lines = 1
        for i, (key, label, w, group) in enumerate(cols, 1):
            val = c[key]
            cell = ws.cell(row=n, column=i, value=val if val != '' else None)
            cell.font = f(bold=key in ('id', 'question'))
            cell.alignment = Alignment(wrap_text=True, vertical='top')
            cell.border = BORDER
            if key in V.ENTRY:
                cell.fill = ENTRY_FILL
            if link.get(key):
                cell.hyperlink = link[key]
                cell.font = f(color='0B6E78')
            lines = max(lines, math.ceil(width_of(val) / max(1.0, (w - 1.5) * 1.05)))
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

    seen = lambda k, order=None: [x for x in (order or [])if any(c[k] == x for c, _ in table)] + sorted(set(c[k] for c, _ in table) - set(order or []))
    row = block('ステップ', 'step', seen('step', V.STEPS), row)
    row = block('対象', 'target', seen('target', V.TARGETS), row)
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
    ws.column_dimensions['B'].width = 10
    return i


def sheet_reconcile(wb, v):
    ws = wb.create_sheet('照合の行')
    used = collections.defaultdict(list)
    for r in v.rows:
        for k in ((v.dec.get(r['key']) or {}).get('meas') or {}).get('links') or []:
            used[k].append(r['id'])
    heads = [('照合の行', 9), ('題材', 26), ('ガイドのスライドの題', 40), ('ガイドの記述', 60), ('判定（summary のまま）', 40), ('結びつけた問い（ID）', 30), ('結果のファイル', 28)]
    for i, (h, w) in enumerate(heads, 1):
        head(ws, 1, i, h)
        ws.column_dimensions[get_column_letter(i)].width = w
    for n, r in enumerate(v.rec.values(), 2):
        t = r['topic']
        vals = [r['key'], '題材 %s：%s' % (t, links.TOPIC_NAME[t]), r['slide'], r['cells'][1], V.verdict_text(r['verdict']),
                '、'.join(used[r['key']]) or 'なし（スライドの事実の照合で、どの問いにも結びつけていない）', links.summary_path(t)]
        lines = 1
        for i, val in enumerate(vals, 1):
            c = ws.cell(row=n, column=i, value=val)
            c.font = f()
            c.alignment = Alignment(wrap_text=True, vertical='top')
            c.border = BORDER
            lines = max(lines, math.ceil(width_of(val) / max(1.0, (heads[i - 1][1] - 1.5) * 1.05)))
        ws.cell(row=n, column=7).hyperlink = links.summary_link(t, links.RECONCILE)
        ws.cell(row=n, column=7).font = f(color='0B6E78')
        ws.row_dimensions[n].height = min(409, max(18, lines * 14.5 + 4))
    ws.freeze_panes = 'B2'
    ws.auto_filter.ref = 'A1:G%d' % (len(v.rec) + 1)


def sheet_lists(wb):
    ws = wb.create_sheet('入力規則')
    lists = collections.OrderedDict([('記入欄の判定', V.MY_JUDGES), ('ステップ', V.STEPS), ('対象', V.TARGETS), ('いつ問うか', V.WHEN), ('4 つの性質', V.PROPS),
                                     ('根拠の種別', V.BASIS_KINDS), ('扱いの判定', V.JUDGES), ('出どころの種別', V.ORIGINS)])
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
    lines = [
        ('設計レビューの問いの一覧', ''),
        ('この表は何か', 'ガイド「%s」と、そのケーススタディ（4 つの題材を実機で 2 回測った）に書いてある、設計レビューで問うことを、1 行 1 問で並べた表。新任のテックリーダーが、設計レビューの場で使う。' % links.GUIDE_TITLE),
        ('行の数', '%d 行（ガイドの問い %d 行、ケーススタディだけにある問い %d 行）。実測と結びついた行は %d 行。件数は「集計」のシートが数式で数える。' % (n_rows, n_guide, n_rows - n_guide, n_meas)),
        ('行にしたもの', 'ガイドのスライドに問いの形で書いてあるものだけ：各 Part の「設計レビューで問うこと」の表、分ける理由・まとめる理由の問い（Part 3）、サーガの 3 つの軸の問い（Part 11）、照合と系譜の 4 つの道具が答える問い（Part 12）、基盤を作る前の問いと 4 つの問い（Part 13）、Part 17 の 9 つのステップの問い・ADR に足す 3 項目・トレードオフの問い。ケーススタディの「設計レビューに持ち帰る問い」。各 Part の扉の「答えられるようになる問い」は、読者が学ぶ問いなので入れていない。ステップ × 基盤の掛け合わせで行を作ることはしていない。'),
        ('まとめ方', '同じ文言の問い（Part 17 と Part 0 は、各 Part の問いを文言のまま引いている）は 1 行にまとめ、出どころを全部書いた。ケーススタディの持ち帰る問いは、ガイドの問いと同じ決定を問うものはその行にまとめ（備考に理由）、無いものだけ別の行にした。'),
        ('使い方 1', '「一覧」を、ステップか対象で絞り込む。初めてなら「まず問う問いか」を「はい」で絞る（Part 17 がステップごとに最初に問うと選んだ問い）。'),
        ('使い方 2', '問いを読み上げ、返ってきた答えを「危ない兆候」「よい答えの例」と比べる。なぜ問うのかを聞かれたら「なぜ問うか」と根拠の列を見せる。'),
        ('使い方 3', '黄色の列（記入欄）に、自分のシステムでの現状の答え、判定（問題なし・要対応・対象外）、担当、期限を書く。記入欄は空のまま配っている。進み具合は「集計」の最後の表に出る。'),
        ('列の説明：定義', 'ID は安定（RQ-P＜Part＞-＜スライドでの順＞。ケーススタディだけの問いは RQ-C＜題材＞-＜順＞）。「よい答えの例」は、ガイドのその Part の記述を、この表の作成時に 1〜2 文にまとめたもの（スライドの文そのものではない）。「いつ問うか」と「4 つの性質のどれに効くか」も、Part 17 が挙げた問いを除いて、この表の作成時に付けた。'),
        ('列の説明：根拠', '「根拠」は、問いが載っているスライドの根拠の行（仕様・文書／指針・報告／書籍）を写したもので、問い 1 つずつの出典ではない。「書籍」は書名・章節・印刷ページだけで、本文は引用していない。スライドの表に「なぜ問うか」「危ない兆候」の欄が無く、この表で補った行は、根拠の種別に「本ガイドの考え」が入り、備考に補ったことを書いた。'),
        ('列の説明：実測', '「実測の判定（照合の表のまま）」は、ケーススタディの results/＜題材＞/summary.md の「ガイドの記述との照合」の行の判定を、そのまま写したもの（行の番号つき）。「区分」は、その判定のかっこの前の語。結びつけた行の判定が 2 種類以上あれば「複数の判定（右の列）」。結びつく行が無ければ「実測なし（未検証）」。数字は summary から写し、2 回の実行で違えば「1 回目 a・2 回目 b」と並べた。'),
        ('列の説明：扱い', 'ケーススタディの架空の宅配注文サービスで、その問いをどう扱ったか。共通の基準に含めた（B1〜B6、題材 C の構成 B）／題材の重点に含めた（E）／実験で比べた（基準・重点には入れていない）／見送った／測っていない／対象外。理由は、入れたものは外したときに再び壊れた数（除去実験）、見送ったものは失うもの。'),
        ('限界 1', '実測は 1 台（WSL2 の Docker）の上の小さな規模で、数字は同じ環境の中の相対比較。「0 件」は、この手順と試行の数で起きなかったという意味で、起きないことの証明ではない。'),
        ('限界 2', '「扱い」の判定は、架空のサービスに対するもので、自分のシステムへの推奨ではない。認証・AI・セキュリティの各基盤、クラウドのマネージド製品、法に関わる点は実機で確かめていない。'),
        ('限界 3', '問いは、ガイドとケーススタディに書いてあるものだけ。設計レビューで問うべきことを網羅した表ではない。法律に関わる行は助言ではない。'),
        ('照合の行', '「照合の行」のシートは、4 つの summary の照合の表の全 %d 行と、それぞれを結びつけた問いの ID。どの問いにも結びつけていない行もある（スライドの事実の確認で、問いに当たらないもの）。' % len(v.rec)),
        ('ガイド', links.GUIDE_REPO + ' ／ Web 版 ' + links.GUIDE_WEB),
        ('ケーススタディ', links.CASE_REPO),
        ('確認日', '%s（ガイドのスライドと、ケーススタディの summary を、この日の版で読み直した）' % links.CHECKED_ON),
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
            cb.font = f(color='0B6E78')
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
    L, last = sheet_main(wb, v, table)
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
