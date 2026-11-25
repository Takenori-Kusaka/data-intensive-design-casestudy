# 設計レビューの問いの一覧（付録）

ガイド「データ指向で設計する大規模クラウドサービス ― 全体像を押さえる 9 つのステップ」（[リポジトリ](https://github.com/Takenori-Kusaka/data-intensive-design-guide)、[Web 版](https://takenori-kusaka.github.io/data-intensive-design-guide/)）と、このケーススタディに書いてある「設計レビューで問うこと」を、1 行 1 問で並べた表。

- **[data-intensive-design-review.xlsx](data-intensive-design-review.xlsx)**：シートは「使い方」「一覧」「集計」「照合の行」「入力規則」
- [SCHEMA.md](SCHEMA.md)：行にするもの、まとめ方、列の定義
- [DECISION_POLICY.md](DECISION_POLICY.md)：実測の欄と「架空の宅配注文サービスでの扱い」の付け方

## 行の数（2026-11-25 時点。`decisions/validation.json` が数えた値）

| | 数 |
| --- | --- |
| ガイドのスライドから拾った問いの記録 | 200（32 の表・スライド） |
| うち、同じ問いとしてまとめた数 | 53（Part 17・Part 0 の引用 45、Part 14 の短い形 8） |
| ガイドの問いの行 | 147 |
| ケーススタディの持ち帰る問い | 34（5 枚） |
| うち、ガイドの問いの行にまとめた数 | 24（16 行に入った） |
| ケーススタディだけの問いの行 | 10 |
| **行の合計** | **157** |
| 列 | 33（うち記入欄 5） |
| 実測（照合の行）と結びついた行 | 66（照合の表 152 行のうち 110 行を使った） |

行数を数え直すには `python catalog/scripts/validate.py`（ステップ別・対象別・判定別の行数も出る）。xlsx の「集計」のシートは、同じ数を数式で数える。

## 作り方

Python 3 と openpyxl、Node.js（追加のパッケージなし）。ガイドのリポジトリを、このリポジトリと同じ親ディレクトリに置く。Excel での確認だけ Windows と Excel が要る。

```
node   catalog/scripts/extract_guide.js                        # ガイドの deck/pNN.js → raw/guide_slides.json（読むだけ）
node   catalog/scripts/extract_guide.js . casestudy_slides.json # このリポジトリの deck → raw/casestudy_slides.json
python catalog/scripts/extract_casestudy.py                    # 4 つの summary の照合の表 → raw/reconcile.json
python catalog/scripts/collect_guide.py                        # 問いを拾う → raw/guide_partNN.json
python catalog/scripts/collect_casestudy.py                    # 持ち帰る問いを拾う → raw/casestudy.json
python catalog/scripts/merge.py                                # まとめて ID を振る → catalog.json
python catalog/scripts/validate.py                             # 検査 → decisions/validation.json
python catalog/scripts/build_xlsx.py                           # → data-intensive-design-review.xlsx
powershell -File catalog/scripts/xlsx_check.ps1 -xlsx <xlsx の絶対パス（バックスラッシュ）>   # Excel で開いて確かめる
```

手で書くファイルは 2 種類だけ：

- `raw/casestudy_map.json`：持ち帰る問いを、ガイドのどの行にまとめるか（1 問ずつ理由つき）
- `decisions/d*.json`：行ごとの、よい答えの例・いつ問うか・性質（`def`）、結びつける照合の行と結果の要約（`meas`）、架空のサービスでの扱い（`case`）

`raw/guide_slides.json` と `raw/casestudy_slides.json` は deck の全文の写しなので、リポジトリには入れない（`.gitignore`）。上の 1・2 行目で作り直せる。

## 検査していること（validate.py）

- ID の一意、全行に判定があること、記入欄以外の空欄が 0、記入欄が空であること
- 値の集合（ステップ、対象、いつ問うか、性質、根拠の種別、扱いの判定、出どころの種別）
- ガイドのスライドの題が、いまの deck に実在すること。summary の節の名前が実在すること。備考などが指す ID が実在すること
- 「実測の判定（照合の表のまま）」が、summary の照合の行の判定と 1 字も違わないこと
- Part 17 が性質・節目に選んだ問いは、その性質・節目が行に入っていること
- 手で書いた数字が、summary・RERUN.md・DESIGN.md のどれかに出てくる数字であること（写し間違いの検出）
- 「測っていない」「対象外」の行に費用の数字を書いていないこと、など（DECISION_POLICY.md）

## 限界

- **実測は 1 台・小さな規模**。数字は同じ環境の中の相対比較で、「0 件」は、その手順と試行の数で起きなかったという意味
- **「扱い」の判定は、架空の宅配注文サービスに対するもの**。自分のシステムへの推奨ではない。認証・AI・セキュリティの各基盤、クラウドのマネージド製品、法に関わる点は実機で確かめていない
- **問いは、ガイドとケーススタディに書いてあるものだけ**。設計レビューで問うべきことの網羅ではない。157 行のうち 91 行は実測と結びついていない（実測なし）
- 「よい答えの例」「いつ問うか」「4 つの性質のどれに効くか」は、ガイドの記述をもとに、この表の作成時に付けた（Part 17 が挙げたものを除く）。スライドの表に欄が無く補った「なぜ問うか」「危ない兆候」は 42 行にあり、備考で分かる
- 「根拠」は、問いが載っているスライド 1 枚の根拠の行で、問い 1 つずつの出典ではない
- 数字の検査は、summary に同じ数字があるかを見るだけで、意味の取り違えまでは見つけない
- スライドの題で突き合わせているので、ガイドやケーススタディのスライドの題・表が変わったら、上の手順を流し直す（題が見つからなければ validate.py が止まる）

道具（`scripts/`）は、前の回のケーススタディ（jwt-claims-casestudy）の `catalog/scripts/` を出発点に、このテーマ用に書き直した。

ライセンスは CC BY 4.0。本表は生成AI（Claude）を使って作成し、作成者が確認して公開している。
