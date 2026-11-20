# データ指向で設計する大規模クラウドサービス ― ケーススタディ（実測）

> 作成中（2026-11-18 着手）。実験と結果はまだありません。

## 目的

ガイド `data-intensive-design-guide` の主張を、手元の実機（WSL2 の Docker）で確かめる。1 つの架空の宅配注文サービス（業務サービスと共通基盤）の上で、次の 4 つを測る。

| # | 題材 | 測るもの |
| --- | --- | --- |
| A | 基盤をまたぐデータ統合 | 二重書き込み・アウトボックス・CDC に障害を入れ、欠落・重複・順序のずれを数える |
| B | 置換と更新の演習 | 稼働中のスキーマ変更、派生ストアの差し替え、ログからの再構築 |
| C | 共通基盤の費用と性能 | ログ・分析を行指向・列指向・ファイルで比べる |
| D | 整合性の異常の再現 | 分離レベルごとの異常、古い読み取り、フェイルオーバーでの損失 |

会社・サービス・数値はすべて架空。実在の組織のシステムとは関係がない。

## 読者

新任のテックリーダー。

## 実測と未検証の区別

- 実機で測るのは OSS（PostgreSQL、Redpanda、Apache Kafka、Debezium、ClickHouse、DuckDB、CockroachDB）
- クラウドのマネージド製品は測らない（ガイドでは公式文書に基づく比較とし、未検証と明記する）
- 認証・AI・セキュリティの各基盤は実測しない
- 単一ホストのため、時計のずれに由来する異常とリージョンをまたぐ障害は測れない

## 構成

- `cs/`：実験の仕組み
- `results/`：結果（件数は必ずここから数える）。実験は題材ごとに 2 回、最初から流した。`results/<題材>/*.jsonl` が 2 回目、`results/<題材>/run1/*.jsonl` が 1 回目。`results/<題材>/summary.md` は 2 回の値が違った箇所を並べて書き、`results/RERUN.md` は 2 回の比較（決定的な欄の変化、「起きた／起きなかった」が変わったケース、件数と時間の揺れの幅）をまとめる
- まとめの作り直し（実験は流さない。Docker に触れない）：WSL の中で `bash cs/common/remake_summaries.sh`。検査は `python -m cs.common.check_summaries`
- `catalog/`：設計レビューの一覧表（xlsx）とその生成・検査のスクリプト
- `research/`：実験の設計メモ

計画と進捗はガイドのリポジトリの `research/plan.md`。

## 道具の出どころ

`catalog/scripts/` の `build_xlsx.py`・`validate.py`・`links.py`・`header_rows.py`・`merge.py`・`xlsx_check.ps1` は、[jwt-claims-casestudy](https://github.com/Takenori-Kusaka/jwt-claims-casestudy) のコミット `080b8da` からコピーした（道具のみ。中身はこのテーマ用に書き換える）。

## 調査日

2026 年 11 月時点。

## ライセンス

文章・スライドは CC BY 4.0（[LICENSE-CONTENT.md](LICENSE-CONTENT.md)）、コードは MIT（[LICENSE](LICENSE)）。

## 生成 AI の利用

このリポジトリの実験・スライド・文章は、生成 AI（Claude）を使って作成し、人が確認している。
