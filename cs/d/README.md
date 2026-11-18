# 題材 D：整合性の異常の再現

設計は [research/DESIGN.md](../../research/DESIGN.md) の 6 節。結果は [results/d/summary.md](../../results/d/summary.md)。

## 流し方

前提：WSL2（Ubuntu-24.04）の中で Docker Engine が使えること、`uv` があること。空きメモリは 3 GB 以上（同時に動かすコンテナは最大で約 0.5 GB）。

```bash
# Windows の Git Bash から（WSL の中なら bash cs/d/run.sh だけでよい）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/common/setup_venv.sh   # 初回だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/d/run.sh               # 全部（約 2 時間）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/d/run.sh d1 summary    # 一部だけ
```

- 結果は 1 回の実行を 1 行として `results/d/*.jsonl` に追記する。完了済みの実行は飛ばすので、途中で止めても続きから流せる。最初からやり直すときは `--fresh`
- 作るコンテナ・ボリューム・ネットワークの名前はすべて `dic-d-` で始まる。それ以外には触れない。終わると消し、残りが無いことを表示する
- ホストに公開するポートは 127.0.0.1 の 25432・25436・25441〜25443・25457・25474
- Windows の空きメモリが 3 GB を切ると、コンテナを消して終了コード 3 で止まる

## ファイル

| ファイル | 中身 |
| --- | --- |
| `config.py` | イメージのタグ、ポート、名前 |
| `db.py` | PostgreSQL・CockroachDB（psycopg）と MySQL（PyMySQL）を同じ形で呼ぶ薄い層、再試行つきのトランザクション |
| `products.py` | D-1〜D-3 の単一ノードの起動と停止 |
| `d1_hermitage.py` | D-1：実行順を固定した手順と判定 |
| `pool.py`・`d2_contention.py`・`d3_defenses.py` | D-2・D-3：並行の負荷（在庫の引き当て、配達員の休憩） |
| `repl.py` | D-4〜D-7 の PostgreSQL プライマリ＋スタンバイ、Toxiproxy による経路の遮断と遅延 |
| `d4_failover.py`・`d5_sync_cost.py`・`d6_stale.py`・`d7_promote.py` | D-4〜D-7 |
| `judges.py`・`audit.py` | 判定器と、誤りを仕込んだデータでの監査 |
| `aggregate.py` | `results/d/summary.md` を結果のファイルから作る |
| `run.py`・`run.sh` | 題材 D を最初から最後まで流す |

compose のファイルは置いていない。実験の途中でコンテナを止める・強制終了する・起動し直す操作が多いので、`docker run` を Python（`cs/common/dockerutil.py`）から呼んでいる。

## 製品と版（2026-11-18 に公式の配布元でタグを確かめて固定）

| 製品 | イメージ |
| --- | --- |
| PostgreSQL 18 | `postgres:18.7-alpine3.24` |
| MySQL 8.4（InnoDB） | `mysql:8.4.13` |
| CockroachDB（単一ノード、ライセンスキーなし） | `cockroachdb/cockroach:v26.4.0` |
| Toxiproxy | `ghcr.io/shopify/toxiproxy:2.13.0` |

ダイジェストは `results/env.json`。試験用のパスワード（`dicpass`）はコンテナの中だけで使う固定値。

## 出どころ

D-1 の手順の考え方（2 つか 3 つのセッションで文の実行順を固定し、各セッションに何が見えたかで異常を判定する）と、G0・G1a・G1b・G1c・OTV・PMP・P4・G-single・G2-item・G2 の各手順は、**Hermitage**（Martin Kleppmann、<https://github.com/ept/hermitage>、CC BY 4.0）の README と `postgres.md`・`mysql.md`・`cockroachdb.md` による。文の並びはこのリポジトリで書き直し、製品をまたいで同じ並びで流せるようにした。P2・P3・P3W・MAVW・PMPW2 はこのケーススタディで足した手順で、Hermitage には無い。判定の規則（読んだ行と最後の表の中身から機械的に判定する）もこのリポジトリのもの。

Hermitage と同じく、これは試験の集まりで、証明ではない。「防いだ」は「この手順では起きなかった」という意味である。
