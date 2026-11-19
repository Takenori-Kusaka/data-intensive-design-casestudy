# 題材 B：置換と更新の演習

設計は [research/DESIGN.md](../../research/DESIGN.md) の 4 節。結果は [results/b/summary.md](../../results/b/summary.md)。

問い：動かしたまま、スキーマを変え、導出ストアを入れ替え、ログから作り直せるか。架空の宅配注文サービスの、注文のイベント、注文の表、検索の索引（導出データ）を使う。

| 実験 | 中身 | 構成 |
| --- | --- | --- |
| B-1 イベントのスキーマ変更 | 1 つの変更（v1 → v2）を Avro・Protocol Buffers・JSON Schema で作り、Redpanda のスキーマレジストリへの登録の可否と、新旧の書き手・受け手を混在させたときの受け手の結果を数える | N：検査なし（NONE）／B：レジストリの既定のまま／BE：BACKWARD_TRANSITIVE（ほかのモードも記録） |
| B-2 古いデータの読み直し | 段ごとには通るが v3 が v1 を読めない v1 → v2 → v3 と、任意フィールドを 2 回足すだけの列を登録し、先頭から読み直す | 同上 |
| B-3 DB のスキーマ変更 | PostgreSQL 18 で、(1) DDL の操作ごとのロック・書き換え・停止時間、(2) 長いトランザクションの後ろに並んだ DDL、(3) 住所の列を 2 列に分ける移行 | N：1 つのトランザクションで変える／BE：expand／migrate／contract と、その除去 2 つ |
| B-4 導出ストアの入れ替え | イベントが流れ、読み手が読み続ける中で、索引をログから作り直して入れ替える | N：同じ場所で一括／BE：並べて作り、照合し、少しずつ切り替える／BE − 照合、BE − 冪等な受け手 |
| B-5 作り直しの前提条件 | 保持期間、スナップショット、コンパクション、削除の印（トゥームストーン）。Redpanda と Apache Kafka。PostgreSQL の DELETE を Debezium で流す実験も含む | ログだけから／スナップショット＋位置／位置なし、保持を縮める／既定のまま |
| B-6 記録のシステムの入れ替え | PostgreSQL から別のインスタンスへ、並行稼働・埋め戻し・照合・読みの切替・書きの切替・旧の削除（または切り戻し）の 6 段階 | N：アプリの二重書き込み／BE：論理レプリケーション＋照合＋遮断つきの切り替え＋逆向きの複製、その除去 3 つ |
| B-7 マテリアライズドビュー | PostgreSQL の REFRESH の間の読み取り、ClickHouse の増分・結合つき増分・refreshable が何に追随するか（ガイド Part 12 が題材 B で測ると書いた点） | 製品ごと |

## 流し方

前提：WSL2（Ubuntu-24.04）の中で Docker Engine が使えること、`uv` があること。空きメモリは 3 GB 以上。同時に動かすコンテナは最大で約 1.2 GB（B-5 の Debezium の実験：Kafka Connect 約 0.6 GB、Redpanda 約 0.4 GB、PostgreSQL 2 台）。

```bash
# Windows の Git Bash から（WSL の中なら bash cs/b/run.sh だけでよい）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/setup_venv.sh   # 初回だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/pull_images.sh  # 初回だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/run.sh          # 全部
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/run.sh b3 b4    # 段を選ぶ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/run.sh --list   # 実行の一覧
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/b/go.sh b5        # 9 分で区切って流す
```

- 段は `audit`（判定器の監査）→ `b1`〜`b7` → `summary`。`--match=<文字列>` で、実行 ID にその文字列を含むものだけを流す。`--fresh` で `results/b/*.jsonl` を消して最初から
- 結果は 1 回の実行を 1 行として `results/b/<段>.jsonl` に追記する。完了済みの実行 ID は飛ばすので、途中で止めても続きから流せる。止められた途中の実行は記録されず、次に流したときに最初からやり直す（どの実行も、最初に `dic-b-` のコンテナとボリュームを消す）
- `go.sh` は、`run.sh` を 9 分（`DIC_SLICE_S` 秒）で打ち切り、結果の行数、残っている `dic-b-` のコンテナ、Windows の空きメモリを表示する。全部で約 2 時間かかるので、区切って繰り返す
- 作るコンテナ・ボリューム・ネットワークの名前はすべて `dic-b-` で始まる。それ以外には触れない
- ホストに公開するポートは 127.0.0.1 の 25632（PostgreSQL）・25633（PostgreSQL：導出、または新の記録のシステム）・25692（ブローカー）・25681（スキーマレジストリ）・25683（Kafka Connect）・25623（ClickHouse の HTTP）
- Windows の空きメモリが 3 GB を切ると、コンテナを消して終了コード 3 で止まる
- Redpanda と Apache Kafka は同じコンテナ名（`dic-b-broker`）で、同時には上げない

## ファイル

| ファイル | 中身 |
| --- | --- |
| `config.py` | イメージのタグ、ポート、名前 |
| `infra.py` | コンテナの起動と待機（PostgreSQL、Redpanda／Apache Kafka、スキーマレジストリ、Kafka Connect、ClickHouse）、トピックの読み直し |
| `schemas.py` | B-1・B-2 のスキーマ（1 つの記述から 3 形式を作る）、書き手・受け手、生成器、受け手の結果の判定 |
| `b12_schema.py` | B-1・B-2 の実行 |
| `load.py` | PostgreSQL への負荷（スレッド）と、要求の記録からの計測（止まった時間、失敗、ロック待ち） |
| `b3_ddl.py` | B-3 の実行と、移行の最終状態の判定 |
| `derive.py` | イベントの生成器、導出ストアへの受け手、導出ストアの最終状態の判定 |
| `b4_swap.py` | B-4 の実行、読み手、新旧の照合 |
| `b5_rebuild.py` | B-5 の実行（保持、コンパクション、削除の印、Debezium の DELETE） |
| `b6_sor.py` | B-6 の実行と、最終状態の判定 |
| `b7_mv.py` | B-7 の実行 |
| `audit.py` | 判定器の監査（誤りを仕込んだデータで、仕込んだ数だけを数えることを確かめる） |
| `aggregate.py`・`agg_sections.py` | `results/b/summary.md` を結果のファイルから作る（照合の判定、除去実験、気づいた点も数から作る） |
| `run.py`・`run.sh`・`go.sh` | 題材 B を最初から最後まで流す／9 分で区切って流す |
| `py.sh` | リポジトリのモジュールを実験用の venv で動かす |
| `dev_one.py`・`dev.sh`・`devmany.sh`・`dev_env.sh` | 実行を 1 つ選んで流して表示する、手元の状態を表示する（結果のファイルには書かない。作りを変えたときの確認用） |

compose のファイルは置いていない。題材 A・D と同じく、`docker run` を Python（`cs/common/dockerutil.py`）から呼んでいる。

## 製品と版（2026-11-19 に公式の配布元でタグを確かめて固定）

| 製品 | イメージ | 使う実験 |
| --- | --- | --- |
| PostgreSQL 18 | `postgres:18.7-alpine3.24` | B-3〜B-7 |
| Redpanda（スキーマレジストリ内蔵） | `docker.redpanda.com/redpandadata/redpanda:v26.2.7` | B-1・B-2・B-4・B-5 |
| Apache Kafka（KRaft、1 ノード） | `apache/kafka:4.4.0` | B-5 の照合 |
| Debezium（Kafka Connect 版） | `quay.io/debezium/connect:3.7.1.Final` | B-5 の DELETE |
| ClickHouse | `clickhouse:26.8.36.3`（公式ライブラリの LTS。題材 B で新しく pull した） | B-7 |

ダイジェストは `results/env.json`。Python のクライアントは confluent-kafka（librdkafka）、psycopg 3、fastavro、protobuf、jsonschema。試験用のパスワード（`dicpass`）はコンテナの中だけで使う固定値。

## 設定

- **Redpanda**：題材 A と同じく、開発用の設定を切って起動する（`--unsafe-bypass-fsync=false`、`developer_mode=false`、`write_caching_default=false`、トピックの自動作成なし）。B-5 では、掃除の間隔を `log_compaction_interval_ms=1000`、セグメントを時間で切る下限を `log_segment_ms_min=1000` にした
- **Apache Kafka**：B-5 では `log.cleaner.backoff.ms=1000`、`log.retention.check.interval.ms=1000`
- **B-5 のトピック**：`segment.bytes=1048576`、`segment.ms=2000`。保持の実験は `retention.ms=5000`（「既定のまま」では指定しない）。コンパクションと削除の印の実験は `cleanup.policy=compact`、`min.cleanable.dirty.ratio=0.01`、削除の印の保持は `delete.retention.ms=3000`（「既定のまま」では指定しない）。セグメントを切らせるため、キーが `f` で始まる詰め物のレコードを毎秒 1 件流す（受け手は読み飛ばす）
- **スキーマレジストリ**：互換性モードは subject ごとに `PUT /config/<subject>` で設定する。「既定のまま」は何も設定しない。書き込みの枠は「0x00＋スキーマ ID 4 バイト＋本体」を自前で付ける（Confluent のシリアライザは使わない）
- **PostgreSQL**：`shared_buffers=64MB`。B-4・B-5 の導出側は `synchronous_commit=off`。B-6 と B-5 の Debezium の実験は `wal_level=logical`。B-6 のアプリはロール `app` で接続し、遮断はこのロールの書き込みの権限を外して入れる
- **負荷（B-3・B-6）**：書き手 4・読み手 2 スレッド、1 スレッド 1 接続、`statement_timeout=3s`、要求が失敗したら接続を捨てて張り直す
- **Debezium（B-5）**：`plugin.name=pgoutput`、`snapshot.mode=no_data`、JsonConverter（schemas.enable=false）。`tombstones.on.delete` は設定しない（既定を見るため）

## 判定

- 受け手の結果（B-1・B-2）：生成器が書いた値と、受け手が読んだ値を、フィールドの意味ごとに突き合わせる（`schemas.judge_read`）
- 止まった時間（B-3・B-6・B-7）：成功した要求の完了が 1 件も無い最長の区間（`load.max_gap`）
- 最終状態：移行した表（`b3_ddl.judge_migration`）、導出ストア（`derive.judge_store`）、記録のシステム（`b6_sor.judge_sor`）を、生成器と、負荷が応答を受けた書き込みの記録から作った正解と比べる。ログの文言は使わない
- 照合（B-4）：`b4_swap.recon_judge`。B-6 の照合は `b6_sor.persistent_diff`
- `audit.py` が、これらに誤りを仕込んだデータと誤りの無いデータを流し、仕込んだ数だけを数えることを確かめる。結果は `results/b/audit.jsonl`

## 設計（DESIGN.md）から変えた点

- **B-7 を足した**：ガイドの Part 12「マテリアライズドビューは、製品で意味が違う」が題材 B で測ると書いているため（DESIGN.md の表には無い）
- **B-1 の構成 B と BE は、版が 2 つなので結果が同じになる**（推移的かどうかの差は B-2 で出る）。B-1 では 7 つのモードすべての登録の可否を記録した
- **B-5 の時間は縮めた**：retention.ms は 5 秒、delete.retention.ms は 3 秒。既定の 7 日・1 日は待っていない
- **B-5 の E-B3（監視）**は、受け手が遅れていた時間を、製品が表示した delete.retention.ms と比べる関数にした。警報が出たら先頭から作り直す
- **B-6 の移行先は同じ版の PostgreSQL**：別の製品への移行は測っていない
- **障害の入れ方**：Toxiproxy と Pumba は使っていない。受け手の停止は、DB へのコミットの後・オフセットの記録の前で止める形で、新のインスタンスの障害は `docker restart` で、件数を合図に入れた
- **受け手と負荷は Python のスレッド**（題材 A は別プロセス）。プロセスの強制終了はしていない
- 測っていない点は `results/b/summary.md` の「限界」と「未検証」に書いた
