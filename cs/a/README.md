# 題材 A：基盤をまたぐデータ統合

設計は [research/DESIGN.md](../../research/DESIGN.md) の 3 節。結果は [results/a/summary.md](../../results/a/summary.md)。

注文サービス（Python）が PostgreSQL 18 の注文の表（記録のシステム）を更新し、状態の変化を、イベント基盤を通じて検索の索引と通知の履歴（導出データ。別の PostgreSQL）へ届ける。届け方を 3 つの構成で比べ、欠落・重複・順序のずれ・最終状態の食い違いを数える。

| 構成 | 作り |
| --- | --- |
| N 二重書き込み | 注文サービスが DB をコミットし、続けてブローカーへ発行して確認応答を待つ（別々の操作）。受け手は冪等でない |
| B アウトボックス＋ポーリング | 注文の更新と同じトランザクションでアウトボックスの表に書く。別のプロセス（`relay.py`）が未送信の行を連番の順に読んで発行し、確認応答の後で送信済みの印を付ける。受け手は冪等（イベント ID を、効果と同じトランザクションで処理済みの表に入れる） |
| BE アウトボックス＋CDC | 同じアウトボックスの表を、Debezium（Kafka Connect、Outbox Event Router）が WAL から読んで発行する。キーは注文 ID（同じ注文は同じパーティション）。受け手は冪等 |

変種と除去：`wm`（最後に読んだ連番より大きい行だけ読むポーリング）、`noidem`（受け手が重複を捨てない）、`nokey`（CDC のキーをイベント ID にする）。CDC の落とし穴（`wal.py`）は、Debezium を止めたまま書き続け、`max_slot_wal_keep_size`（既定の -1 と 64MB）と `idle_replication_slot_timeout` を比べる。

## 流し方

前提：WSL2（Ubuntu-24.04）の中で Docker Engine が使えること、`uv` があること。空きメモリは 3 GB 以上（同時に動かすコンテナは最大で約 1.5 GB：Kafka Connect 約 0.7 GB、ブローカー 約 0.2〜0.5 GB、PostgreSQL 2 台、Toxiproxy）。イメージは合計で約 4 GB（Debezium の Kafka Connect が約 2.5 GB）。

```bash
# Windows の Git Bash から（WSL の中なら bash cs/a/run.sh だけでよい）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/a/setup_venv.sh   # 初回だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/a/run.sh            # 全部
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/a/run.sh wal summary # 一部だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/a/run.sh --list     # 実行の一覧
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/a/run.sh poll       # 追加実験 A-P1（名指ししたときだけ流れる）
```

- 手順は `audit`（判定器の監査）→ `main`（Redpanda で構成 × ケース）→ `wal`（CDC の落とし穴）→ `kafka`（Apache Kafka での照合）→ `summary`
- 追加実験 `poll`（A-P1：CDC の遅れと、コネクタの読み取り間隔）は、引数なしの「全部」には入らない。`run.sh poll` と名指ししたときだけ流れ、`results/a/poll.jsonl` だけに書く（本実験の結果のファイル、`memlog.jsonl`、`results/env.json` は変えない。`--fresh` とは一緒に使えない）。`--budget=<秒>` を付けると、その秒数に収まらない実行を始めずに止まる（続きは同じコマンドで流す）。`run.sh poll --list` で実行 ID の一覧
- 結果は 1 回の実行を 1 行として `results/a/*.jsonl` に追記する。完了済みの実行は飛ばすので、途中で止めても続きから流せる。最初からやり直すときは `--fresh`
- 1 回の実行ごとに、コンテナとボリュームを消して作り直す。作るコンテナ・ボリューム・ネットワークの名前はすべて `dic-a-` で始まる。それ以外には触れない。終わると消し、残りが無いことを表示する
- ホストに公開するポートは 127.0.0.1 の 25532（正の PostgreSQL）・25533（導出の PostgreSQL）・25574（Toxiproxy の API）・25583（Kafka Connect の REST）・25592（ブローカー。Toxiproxy 経由）
- Windows の空きメモリが 3 GB を切ると、コンテナを消して終了コード 3 で止まる
- Redpanda と Apache Kafka は同じコンテナ名（`dic-a-broker`）で、同時には上げない

## ファイル

| ファイル | 中身 |
| --- | --- |
| `config.py` | イメージのタグ、ポート、名前、負荷の既定値 |
| `infra.py` | コンテナの起動と待機、スキーマ、Debezium のコネクタの登録、障害の注入（切断・一時停止・強制終了）、トピックの読み直し |
| `workload.py` | 生成器。書き手ごとの更新の列を種から決める（期待するイベント ID の集合はここから作る） |
| `service.py` | 注文サービス（N：二重書き込み、B・BE：アウトボックス）。呼び出し元から見た応答を 1 行ずつ記録する |
| `relay.py` | アウトボックスのポーリング（未送信の行を読む作りと、連番より大きい行だけ読む変種） |
| `consumer.py` | 受け手（索引・通知）。効果を先にコミットし、オフセットは後で記録する |
| `cases.py` | 1 回の実行（起動 → 負荷 → 障害 → 落ち着くまで待つ → 判定 → 照合 → 片付け） |
| `wal.py` | CDC の落とし穴（Debezium を止めたまま書き続ける） |
| `judges.py`・`audit.py` | 判定器と、誤りを仕込んだ列での監査 |
| `aggregate.py` | `results/a/summary.md` を結果のファイルから作る |
| `run.py`・`run.sh` | 題材 A を最初から最後まで流す（追加実験 A-P1 は `poll` と名指ししたときだけ） |
| `memlog.py` | 通しの実行のあいだ、約 60 秒おきに Windows の空きメモリ、vmmemWSL の作業セット、WSL の中の used と buff/cache を `results/a/memlog.jsonl` に追記する |
| `py.sh` | リポジトリのモジュールを実験用の venv で動かす（`bash cs/a/py.sh cs.a.<モジュール> [引数]`） |
| `dev_one.py`・`dev_many.sh` | 構成とケースを 1 つ選んで 1 回だけ流し、結果を表示する（結果のファイルには書かない。作りを変えたときの確認用） |
| `dev_wal.py` | CDC の落とし穴を 1 条件だけ流して表示する（同上） |

compose のファイルは置いていない。題材 D と同じく、実験の途中でコンテナを止める・強制終了する・起動し直す操作が多いので、`docker run` を Python（`cs/common/dockerutil.py`）から呼んでいる。

## 製品と版（2026-11-18 に公式の配布元でタグを確かめて固定）

| 製品 | イメージ |
| --- | --- |
| PostgreSQL 18 | `postgres:18.7-alpine3.24` |
| Redpanda | `docker.redpanda.com/redpandadata/redpanda:v26.2.7` |
| Apache Kafka（KRaft、1 ノード） | `apache/kafka:4.4.0` |
| Debezium（Kafka Connect 版） | `quay.io/debezium/connect:3.7.1.Final` |
| Toxiproxy | `ghcr.io/shopify/toxiproxy:2.13.0` |

ダイジェストは `results/env.json`。試験用のパスワード（`dicpass`）はコンテナの中だけで使う固定値。Python のクライアントは confluent-kafka（librdkafka）と psycopg 3。

## 設定

### Redpanda：開発用の設定を切る

このイメージは、何も指定しないと開発用の設定で起動する（`rpk redpanda start --mode help` の表示と、起動後の値で確かめた）：ブローカーに `--unsafe-bypass-fsync` を渡し、クラスタ設定 `write_caching_default` を true にする。欠落を数える実験なので、次のように切り、実行のたびに値を読んで結果に記録している（切れていなければ実行を止める）。

- 起動の引数：`--unsafe-bypass-fsync=false`、`--set redpanda.developer_mode=false`（ほかに `--smp 1 --memory 1G --reserve-memory 0M --overprovisioned --check=false`）
- 起動後：`rpk cluster config set write_caching_default false`、`rpk cluster config set auto_create_topics_enabled false`

Apache Kafka は既定のまま（メッセージごとの fsync はしない）。トピックの自動作成だけ切った。

### PostgreSQL

- 正の側：`wal_level=logical`、`max_wal_senders=4`、`max_replication_slots=4`、`shared_buffers=64MB`。`wal.py` では条件ごとに `max_slot_wal_keep_size`（-1／64MB）か `idle_replication_slot_timeout=20s` を足す
- 導出の側：`synchronous_commit=off`（受け手が書き手に追いつけるようにするため。導出の側は落とさない）

### Debezium 3.7.1（Kafka Connect）

公式文書（<https://debezium.io/documentation/reference/3.7/connectors/postgresql.html>、<https://debezium.io/documentation/reference/3.7/transformations/outbox-event-router.html>）で確かめた。コネクタの設定は `infra.py` の `connector_config()`、実際に使った値は BE の各行の `connector_config`。

| 設定 | 値 | 既定と理由 |
| --- | --- | --- |
| `plugin.name` | `pgoutput` | 既定は `decoderbufs`。PostgreSQL に組み込みのプラグインを使う |
| `publication.autocreate.mode` | `filtered` | 既定は `all_tables`。publication をアウトボックスの表だけにする |
| `table.include.list` | `public.outbox` | 取り込むのはアウトボックスの表だけ |
| `snapshot.mode` | `no_data` | スロットを作った時点から流す。スロット喪失後の復旧だけ `initial` |
| `heartbeat.interval.ms` | `1000` | 既定は 0（送らない） |
| `poll.interval.ms` | 本実験では指定しない。追加実験 A-P1 だけ、指定なし・`100`・`50`・`10` | 既定は 500。下の「追加実験 A-P1」 |
| `topic.creation.default.replication.factor`・`.partitions` | `1`・`1` | ブローカーのトピックの自動作成を切ったので、ハートビート用のトピックを Kafka Connect に作らせる。無いと、コネクタは RUNNING のまま発行が止まった |
| `transforms.outbox.type` | `io.debezium.transforms.outbox.EventRouter` | Outbox Event Router |
| `transforms.outbox.table.field.event.id` | `event_id` | イベント ID の列（既定は `id`） |
| `transforms.outbox.table.field.event.key` | `aggregateid`（除去実験では `event_id`） | メッセージのキー。注文 ID |
| `transforms.outbox.table.field.event.payload` | `payload` | JSON の文字列を入れた text の列 |
| `transforms.outbox.route.topic.replacement` | `dic.orders` | 送り先のトピック（4 パーティション。先に作っておく） |
| `key.converter`・`value.converter` | `StringConverter` | キーも値も文字列のまま送る |
| Kafka Connect の `OFFSET_FLUSH_INTERVAL_MS` | `10000` | 既定は 60000。再送の範囲がこの間隔で決まる |
| Kafka Connect の `HEAP_OPTS` | `-Xms256M -Xmx512M` | メモリを抑える |

### 追加実験 A-P1：コネクタの読み取り間隔（`poll.interval.ms`）

本実験の A-N1 で、障害なし・並行度 1 の遅れは BE のほうが B より長かった。BE の遅れがコネクタの読み取り間隔で変わるかを確かめるために、`poll.interval.ms` だけを変えて流す（`run.py` の `plan_poll()`、`infra.py` の `connector_config(poll_interval_ms=...)`）。

- 設定の名前・既定値・意味は、Debezium 3.7 の PostgreSQL コネクタの文書で確かめた（<https://debezium.io/documentation/reference/3.7/connectors/postgresql.html#postgresql-property-poll-interval-ms>。「Advanced Debezium PostgreSQL connector configuration properties」の表。確認日 2026-11-20）。`poll.interval.ms`：既定 500。正の整数で、コネクタが反復ごとに新しい変更イベントの出現を待つミリ秒数。5000 より大きい値を入れても、実際の待ち時間は 5000 ms で頭打ちになる
- 条件：BE で `poll.interval.ms` を、指定なし（＝本実験と同じ登録内容）・100・50・10 の 4 通り。比較のために B（ポーリングの間隔 50 ms）も同じセッションで流す。障害なし・並行度 1、負荷は A-N1 と同じ生成器と種、各 5 試行。条件は試行ごとにひと回りさせ（試行 1 で 5 条件、次に試行 2…）、最初に流す条件も試行ごとに 1 つずつずらす
- **変えるのは `poll.interval.ms` の 1 つだけ。** 同じ表にある `poll.dispatch.interval.ms`（既定 0）、`max.batch.size`（既定 2048）、`max.queue.size`（既定 8192）と、Kafka Connect のプロデューサーの設定（linger など）は指定していない（製品の既定のまま）
- 実行 ID は `poll:redpanda:be:pi<default|100|50|10>:c1:t<n>` と `poll:redpanda:b:relay50:c1:t<n>`。各行に、登録の後に Kafka Connect の REST（`GET /connectors/<名前>/config`）で読み返した設定（`connector_config_registered`。パスワードは除く）、流した順（`order`）、実行の直前の空きメモリ（`memory_before`）を記録する。指定しなかった設定は読み返しに現れない
- この追加実験は 1 回だけ流した（2026-11-20）。2 回の比較（`results/RERUN.md`）の対象ではなく、summary では「A-P1」の節に、1 回だけの結果として書く。流したときは 3 つに区切り、区切りの間に 2 回、WSL のページキャッシュを捨てた（空きメモリを保つため。直後の実行は `poll:redpanda:be:pi50:c1:t3` と `poll:redpanda:be:pi50:c1:t4`）

### クライアント

- 発行（N の注文サービス、B の運び手）：`acks=all`、`enable.idempotence=true`。N は確認応答を 3 秒まで待ち、来なければ要求を失敗として返す（DB はコミット済み）
- 受け手：オフセットの自動コミットなし。効果をコミットした後、10 件ごとと、読むものが無くなったときにオフセットを記録する。役割ごとに 1 プロセスで、4 パーティションを固定で割り当てる

## 判定

`judges.py` は、生成器が決めたイベント ID（書き手 ID＋連番）の集合、呼び出し元の記録（応答済みか）、注文の表と履歴（正）、トピックを先頭から読み直した列、受け手の記録（渡された回数、状態に効いた回数）、索引の最終状態を突き合わせる。ログの文言や壁時計の時刻は使わない（遅れの計測だけ、同じホストの時計を使う）。`audit.py` が、欠落・重複・逆順・食い違い・DB に無いイベントを仕込んだ列で、仕込んだ数だけを数えることを確かめる。
