# 題材 C：共通基盤の費用と性能（ログ・分析）

設計は [research/DESIGN.md](../../research/DESIGN.md) の 5 節。結果は [results/c/summary.md](../../results/c/summary.md)。

問い：ログ基盤・分析基盤で、どの持ち方が何を払うか。架空の宅配注文サービスの、注文イベントのログ（1 行が 1 イベント、23 列）を、行指向（PostgreSQL 18）・列指向（ClickHouse）・ファイル（DuckDB＋Parquet）に入れて比べる。

| 実験 | 中身 | 比べるもの |
| --- | --- | --- |
| C-1 行指向と列指向 | 同じ CSV を 3 製品に入れ、容量、取り込み時間、6 本の問い合わせ（点の検索、範囲、集計 2 本、上位 N、キーでない列の絞り込み）。集計 1 本が読んだ量、1 行ずつの書き込み、PostgreSQL で集計を並べたときの検索と挿入 | 3 製品 × 3 規模（10 万・100 万・300 万行）× 5 試行 |
| C-2 取り込み時スキーマと読み出し時スキーマ | (1) 型つきの列／JSON のまま、の容量・取り込み・問い合わせ。(2) 上流のスキーマが揺れたとき（型が変わる、項目が消える・増える、名前が変わる、null）に、失敗がどこに出るか | (1) 6 通りの持ち方 × 5 試行。(2) 17 通りの取り込みの作り × 8 ケース |
| C-3 保持と圧縮 | 並び順とコーデックを変えたときの容量と時間。古い 10 日分を落とす操作の時間と容量 | 並び順 3 × コーデック、落とし方 9 通り、各 5 試行 |
| C-4 冪等な再実行 | 同じ範囲のバッチを 2 回流す（同じ入力／訂正後の入力／1 回目が半分で止まる）。続けて次の日のバッチを流す | 流し方 15 通り × 場面 3 |
| C-5 索引の費用 | 索引を足す前後の、取り込み、挿入、容量、読み取り（PostgreSQL の B ツリー・INCLUDE・GIN、ClickHouse のスキップ索引、DuckDB の索引） | 11 通り × 5 試行 |
| C-6 Collector の送信で失う場面 | OpenTelemetry Collector のメモリのキュー／ディスクのキュー／Kafka を間に置く設計で、下流の停止・強制終了・あふれ・再試行の時間切れ・保持の超過のときに、受け付け済みのログを失うか | 4 設計 × 4〜6 場面 × 3 試行 |

構成は **N**（素朴な作り：行指向の表に、製品の既定のまま型つきで取り込み、INSERT で流し直し、DELETE で古い行を消す）、**B**（共通の基準：再実行しても同じ結果になる取り込み）、**BE**（基準＋題材 C の重点：E-C1 列指向と圧縮、E-C2 取り込み時に型を決める列と後から読む列を分けて検査する、E-C3 保持期間をパーティションの単位で落とす）。除去実験の表は、対応する実験の数字から作る（`agg_sections.ablation`）。

## 流し方

前提：WSL2（Ubuntu-24.04）の中で Docker Engine が使えること、`uv` があること。空きメモリは 3 GB 以上。同時に動かすサーバーのコンテナは 1 つ（上限 1.5 GB）。C-6 だけ Collector 2 つ（各 256 MB）と Redpanda（1.2 GB）。

```bash
# Windows の Git Bash から（WSL の中なら bash cs/c/run.sh だけでよい）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/setup_venv.sh   # 初回だけ（DuckDB・numpy・pyarrow を入れる）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/pull_images.sh  # 初回だけ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/run.sh          # 全部（片付けは含まない）
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/run.sh c1 c3    # 段を選ぶ
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/run.sh --list   # 実行の一覧
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/go.sh c1        # 9 分で区切って流す
MSYS_NO_PATHCONV=1 wsl -d Ubuntu-24.04 -- bash /mnt/<ドライブ>/<リポジトリ>/cs/c/run.sh clean    # dic-c-* と、生成したデータを消す
```

- 段は `audit`（判定器の監査）→ `c1`〜`c6` → `summary`。`clean` は指定したときだけ動く。`--match=<文字列>` で、実行 ID にその文字列を含むものだけを流す。`--fresh` で `results/c/*.jsonl` を消して最初から
- 結果は 1 回の実行を 1 行として `results/c/<段>.jsonl` に追記する。完了済みの実行 ID は飛ばすので、途中で止めても続きから流せる。止められた途中の実行は記録されず、次に流したときに最初からやり直す（どの実行も、最初に `dic-c-` のコンテナとボリュームを消す）
- `go.sh` は、`run.sh` を 9 分（`DIC_SLICE_S` 秒）で打ち切り、結果の行数、残っている `dic-c-` のコンテナ、Windows の空きメモリを表示する。全部で約 2 時間半かかるので、区切って繰り返す
- 作るコンテナ・ボリューム・ネットワークの名前はすべて `dic-c-` で始まる。それ以外には触れない
- ホストのポートは 25732（PostgreSQL）・25723（ClickHouse の HTTP）・25718（Collector の OTLP/HTTP）が 127.0.0.1、25719（C-6 の受け口。コンテナから届くよう WSL の中の全アドレスで待つ）
- 生成したデータ（CSV・NDJSON・Parquet、製品の設定ファイル）は、リポジトリの外の `~/dic-cs/data-c/`（`DIC_DATA_C` で変えられる）に置き、コンテナへは読み取り専用で見せる。データセットは種と行数が同じなら使い回し、無ければ作り直す。終わったら `run.sh clean` で消す
- Windows の空きメモリが 3 GB を切ると、コンテナを消して終了コード 3 で止まる

## ファイル

| ファイル | 中身 |
| --- | --- |
| `config.py` | イメージのタグ、ポート、メモリの上限、規模、繰り返しの回数 |
| `gen.py` | 生成器（注文イベントのログ）と正解。行は 10 万行ごとの塊を乱数の種から作り、CSV（と NDJSON）に書く。問い合わせの正解は、生成した配列から numpy で数える |
| `prod.py` | 3 製品の起動・待機・問い合わせ（PostgreSQL は psycopg、ClickHouse は HTTP、DuckDB はプロセスの中） |
| `queries.py` | 表の定義（製品ごとの型）、6 本の問い合わせ、結果を正解と比べる `result_diff` |
| `judges.py` | 判定器：揺れた行の分類（`classify_drift`）、集計の照合（`judge_sum`）、再実行の後の表の照合（`judge_table`） |
| `c1_rowcol.py`〜`c6_collector.py` | 各実験の実行 |
| `audit.py` | 判定器の監査（誤りを仕込んだデータで、仕込んだ数だけを数えることを確かめる） |
| `aggregate.py`・`agg_sections.py` | `results/c/summary.md` を結果のファイルから作る（照合の判定、除去実験、気づいた点も数から作る） |
| `run.py`・`run.sh`・`go.sh` | 題材 C を最初から最後まで流す／9 分で区切って流す |
| `setup_venv.sh`・`pull_images.sh`・`py.sh` | 環境の用意、リポジトリのモジュールを実験用の venv で動かす |
| `dev_one.py`・`dev.sh`・`dev_probe.py`・`dev_env.sh` | 実行を選んで流して表示する、製品に文を投げて結果かエラーを見る、手元の状態を表示する（結果のファイルには書かない。作りを変えたときの確認用） |

compose のファイルは置いていない。ほかの題材と同じく、`docker run` を Python（`cs/common/dockerutil.py`）から呼んでいる。

## 製品と版

| 製品 | イメージ・パッケージ | 使う実験 |
| --- | --- | --- |
| PostgreSQL 18 | `postgres:18.7-alpine3.24`（題材 D で pull 済み） | C-1〜C-5 |
| ClickHouse | `clickhouse:26.8.36.3`（題材 B で pull 済み） | C-1〜C-5 |
| DuckDB | Python のパッケージ `duckdb` 2.0.0（venv に入れた。Parquet の読み書きも DuckDB） | C-1〜C-5 |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib:0.165.0`（2026-11-20 に Docker Hub でタグを確かめて pull。525 MB） | C-6 |
| Redpanda | `docker.redpanda.com/redpandadata/redpanda:v26.2.7`（題材 A で pull 済み） | C-6 の Kafka の役 |

ダイジェストと、サーバーが返した版は `results/env.json`。生成器は numpy と pyarrow（CSV の書き出し）を使う。試験用のパスワード（`dicpass`）はコンテナの中だけで使う固定値。

## 設定

- **メモリ**：PostgreSQL はコンテナ 1536 MB・`shared_buffers=384MB`、ClickHouse はコンテナ 1536 MB・`max_server_memory_usage` 1 GB・`mark_cache_size` 128 MiB・`uncompressed_cache_size=0`、DuckDB は `memory_limit='1GB'`。ほかは製品の既定のまま（製品が表示した値を各実行の `settings` に記録）
- **表**：PostgreSQL は数値を bigint・integer・smallint、文字列を text、NULL を許すのは coupon_code と rating だけ。ClickHouse は Int64・Int32・Int16、種類の少ない文字列を LowCardinality(String)、NULL を許す 2 列を Nullable、`ORDER BY (ts, event_id)`。DuckDB は BIGINT・INTEGER・SMALLINT・VARCHAR で、Parquet は既定（Snappy）
- **取り込み**：サーバーが同じファイルを直接読む（PostgreSQL は `COPY … FROM '<ファイル>'`、ClickHouse は `INSERT … SELECT FROM file()`、DuckDB は `read_csv`）。PostgreSQL は取り込みの後に主キーと ts の索引を作り、`VACUUM (ANALYZE)` する。ClickHouse は `OPTIMIZE TABLE … FINAL` まで
- **問い合わせの時間**：1 回の実行の中で、暖機 1 回のあと 7 回。毎回、結果を生成器の正解と突き合わせる。OS のページキャッシュは捨てない
- **C-2 の BE**：必須の項目（event_id・ts・store_id・status・amount・distance_m）がそろい、整数の項目が JSON の整数である行だけを本体の表に入れ、それ以外は理由を付けて保留の表に入れる。知らないキーは別の列（PostgreSQL は jsonb、ClickHouse は JSON の文字列、DuckDB は JSON）に残す
- **C-3**：ClickHouse のコーデックは全列に同じものを指定。TTL は `now()` ではなく、固定の境目で表した式（壁時計に頼らないため）
- **C-4**：ClickHouse の INSERT は `async_insert=0`。削除は `mutations_sync=2`
- **C-6**：Collector の再試行は `initial_interval=500ms`・`max_interval=1s`・`max_elapsed_time=120s`（時間切れの場面だけ 3s）、`timeout=2s`。キューは既定（あふれの場面だけ `queue_size=10`）。ディスクのキューは `file_storage`（`fsync: true`）。Kafka を読む側は、既定のままと、`message_marking.after=true`＋`error_backoff`＋自前のキューなしの 2 通り。Redpanda は開発用の設定を切って起動（題材 A と同じ）。保持の場面のトピックは `retention.ms=2000`・`segment.ms=1000`

## 判定

- 問い合わせの結果：生成器が配列から数えた正解と、製品の結果を、行の集合と順序で比べる（`queries.result_diff`）。3 製品が同じ正解に一致したうえで時間を比べる
- スキーマの揺れ：生成器が「上流が意味していた値」を 1 行ずつ持ち、取り込みの後に読める値を、そのまま・NULL・0 で埋まる・値が変わる・文字列のまま・行が無い・保留・読めない、に分ける（`judges.classify_drift`）。続けて、その項目の合計と平均を正解と比べる
- 再実行：表にあるべき行（event_id と金額）を生成器から作り、重複・欠落・古い値・余分を数える（`judges.judge_table`）
- 保持：残った行数と金額の合計を、生成器の日ごとの正解と比べる
- Collector：送ったログの ID と HTTP の応答を正とし、受け口が最後に持っている ID と比べる（`c6_collector.judge_delivery`）
- `audit.py` が、これらに誤りを仕込んだデータと誤りの無いデータを流し、仕込んだ数だけを数えることを確かめる。結果は `results/c/audit.jsonl`

## 設計（DESIGN.md）から変えた点

- **公開のデータセットは使っていない**：DESIGN.md は「公開の小さなデータセットと、生成器で作るログ」としていた。公開のデータでは正解を生成器から作れないので、生成器のログだけにした
- **C-6 を足した**：ガイドの Part 13「テレメトリは失いうる前提」が、Collector の送信で「まだ失う場面」を題材 C で実測すると書いているため（DESIGN.md の表には無い）。OpenTelemetry Collector のイメージを新しく pull した
- **C-1 に足したもの**：集計が読んだ量、1 行ずつの書き込み、PostgreSQL で集計を並べたときの検索と挿入（ガイドの Part 5 の「使う列だけ読む」「1 行ずつの書き込みは非効率」「大きな集計が OLTP の資源を使う」に当たる点）
- **規模は 300 万行まで**：この機械のメモリの都合で、生の大きさは 542 MB まで。どの規模もメモリに収まるので、「1 つで足りる」規模の境目は引けていない
- **C-2 の ClickHouse の型つきの表は、列を Nullable にしていない**（よくある定義）。Nullable にした場合は測っていない
- **障害の入れ方（C-6）**：Toxiproxy と Pumba は使っていない。下流の停止は受け口が 503 を返す形、強制終了は `docker kill`、ブローカーの停止は `docker pause` で、要求の件数を合図に入れた
- **時間は縮めた（C-6）**：再試行の上限は 120 秒と 3 秒、保持は 2 秒。既定の 5 分は待っていない
- 測っていない点は `results/c/summary.md` の「限界」と「未検証」に書いた
