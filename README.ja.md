# tasuki

[English](README.md) | 日本語

Go アプリケーションに組み込んで使う durable workflow engine。
名前は駅伝の襷（tasuki）から。走者から走者へ襷を引き継いで長い距離を走り切るように、ワーカーからワーカーへ実行を引き継いでワークフローを完走させる。

Temporal のような「リトライ、タイマー、状態永続化を自分で書かず、ワークフローをコードとして書く」体験を、専用サーバーなしで提供する。
エンジンはライブラリとしてアプリケーションプロセス内で動き、永続化はアプリケーションが持つデータストアに相乗りする。
バックエンドはインターフェースで差し替え可能で、参照実装は PostgreSQL、対応対象に MySQL / MariaDB、SQLite、Spanner、TiDB、DynamoDB、Firestore を含む。

## ステータス

M4（バックエンド拡充）完了。PostgreSQL / SQLite / MySQL・MariaDB・TiDB / Spanner / DynamoDB / Firestore が適合・カオス可能な状態。
M5（性能と拡張）と M6（Query / Signal dedupe / Nack / SearchAttributes / Memo / LocalActivity / StartToClose / Update / SignalBatch など）は実装済。
品質スプリント（CI マトリクス・カバレッジ計測・ドキュメント整備）完了。詳細は [docs/ja/04-testing.md](docs/ja/04-testing.md)。
M5 のベンチマーク基盤: `go run ./cmd/bench`（memory / postgres / sqlite）。
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory / sqlite / mysql / spanner は同一 `Backend` 内のプロセス内 wakeup（`backend/hub`）。DynamoDB は `wf_wake`（Streams 有効）＋ wake アイテムポーリング、Firestore は `wf_notify` の Snapshot で**プロセスをまたいだ**起床もできる（いずれも hint。正しさは Claim / GetInstance）。
Client の `Result` は postgres（`tasuki_terminal`）および他ストア（hub / 上記 cross-process 経路）で終端時に起床できる。
Worker はインスタンスごとの sticky ジャーナルキャッシュ（`next_seq` 照合、差分は `GetJournal`）でフル履歴の再読を減らす。
tasuki は各 workflow instance を durable actor として扱い、journal replay モデルを維持する。
ジャーナル件数が `JournalWarnThreshold`（既定 10000、負数で無効）以上のとき Worker は Warn ログとメトリクスを出す。長寿命ワークフローは `workflow.ContinueAsNew` で履歴を打ち切る（[docs/ja/02-architecture.md](docs/ja/02-architecture.md)、[docs/ja/03-api.md](docs/ja/03-api.md)）。
長時間アクティビティは `activity.RecordHeartbeat` でリース延長と進捗記録ができ、リトライ時に `GetHeartbeatDetails` で取り出せる。
アクティビティの 1 試行上限は `workflow.WithStartToCloseTimeout`（超過は通常失敗としてリトライ対象）。
ワークフローの読み取り専用問い合わせは `workflow.SetQueryHandler` と `tasuki.Query`（Worker 同一プロセス）で行う。
実行中ワークフローへの同期 Update は `workflow.SetUpdateHandler` と `tasuki.Update`（任意 `WithUpdateID`）。
`Client.Signal` は `WithDedupeID` でインスタンス単位の再送冪等にできる。
同一インスタンスへの一括送信は `Client.SignalBatch`（原子的。item ごとの任意 dedupe）。
ローリング中に旧 Worker が新履歴を扱えない場合はタスクを Nack し、新 Worker が拾えるようにする（`IncompatibleRetryDelay`）。
検索属性は `WithSearchAttributes`（Start）と `workflow.UpsertSearchAttributes` で付け、`Client.List` の完全一致フィルタで絞り込める。
メモは `WithMemo` / `workflow.UpsertMemo` で付け、Get で見える表示用注釈（List フィルタには使わない）。
短い同一 Worker 実行は `workflow.ExecuteLocal`（アクティビティタスクキューなし・リトライなし。結果はジャーナルに記録）。
ペイロードの at-rest 暗号化は `codec.Encrypted`（AES-256-GCM、鍵ローテーション対応。Worker の `Codec` と Client の `WithCodec` に設定）。

## クイックスタート

PostgreSQL:

```bash
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./examples/m1-postgres/
```

MySQL:

```bash
docker compose up -d mysql
export TASUKI_MYSQL_DSN='tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true&loc=UTC'
go run ./examples/m4-mysql/
```

TiDB（`backend/mysql` を流用）:

```bash
docker compose up -d tidb
export TASUKI_TIDB_DSN='root@tcp(127.0.0.1:4000)/tasuki?parseTime=true&loc=UTC'
export TASUKI_MYSQL_DSN="$TASUKI_TIDB_DSN"
go run ./examples/m4-mysql/
```

Spanner（Emulator）:

```bash
docker compose up -d spanner
export SPANNER_EMULATOR_HOST=localhost:9010
export TASUKI_SPANNER_DSN=projects/tasuki/instances/tasuki/databases/tasuki
go run ./examples/m4-spanner/
```

DynamoDB（Local）:

```bash
docker compose up -d dynamodb
export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_ACCESS_KEY_ID=local AWS_SECRET_ACCESS_KEY=local AWS_REGION=us-east-1
go run ./examples/m4-dynamodb/
```

Firestore（Emulator）:

```bash
docker compose up -d firestore
export FIRESTORE_EMULATOR_HOST=localhost:8086
export TASUKI_FIRESTORE_PROJECT=tasuki
go run ./examples/m4-firestore/
```

SQLite（追加インフラ不要）:

```bash
go run ./examples/m3-sqlite/
```

決定性解析:

```bash
go run ./analyzers/determinism/cmd/determinism -- ./...
```

`*workflow.Context` レシーバを持つワークフロー関数内の非決定的呼び出しを検出する:
`go` 文、`time.Now/Since/Until/Sleep/After/AfterFunc/NewTimer/NewTicker/Tick`
（時刻は `workflow.Now`、タイマーは `workflow.Sleep` を使用）、`math/rand`・
`math/rand/v2`・`crypto/rand`（`workflow.SideEffect` または `workflow.NewUUID` を使用）、
`os.Getenv/LookupEnv/Environ/Hostname/Getpid/Getppid/Getwd/Executable` と `os.Args`
（入力やアクティビティ経由で渡す）、`sync`・`sync/atomic`・`runtime`、
`net`・`net/http`・`os/exec`（I/O はアクティビティで行う）、チャネル操作（`select`・
送受信・`make(chan ...)`・チャネルに対する `range` は `workflow.Execute`/`ExecuteAsync` と `workflow.Await` を使用）
および map に対する `range`（順序がランダム）。`workflow.SideEffect`/`NewUUID`/
`SetQueryHandler` に渡すクロージャは除外される（`SetUpdateHandler` のハンドラは解析対象）。
ワークフローから呼ばれるヘルパー関数は解析しないため、決定的に保つこと。

## 閲覧 UI（contrib）

インスタンス一覧・ジャーナルビューア。詳細ページでは running インスタンスを Cancel / Terminate / Signal できる（Cancel・Terminate は確認チェック、Signal は name + JSON。いずれも CSRF。Cancel は協調キャンセル、Terminate は即時終了）。

既定は loopback（`127.0.0.1:8080`）のみで待ち受け、HTTP タイムアウト付き（ReadHeader 5s / Read 10s / Write 15s / Idle 60s）。外部公開はトークン必須。無認証の外部公開は `--allow-unauthenticated-external` の明示 opt-in が必要（危険）:

```bash
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=127.0.0.1:8080
export TASUKI_UI_TOKEN='change-me'
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"
# open http://127.0.0.1:8080
# 外部公開（認証付き）
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=0.0.0.0:8080 -token="$TASUKI_UI_TOKEN"
# 共有デプロイ例（TLS 終端 + 認証）: リバースプロキシで TLS を終端し、UI は loopback + token で起動する
# Caddy 例:
# example.com {
#   reverse_proxy 127.0.0.1:8080
# }
# TASUKI_UI_TOKEN='...' go run ./contrib/ui/cmd/tasuki-ui -backend=postgres -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=postgres -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_SQLITE_PATH=./tasuki.db
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=sqlite -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_MYSQL_DSN='tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=mysql -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_SPANNER_DSN='projects/p/instances/i/databases/d'
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=spanner -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=dynamodb -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"

export FIRESTORE_EMULATOR_HOST=localhost:8081
export TASUKI_FIRESTORE_PROJECT=tasuki
go run -tags tasuki_all ./contrib/ui/cmd/tasuki-ui -backend=firestore -addr=127.0.0.1:8080 -token="$TASUKI_UI_TOKEN"
```

## ベンチマーク

E2E スループット（投入開始→全完了の完走インスタンス数 / 秒）と遅延分布（p50/p95/p99）を測る。
既定で既存データを削除しない（`--reset` + `TASUKI_ALLOW_RESET=1` の明示指定のみ削除）。同一ストアでの繰り返しは `--run-id` で分離される:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -claim-limit=50 -activity-concurrency=8 -workflow-concurrency=8
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -scenario=mixed -run-id=try1
go run ./cmd/bench -backend=memory -instances=50 -workers=2 -scenario=long-history
# 破壊的リセット（対象を確認して明示指定）
TASUKI_ALLOW_RESET=1 go run ./cmd/bench -backend=memory -instances=200 -workers=4 --reset
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4
export TASUKI_SQLITE_PATH=./bench.db
go run -tags tasuki_all ./cmd/bench -backend=sqlite -instances=200 -workers=4
```

`WorkerOptions.ClaimLimit`（デフォルト 10）で 1 tick あたりの Claim 件数を変えられる。bench では `-claim-limit`。

全ストアで 1 tick 内の複数ワークフロー前進を `CommitAdvancements` にまとめられる（DynamoDB は TransactWrite 100 件超で逐次フォールバック）。
`WorkerOptions.ActivityConcurrency`（デフォルト 1）で Claim 済み activity の並列度を変えられる。bench では `-activity-concurrency`。
`WorkerOptions.WorkflowConcurrency`（デフォルト 1）で Claim 済み workflow の並列度を変えられる（同一 instance はプロセス内で直列）。bench では `-workflow-concurrency`。

基準メモ: [docs/ja/06-bench-baseline.md](docs/ja/06-bench-baseline.md)。

テスト:

```bash
go test ./... -race
cd backend/postgres && go test ./...
cd backend/mysql && go test ./...
cd backend/spanner && go test ./...
cd backend/dynamodb && go test ./...
cd backend/firestore && go test ./...
cd backend/sqlite && go test ./...
go test ./chaos/ -timeout 5m
```

観測性: [docs/ja/05-observability.md](docs/ja/05-observability.md)

## 設計ドキュメント

| 文書 | 内容 |
|---|---|
| [docs/ja/01-overview.md](docs/ja/01-overview.md) | 目的、要求、非目標、既存プロダクト比較、用語 |
| [docs/ja/02-architecture.md](docs/ja/02-architecture.md) | 実行モデル、exactly-once 状態遷移プロトコル、データモデル、通知・起床機構 |
| [docs/ja/03-api.md](docs/ja/03-api.md) | 公開 API、コード例、決定性の制約、テスト支援 |
| [docs/ja/04-testing.md](docs/ja/04-testing.md) | テスト戦略、検証手法、リスクと対策 |
| [docs/ja/05-observability.md](docs/ja/05-observability.md) | ログと OpenTelemetry メトリクス、トラブルシューティング |
| [docs/ja/06-bench-baseline.md](docs/ja/06-bench-baseline.md) | ベンチマーク基準測定結果と測定手順 |
| [docs/ja/07-retention.md](docs/ja/07-retention.md) | 完了済みインスタンスのデータ保持と削除（Retention） |
| [docs/ja/08-fair-dispatch.md](docs/ja/08-fair-dispatch.md) | 公平ディスパッチと高負荷ワークフローの隔離 |
| [docs/ja/09-limits.md](docs/ja/09-limits.md) | バックエンドの原子性予算と制限 |
| [docs/ja/10-modules.md](docs/ja/10-modules.md) | モジュール構成、バージョン付け、公開手順 |
