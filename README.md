# tasuki

Go アプリケーションに組み込んで使う durable workflow engine。
名前は駅伝の襷（tasuki）から。走者から走者へ襷を引き継いで長い距離を走り切るように、ワーカーからワーカーへ実行を引き継いでワークフローを完走させる。

Temporal のような「リトライ、タイマー、状態永続化を自分で書かず、ワークフローをコードとして書く」体験を、専用サーバーなしで提供する。
エンジンはライブラリとしてアプリケーションプロセス内で動き、永続化はアプリケーションが持つデータストアに相乗りする。
バックエンドはインターフェースで差し替え可能で、参照実装は PostgreSQL、対応対象に MySQL / MariaDB、SQLite、Spanner、TiDB、DynamoDB、Firestore を含む。

## ステータス

M4（バックエンド拡充）完了。PostgreSQL / SQLite / MySQL·MariaDB·TiDB / Spanner / DynamoDB / Firestore が適合・カオス可能な状態。
M5 着手中（性能と拡張）。ベンチマーク基盤: `go run ./cmd/bench`（memory / postgres）。
PostgreSQL Worker は `LISTEN`/`NOTIFY`（チャネル `tasuki_tasks`）で起床し、`PollInterval` はフォールバックおよびタイマー／スケジュール用。memory / sqlite / mysql / spanner は同一 `Backend` 内のプロセス内 wakeup（`backend/hub`）。DynamoDB は `wf_wake`（Streams 有効）＋ wake アイテムポーリング、Firestore は `wf_notify` の Snapshot で**プロセスをまたいだ**起床もできる（いずれも hint。正しさは Claim / GetInstance）。
Client の `Result` は postgres（`tasuki_terminal`）および他ストア（hub / 上記 cross-process 経路）で終端時に起床できる。
Worker はインスタンスごとの sticky ジャーナルキャッシュ（`next_seq` 照合、差分は `GetJournal`）でフル履歴の再読を減らす。
ジャーナル件数が `JournalWarnThreshold`（既定 10000、負数で無効）以上のとき Worker は Warn ログとメトリクスを出す。長寿命ワークフローは `workflow.ContinueAsNew` で履歴を打ち切る（[docs/02-architecture.md](docs/02-architecture.md)、[docs/03-api.md](docs/03-api.md)）。
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

## 閲覧 UI（contrib）

インスタンス一覧・ジャーナルビューア。詳細ページでは running インスタンスを Cancel / Terminate / Signal できる（Cancel・Terminate は確認チェック、Signal は name + JSON。いずれも CSRF。Cancel は協調キャンセル、Terminate は即時終了）。

共有デプロイでは `TASUKI_UI_TOKEN` または `-token` で共有シークレットを設定する（Bearer / HTTP Basic。未設定なら認証なし）:

```bash
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=:8080
export TASUKI_UI_TOKEN='change-me'
go run ./contrib/ui/cmd/tasuki-ui -backend=memory -addr=:8080 -token="$TASUKI_UI_TOKEN"
# open http://localhost:8080
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./contrib/ui/cmd/tasuki-ui -backend=postgres -addr=:8080

export TASUKI_SQLITE_PATH=./tasuki.db
go run ./contrib/ui/cmd/tasuki-ui -backend=sqlite -addr=:8080

export TASUKI_MYSQL_DSN='tasuki:tasuki@tcp(localhost:3306)/tasuki?parseTime=true'
go run ./contrib/ui/cmd/tasuki-ui -backend=mysql -addr=:8080

export TASUKI_SPANNER_DSN='projects/p/instances/i/databases/d'
go run ./contrib/ui/cmd/tasuki-ui -backend=spanner -addr=:8080

export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
go run ./contrib/ui/cmd/tasuki-ui -backend=dynamodb -addr=:8080

export FIRESTORE_EMULATOR_HOST=localhost:8081
export TASUKI_FIRESTORE_PROJECT=tasuki
go run ./contrib/ui/cmd/tasuki-ui -backend=firestore -addr=:8080
```

## ベンチマーク

E2E スループット（完走インスタンス数 / 秒）を測る:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -claim-limit=50 -activity-concurrency=8 -workflow-concurrency=8
docker compose up -d postgres
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./cmd/bench -backend=postgres -instances=200 -workers=4
export TASUKI_SQLITE_PATH=./bench.db
go run ./cmd/bench -backend=sqlite -instances=200 -workers=4
```

`WorkerOptions.ClaimLimit`（デフォルト 10）で 1 tick あたりの Claim 件数を変えられる。bench では `-claim-limit`。

全ストアで 1 tick 内の複数ワークフロー前進を `CommitAdvancements` にまとめられる（DynamoDB は TransactWrite 100 件超で逐次フォールバック）。
`WorkerOptions.ActivityConcurrency`（デフォルト 1）で Claim 済み activity の並列度を変えられる。bench では `-activity-concurrency`。
`WorkerOptions.WorkflowConcurrency`（デフォルト 1）で Claim 済み workflow の並列度を変えられる（同一 instance はプロセス内で直列）。bench では `-workflow-concurrency`。

詳細は [docs/superpowers/specs/2026-07-24-m5-bench-design.md](docs/superpowers/specs/2026-07-24-m5-bench-design.md)。

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

観測性: [docs/05-observability.md](docs/05-observability.md)

## 設計ドキュメント

| 文書 | 内容 |
|---|---|
| [docs/01-overview.md](docs/01-overview.md) | 目的、要求、非目標、既存プロダクト比較、用語 |
| [docs/02-architecture.md](docs/02-architecture.md) | 実行モデル、exactly-once 状態遷移プロトコル、データモデル |
| [docs/03-api.md](docs/03-api.md) | 公開 API、コード例、決定性の制約、テスト支援 |
| [docs/04-plan.md](docs/04-plan.md) | マイルストーン、テスト戦略、リスク |
| [docs/05-observability.md](docs/05-observability.md) | ログと OpenTelemetry メトリクス |
| [docs/superpowers/plans/2026-07-23-m0-execution-model.md](docs/superpowers/plans/2026-07-23-m0-execution-model.md) | M0 実装プラン |
| [docs/superpowers/plans/2026-07-23-m1-postgres-backend.md](docs/superpowers/plans/2026-07-23-m1-postgres-backend.md) | M1 実装プラン |
| [docs/superpowers/plans/2026-07-23-m2-expressiveness.md](docs/superpowers/plans/2026-07-23-m2-expressiveness.md) | M2 実装プラン |
| [docs/superpowers/plans/2026-07-23-m3-operability.md](docs/superpowers/plans/2026-07-23-m3-operability.md) | M3 実装プラン |
| [docs/superpowers/plans/2026-07-23-m4-mysql-backend.md](docs/superpowers/plans/2026-07-23-m4-mysql-backend.md) | M4 MySQL 実装プラン |
| [docs/superpowers/plans/2026-07-23-m4-tidb.md](docs/superpowers/plans/2026-07-23-m4-tidb.md) | M4 TiDB 検証プラン |
| [docs/superpowers/specs/2026-07-23-m4-spanner-design.md](docs/superpowers/specs/2026-07-23-m4-spanner-design.md) | M4 Spanner 設計 |
| [docs/superpowers/plans/2026-07-23-m4-spanner.md](docs/superpowers/plans/2026-07-23-m4-spanner.md) | M4 Spanner 実装プラン |
| [docs/superpowers/specs/2026-07-23-m4-dynamodb-design.md](docs/superpowers/specs/2026-07-23-m4-dynamodb-design.md) | M4 DynamoDB 設計 |
| [docs/superpowers/plans/2026-07-23-m4-dynamodb.md](docs/superpowers/plans/2026-07-23-m4-dynamodb.md) | M4 DynamoDB 実装プラン |
| [docs/superpowers/specs/2026-07-23-m4-firestore-design.md](docs/superpowers/specs/2026-07-23-m4-firestore-design.md) | M4 Firestore 設計 |
| [docs/superpowers/plans/2026-07-23-m4-firestore.md](docs/superpowers/plans/2026-07-23-m4-firestore.md) | M4 Firestore 実装プラン |
