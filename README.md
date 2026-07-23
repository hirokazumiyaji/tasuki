# tasuki

Go アプリケーションに組み込んで使う durable workflow engine。
名前は駅伝の襷（tasuki）から。走者から走者へ襷を引き継いで長い距離を走り切るように、ワーカーからワーカーへ実行を引き継いでワークフローを完走させる。

Temporal のような「リトライ、タイマー、状態永続化を自分で書かず、ワークフローをコードとして書く」体験を、専用サーバーなしで提供する。
エンジンはライブラリとしてアプリケーションプロセス内で動き、永続化はアプリケーションが持つデータストアに相乗りする。
バックエンドはインターフェースで差し替え可能で、参照実装は PostgreSQL、対応対象に MySQL / MariaDB、SQLite、Spanner、TiDB、DynamoDB、Firestore を含む。

## ステータス

M4（MySQL / MariaDB + TiDB 検証）完了。`backend/mysql` を TiDB（unistore）でも適合・カオス済み。専用 TiDB モジュールは不要。
次のストア候補は Spanner → DynamoDB → Firestore。

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
# 例: MySQL 向け example と同じコードパス（DSN だけ差し替え）
export TASUKI_MYSQL_DSN="$TASUKI_TIDB_DSN"
go run ./examples/m4-mysql/
```

SQLite（追加インフラ不要）:

```bash
go run ./examples/m3-sqlite/
```

決定性解析:

```bash
go run ./analyzers/determinism/cmd/determinism -- ./...
```

テスト:

```bash
go test ./... -race
cd backend/postgres && go test ./...   # 要 TASUKI_POSTGRES_DSN
cd backend/mysql && go test ./...      # 要 TASUKI_MYSQL_DSN / TASUKI_TIDB_DSN
cd backend/sqlite && go test ./...
go test ./chaos/ -timeout 2m          # postgres / mysql / tidb の DSN
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
