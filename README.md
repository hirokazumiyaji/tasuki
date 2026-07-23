# tasuki

Go アプリケーションに組み込んで使う durable workflow engine。
名前は駅伝の襷（tasuki）から。走者から走者へ襷を引き継いで長い距離を走り切るように、ワーカーからワーカーへ実行を引き継いでワークフローを完走させる。

Temporal のような「リトライ、タイマー、状態永続化を自分で書かず、ワークフローをコードとして書く」体験を、専用サーバーなしで提供する。
エンジンはライブラリとしてアプリケーションプロセス内で動き、永続化はアプリケーションが持つデータストアに相乗りする。
バックエンドはインターフェースで差し替え可能で、参照実装は PostgreSQL、対応対象に MySQL / MariaDB、SQLite、Spanner、TiDB、DynamoDB、Firestore を含む。

## ステータス

M2（表現力）完了。Async/Await、シグナル、子ワークフロー、SideEffect、GetVersion、Cancel、ContinueAsNew が使える。
適合テスト（memory / postgres）、カオステスト、ドキュメント例テストが動く。
次は M3（運用性: SQLite、cron、OTel、静的解析など）へ進む。

## クイックスタート

```bash
docker compose up -d
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./examples/m1-postgres/
```

期待出力: `inv-123`

テスト:

```bash
go test ./... -race
cd backend/postgres && go test ./...   # 要 TASUKI_POSTGRES_DSN
go test ./chaos/ -timeout 2m          # 要 TASUKI_POSTGRES_DSN
```

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
