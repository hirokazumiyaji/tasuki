# PostgreSQL マイグレーション

[English](README.md) | 日本語

tasuki のスキーマはバージョニングされたマイグレーションファイルで管理する。

```
migrations/
├── 000001_init.up.sql            基本テーブルとインデックス
├── 000001_init.down.sql          全テーブルの DROP
├── 000002_vacuum_tuning.up.sql   autovacuum / fillfactor 設定
├── 000002_vacuum_tuning.down.sql 設定のリセット
├── 000003_purge_search_indexes.up.sql   purge 用 (completed_at) + search_attributes 用 GIN インデックス
└── 000003_purge_search_indexes.down.sql 上記インデックスの削除
```

命名規則は `<6桁バージョン>_<名前>.up.sql` / `.down.sql`。ファイル名は辞書順 = 適用順になる。

## `Migrate()` の動作

`postgres.Backend.Migrate(ctx)` は次を行う。

1. 適用記録テーブル `tasuki_schema_migrations` を作成する。
2. 未適用のマイグレーションをバージョン順にトランザクション内で適用し、同じトランザクションで記録行を INSERT する。
3. 適用済みバージョンはスキップする。呼び出しは冪等で、並行呼び出しも安全（バージョン主キーによる原子的 claim）。

旧来の `schema.sql`（累積・冪等な 1 ファイル）で作成済みのデータベースは、`wf_instances` の存在を検出してバージョン 1 として記録し、以降の差分だけを適用する。

補助 API:

| API | 用途 |
|---|---|
| `postgres.LatestSchemaVersion()` | このビルドが埋め込む最新バージョン |
| `b.SchemaVersion(ctx)` | 適用済みの最大バージョン（未マイグレーションは 0） |
| `b.ValidateSchema(ctx)` | 必要テーブルの存在確認（未マイグレーション時のフェイルセーフ） |

Worker は起動時に `ValidateSchema` を自動で呼ぶ（`WorkerOptions.DisableSchemaValidation` で無効化）。テーブルが足りない場合はポーリングループを起動せずエラーログを出す。

## 外部ツールとの併用

マイグレーションファイルはプレーンな SQL なので、既存ツールからそのまま使える。アプリ起動時の自動マイグレーションを外したい場合（権限分離・ゼロダウンタイムデプロイ）は、CD パイプラインでツールに適用させ、`tasuki_schema_migrations` も同じトランザクションで記録すれば `Migrate()` は差分なしで冪等に動く。

### golang-migrate

```bash
migrate -path backend/postgres/migrations \
  -database 'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable' up
```

`migrate` 自身の `schema_migrations` テーブルでバージョン管理する。tasuki 側 `Migrate()` を併用する場合は、`Migrate()` の記録（`tasuki_schema_migrations`）とツール側の記録がずれないよう、どちらか一方を適用元に決めておく。

### goose

```bash
goose -dir backend/postgres/migrations postgres \
  'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable' up
```

注釈（`-- +goose Up`）のないファイルは全体が Up 扱いになるため、Down は `goose down` ではなくファイルを直接実行する運用でもよい。ツール独自のアノテーションを追記して使う構成も可能。

### Atlas

```bash
atlas migrate apply --dir file://backend/postgres/migrations \
  --url 'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
```

## 新しいマイグレーションの追加

1. `000004_<名前>.up.sql` / `.down.sql` を追加する。バージョンは必ず増分。
2. `.up.sql` は idempotent に書かなくてよい（適用は 1 回きり、失敗時はトランザクションでロールバックされる）。ただし `CREATE TABLE IF NOT EXISTS` のような冪等 DDL にしておくと、旧 schema.sql から移行したデータベースとの差異が吸収しやすい。
3. テストは実 PostgreSQL に対して `go test ./...`（`TASUKI_POSTGRES_DSN` 必須）で検証する。
