# PostgreSQL Migrations

English | [日本語](README.ja.md)

tasuki's database schema is managed using versioned migration files.

```
migrations/
├── 000001_init.up.sql            Base tables and indexes
├── 000001_init.down.sql          DROP all tables
├── 000002_vacuum_tuning.up.sql   autovacuum / fillfactor tuning
├── 000002_vacuum_tuning.down.sql Reset tuning settings
├── 000003_purge_search_indexes.up.sql   purge (completed_at) + search_attributes GIN indexes
└── 000003_purge_search_indexes.down.sql Drop those indexes
```

The naming convention is `<6-digit-version>_<name>.up.sql` / `.down.sql`. Lexicographical file name order determines application order.

## `Migrate()` Behavior

`postgres.Backend.Migrate(ctx)` performs the following steps:

1. Creates the tracking table `tasuki_schema_migrations`.
2. Applies unapplied migrations in version order within a transaction, and inserts a tracking record in the same transaction.
3. Skips already applied versions. The call is idempotent and safe to invoke concurrently across multiple processes (atomic claim via primary key on version).

Databases previously provisioned with the legacy cumulative `schema.sql` are detected by the presence of `wf_instances`, recorded as version 1, and only subsequent diffs are applied.

Auxiliary APIs:

| API | Purpose |
|---|---|
| `postgres.LatestSchemaVersion()` | Returns the latest schema version embedded in this build |
| `b.SchemaVersion(ctx)` | Returns the maximum applied version (0 if not migrated) |
| `b.ValidateSchema(ctx)` | Verifies that required tables exist (failsafe against unmigrated databases) |

Workers automatically invoke `ValidateSchema` on startup (can be disabled via `WorkerOptions.DisableSchemaValidation`). If required tables are missing, the worker logs an error and avoids starting the polling loop.

## Using with External Tools

Because migration files are plain SQL, they can be used directly with existing migration tools. If you prefer to disable automatic runtime migrations (e.g. for privilege separation or zero-downtime deployment pipelines), apply migrations via your CD tool and insert the corresponding row into `tasuki_schema_migrations` in the same transaction. `Migrate()` will then act as an idempotent no-op.

### golang-migrate

```bash
migrate -path backend/postgres/migrations \
  -database 'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable' up
```

`golang-migrate` manages versions in its own `schema_migrations` table. When using both `golang-migrate` and tasuki's `Migrate()`, designate one tool as the single source of truth to avoid mismatched version state.

### goose

```bash
goose -dir backend/postgres/migrations postgres \
  'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable' up
```

Files without annotations (`-- +goose Up`) are treated as Up in their entirety. For Down operations, running the down SQL file directly or adding goose annotations is recommended.

### Atlas

```bash
atlas migrate apply --dir file://backend/postgres/migrations \
  --url 'postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
```

## Adding New Migrations

1. Add `000004_<name>.up.sql` / `.down.sql`. Versions must always be monotonically incrementing.
2. `.up.sql` does not strictly need to be idempotent (it runs only once and rolls back on failure in a transaction), but using idempotent DDL like `CREATE TABLE IF NOT EXISTS` helps bridge differences with databases migrated from older setups.
3. Validate against a real PostgreSQL instance with `go test ./...` (requires `TASUKI_POSTGRES_DSN`).
