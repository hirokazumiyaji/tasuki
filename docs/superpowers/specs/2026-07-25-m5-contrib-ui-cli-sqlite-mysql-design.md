# M5 Contrib UI CLI sqlite + mysql Design

**Date:** 2026-07-25  
**Status:** Approved  
**Parent:** [2026-07-24-m5-contrib-ui-design.md](./2026-07-24-m5-contrib-ui-design.md)  
**Decisions:** Add sqlite and mysql to `tasuki-ui` CLI only (Approach A); Migrate on start; no Reset; env-based config matching existing postgres pattern.

## Goal

Let operators point `tasuki-ui` at **SQLite** or **MySQL** backends in addition to memory and postgres, without changing the HTTP handler.

## Non-goals

- Spanner / DynamoDB / Firestore in this slice
- Syncing `cmd/bench` backends
- Shared `openBackend` package extraction (optional follow-up)
- Auth / TLS
- Build tags / optional module splits

## Layout

| Path | Role |
|---|---|
| `contrib/ui/cmd/tasuki-ui/main.go` | Extend `-backend` and `openBackend` |
| `README.md` | Document env vars and examples |
| Optional: `contrib/ui/cmd/tasuki-ui/main_test.go` | Smoke: unknown backend error; sqlite tempfile open+Migrate |

## CLI

```text
-backend=memory|postgres|sqlite|mysql  (default memory)
-addr=:8080
```

| Backend | Config | On start |
|---|---|---|
| memory | — | — |
| postgres | `TASUKI_POSTGRES_DSN` | `Migrate` |
| sqlite | `TASUKI_SQLITE_PATH` (file path) | `Migrate` |
| mysql | `TASUKI_MYSQL_DSN` | `Migrate` |

Missing required env → exit 2 with clear error. Unknown `-backend` → exit 2. Always `Close` on shutdown via deferred closer. **Do not** call `Reset`.

## Data / API

Reuse existing `sqlite.New(path)`, `mysql.New(ctx, dsn)`, and their `Migrate`. Handler unchanged (`NewHandler(client)`).

## Verification

- Unit/smoke in `main_test.go` (or unexported test via `openBackend` in same package):
  - `openBackend(ctx, "nope")` errors
  - `openBackend(ctx, "sqlite")` with temp path + env set → Migrate OK, closer OK
- Manual: mysql when DSN available (optional; not required in CI without service)

## Docs

README 閲覧 UI section: add sqlite / mysql examples and env names.

## Acceptance

- [ ] `-backend=sqlite` with `TASUKI_SQLITE_PATH`
- [ ] `-backend=mysql` with `TASUKI_MYSQL_DSN`
- [ ] Migrate on start; no Reset
- [ ] README updated
- [ ] Smoke test for unknown backend + sqlite tempfile
- [ ] Handler / engine unchanged

## Follow-ups

- Remaining stores in CLI
- Extract shared backend opener for bench + UI + chaos
