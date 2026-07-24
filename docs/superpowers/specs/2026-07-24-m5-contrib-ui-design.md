# M5 Contrib Read-only Web UI Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（Web UI / contrib）, FR-15  
**Decisions:** Read-only list + detail; `contrib/ui` Handler + `cmd/tasuki-ui`; memory + postgres CLI; server-rendered HTML via embed; Approach C (Handler + CLI).

## Goal

Ship a minimal **contrib** browser UI to list workflow instances and view journal history, without adding dependencies to the engine core or write operations.

## Non-goals

- Signals, Terminate, Start, or any mutations
- Auth, TLS, SPA frameworks
- Live refresh / websockets
- CLI support for every M4 store (extend later)
- Separate `go.mod` for contrib (optional later)

## Layout

| Path | Role |
|---|---|
| `contrib/ui/handler.go` | `NewHandler(*tasuki.Client) http.Handler` |
| `contrib/ui/templates/` or `embed` HTML | List + detail pages |
| `contrib/ui/cmd/tasuki-ui/main.go` | Flags, backend open, ListenAndServe |

Module: remains `github.com/hirokazumiyaji/tasuki` (package `…/contrib/ui`).

## HTTP routes

| Method | Path | Behavior |
|---|---|---|
| GET | `/` | List via `Client.List` with `status`, `name` query params; simple HTML table linking to detail |
| GET | `/instances/{id}` | `Client.Get` + `GetJournal`; 404 if missing; show status, ids, journal events (seq, type, name, ref_seq, payload truncated) |

Static CSS embedded (minimal, light theme, no purple gradients).

## CLI

```text
-backend=memory|postgres  (default memory)
-addr=:8080
```

Postgres: require `TASUKI_POSTGRES_DSN`; `Migrate` on start; **do not** `Reset`.

## Data

Reuse existing Client APIs only (`List`, `Get`, `GetJournal`). No new Backend methods.

## Verification

- `httptest` smoke: list empty / with instances; detail 404 / OK
- Manual: `go run ./contrib/ui/cmd/tasuki-ui -backend=memory`

## Docs

README: short “閲覧 UI” section with run commands.

## Acceptance

- [ ] `NewHandler` + routes
- [ ] CLI memory + postgres
- [ ] Read-only
- [ ] README
- [ ] No engine API changes

## Follow-ups

- More backends in CLI
- Mutations (signal/terminate) behind confirm
- Separate contrib module
