# M5 Shared Backend Opener Design

**Date:** 2026-07-25  
**Status:** Approved (autonomous)  
**Parent:** CLI follow-ups from contrib UI / bench

## Goal

One helper to open memory/postgres/sqlite/mysql/spanner/dynamodb/firestore from env + name, used by `tasuki-ui` and `cmd/bench`.

## API

Package `internal/backendopen`:

```go
type Options struct {
	Reset bool // bench=true; UI=false
}

func Open(ctx context.Context, name string, opts Options) (backend.Backend, func(), error)
```

Env vars unchanged (`TASUKI_*`). Migrate always for persistent stores. Close via returned func.

## Non-goals

- Chaos worker migration (different env style `TASUKI_BACKEND`)
- Changing env names

## Acceptance

- [x] UI + bench call `backendopen.Open`
- [x] Existing smoke tests still pass
