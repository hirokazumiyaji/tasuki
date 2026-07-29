# M5 Task Backlog Metrics Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) risk「アプリ DB への負荷」; M5 Leftover A-Track Slice 1  
**Decisions:** Required `Backend.CountClaimableTasks` (per-queue map); Worker samples once per poll tick when Metrics set; Gauge `tasuki.tasks.backlog` with attrs `kind`, `queue`; count errors are non-fatal.

## Goal

Expose how many tasks are currently claimable (`visible_at <= now`) per queue so operators can see store pressure before the app DB saturates.

## Non-goals

- Cross-process notify / Streams
- Historical backlog time series beyond OTel exporter retention
- Counting leased-but-not-yet-visible tasks as backlog (they are not claimable)
- Hard alerts inside the library

## API

```go
// CountClaimableTasks returns per-queue counts of tasks with visible_at <= store now
// for kind among queues. Queues with zero may be omitted.
CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error)
```

Eligibility matches `ClaimTasks` filters: `kind`, `queue IN queues`, `visible_at <= now`. Empty `queues` → empty map, nil error.

## Worker

In `tick`, when `Metrics != nil`, for kinds `workflow` and `activity`:

1. Call `CountClaimableTasks(ctx, kind, w.opts.Queues)`
2. On error: Debug log; continue tick
3. On success: for each queue in `w.opts.Queues`, `RecordBacklog(ctx, kind, queue, n)` (`n` is 0 if omitted from map)

## Metrics

| Name | Type | Attributes |
|---|---|---|
| `tasuki.tasks.backlog` | Int64Gauge | `kind`, `queue` |

## Stores

Implement on memory, sqlite, mysql, postgres, spanner, dynamodb, firestore.

## Verification

- `backendtest` case: create instance → count workflow on `default` ≥ 1 → claim → count decreases (or 0)
- Worker: with Metrics set, tick after CreateInstance records without failing claim path
- Docs: `docs/05-observability.md`, risk row update

## Acceptance

- [x] Interface + all seven backends
- [x] backendtest coverage
- [x] Worker sampling + gauge helper
- [x] Observability docs
