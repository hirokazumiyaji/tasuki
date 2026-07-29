# M5 PostgreSQL Task Notify Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（ストアの通知機構）, [docs/02-architecture.md](../../02-architecture.md)  
**Decisions:** Scope = Worker wake only on PostgreSQL; emit on task-become-visible writes; Approach A = optional `TaskNotifier` + Worker type assert; polling remains fallback.

## Goal

Reduce E2E latency caused by `PollInterval` waits on PostgreSQL by waking Workers via `LISTEN`/`NOTIFY` when tasks become claimable. Notifications are **hints only**; correctness remains in `ClaimTasks` / leases. Timers and due schedules still rely on the poll ticker.

## Non-goals

- `Client.Result` / GetInstance wake-on-complete
- Other stores (MySQL, DynamoDB Streams, Firestore listen, etc.)
- Mandatory DB triggers (application `pg_notify` is enough for this slice)
- Per-queue channels
- Removing `PollInterval` polling
- Numeric SLO gates in CI

## Optional capability

```go
package backend

// TaskNotifier is an optional Backend capability.
// Workers that detect it may wake early when tasks may be visible.
type TaskNotifier interface {
	// Subscribe delivers coalesced wake hints (buffer 1 is fine).
	// Cancelling ctx ends the subscription.
	// The returned error is only for subscribe/setup failure.
	Subscribe(ctx context.Context) (<-chan struct{}, error)
}
```

Do **not** add `Subscribe` to the required `Backend` interface. Only `backend/postgres.Backend` implements it in this slice.

## Channel

| Item | Value |
|---|---|
| Channel name | `tasuki_tasks` |
| Payload | empty / ignored |
| Coalesce | yes (drop duplicate wakes while one is pending) |

## Worker loop

```
ch := nil
if n, ok := backend.(TaskNotifier); ok {
  ch, err = n.Subscribe(ctx)  // on error: log and continue ticker-only
}
ticker := PollInterval
for {
  tick(ctx)  // FireDueTimers, ClaimDueSchedules, ClaimTasks — unchanged
  select {
  case <-ctx.Done(): return
  case <-ticker.C:
  case <-ch:  // only if ch != nil
  }
}
```

`FireDueTimers` / `ClaimDueSchedules` stay on the ticker path (future-visible retries and timer deadlines are not NOTIFY’d in this slice).

## When to NOTIFY (postgres)

Emit `pg_notify('tasuki_tasks', '')` after a write that can make a task newly claimable soon (same transaction preferred; post-commit also OK — duplicates preferred over misses):

| Operation | Notify? |
|---|---|
| `CreateInstance` WF task insert | yes |
| `CommitAdvancement` activity / child WF / ensure WF task | yes |
| `CompleteActivity` → inbox + ensure WF | yes |
| `SendToInbox` → ensure WF | yes |
| `FireDueTimers` → enqueue / ensure | yes |
| `ClaimDueSchedules` → started instance tasks | yes |
| `ReleaseLease` (immediate visibility) | yes |
| `RetryActivity` with future `visible_at` | **no** (ticker) |
| `ExtendLease` | no |

Centralize emit in a small helper (e.g. `notifyTasks(ctx)`) called from the sites above.

## LISTEN connection

- Dedicated pgx connection (not from the query pool) for `LISTEN tasuki_tasks`
- On disconnect: reconnect with backoff; until reconnected, Worker uses ticker only
- Close connection on `Subscribe` ctx cancel / Backend `Close`

## Verification

- Existing postgres conformance + chaos remain green
- New postgres test: after `Subscribe`, `CreateInstance` yields ≥1 wake within a short timeout
- Memory / non-notifier backends: Worker behavior unchanged (ticker only)
- Bench (manual): `-backend=postgres -poll=1s -instances=50 -workers=2` should show higher throughput with notify than without (compare via temporary disable or pre-change baseline). No hard CI threshold in this slice.

## Docs

- README: one sentence that postgres Workers use LISTEN/NOTIFY with `PollInterval` as fallback / timer cadence
- Spec is the source of truth; optional short note in architecture later

## Acceptance

- [x] `TaskNotifier` in `backend` package; postgres implements `Subscribe`
- [x] Notify on listed enqueue/ensure/release paths
- [x] Worker wakes on notify **or** ticker
- [x] postgres conform + chaos green
- [x] Notify subscribe unit/integration test
- [x] README note
- [x] No required API changes on other backends

## Follow-ups (out of scope)

- Client.Result notify
- Retry/timer deadline notify (or `pg_notify` when `visible_at` becomes due via trigger)
- Other store notifiers behind the same interface
- Bench flag to force-disable notify for A/B
