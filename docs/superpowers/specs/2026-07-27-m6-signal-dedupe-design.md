# M6 Signal Dedupe ID Design

**Date:** 2026-07-27  
**Status:** Proposed  
**Parent:** [docs/04-plan.md](../../04-plan.md) 将来候補「シグナルの重複排除 ID」  
**Decisions:** Instance-scoped keys `(instance_id, dedupe_id)`; optional `WithDedupeID`; duplicate → `nil` (idempotent); separate `wf_signal_dedupe` table; keys removed when the instance becomes terminal; Cancel / Terminate out of scope.

## Goal

Allow clients to retry `Signal` safely by attaching a dedupe ID so the same logical signal is delivered at most once per workflow instance.

## Non-goals

- Global (store-wide) dedupe IDs
- TTL / time-based expiry independent of instance lifecycle
- Dedupe for Cancel or Terminate
- Recording dedupe IDs in the journal for replay matching
- Search attributes

## API

```go
err := c.Signal(ctx, instanceID, "approve", payload, tasuki.WithDedupeID("pay-intent-42"))
```

- `WithDedupeID` omitted or empty → current behavior (duplicates are delivered).
- Non-empty dedupe ID → at most one inbox insertion per `(instanceID, dedupeID)`.
- A second `Signal` with the same pair returns `nil` and does not grow the inbox (whether or not the first copy has been drained into the journal).

## Backend contract

Extend inbox send so optional dedupe can be applied atomically with the insert. Preferred shape (names flexible):

```go
SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error
```

Empty `dedupeID` preserves today’s path. Non-empty path:

1. Ensure the instance exists (else `backend.ErrNotFound`).
2. Insert into `wf_signal_dedupe` with PK `(instance_id, dedupe_id)`.
3. On unique conflict → commit/return `nil` without inserting inbox (and without an extra wakeup).
4. On success → insert inbox event + ensure workflow task / notify as today.

`journal.Event` does **not** carry the dedupe ID into history. The key exists only in `wf_signal_dedupe`.

## Schema

Logical table / collection `wf_signal_dedupe`:

| Column | Notes |
|---|---|
| `instance_id` | PK part 1, FK-like to `wf_instances.id` where the store supports it |
| `dedupe_id` | PK part 2, non-empty text |
| `created_at` | optional audit |

Apply across all backends (memory, postgres, sqlite, mysql, spanner, dynamodb, firestore) via existing migrate paths.

## Lifecycle

- Keys live for the instance’s active lifetime.
- When the instance becomes terminal (`completed`, `failed`, `terminated`, `canceled`, `continued`), delete that instance’s rows from `wf_signal_dedupe` in the same advancement / terminate path that updates status (or CASCADE where FK is available).
- After terminal cleanup, a further Signal with the same dedupe ID is a new insert if the store still accepts inbox for that instance (same as today for post-terminal Signal); this is acceptable and rare.

## Semantics

| Case | Result |
|---|---|
| First Signal with dedupe ID | One inbox item; dedupe row created |
| Retry same `(instanceID, dedupeID)` | `nil`; inbox unchanged |
| Different dedupe ID or no option | Normal append |
| Unknown instance | `backend.ErrNotFound` |
| Signal without dedupe (legacy) | Unchanged; duplicates delivered |

Architecture doc row for signals updates from “inbox は重複排除しない” to “optional dedupe ID → at-most-once per instance; default remains at-least-once delivery of each send”.

## Tests

- `backendtest`: with dedupe, two `SendToInbox` → one inbox row; without dedupe, two rows
- Worker/client: workflow `ReceiveSignal` once despite client retry with same `WithDedupeID`
- Terminal instance clears dedupe rows (store-level assertion)
- All seven backends via shared suite where practical (memory + SQL pattern; cloud stores follow migrate + suite hooks)

## Docs

- `docs/03-api.md` (Client `Signal` + `WithDedupeID`)
- `docs/02-architecture.md` reliability table
- `docs/04-plan.md` future list
- README one-liner if space fits

## Acceptance

- [ ] `WithDedupeID` on `Client.Signal`
- [ ] `wf_signal_dedupe` + `SendToInbox` dedupe path on all backends
- [ ] Terminal cleanup
- [ ] Tests
- [ ] Docs
