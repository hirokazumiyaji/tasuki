# M6 Signal Batch Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Atomic same-instance multi-signal send via `Client.SignalBatch` and `Backend.SendToInboxBatch`.

**Architecture:** New Backend method applies N inbox inserts (+ optional dedupe keys) in one transaction; ensure WF task once; Client marshals `[]SignalItem`.

**Spec:** [docs/superpowers/specs/2026-07-29-m6-signal-batch-design.md](../specs/2026-07-29-m6-signal-batch-design.md)

### Task 1: Spec + Plan
### Task 2: Backend interface + memory + Client.SignalBatch + unit/integration tests
### Task 3: Implement `SendToInboxBatch` on remaining backends (postgres/sqlite/mysql/spanner/dynamodb/firestore); `SendToInbox` delegates
### Task 4: Docs

## Notes

### Types (`backend/types.go` or `backend.go`)

```go
var ErrBatchTooLarge = errors.New("batch too large")

type InboxItem struct {
    Event    journal.Event
    DedupeID string
}

const DefaultInboxBatchLimit = 100
```

DynamoDB/Firestore: `limit := caps.MaxAdvancementEffects / 4` (dedupe+inbox per item + ensure/CAS headroom); min 1.

### Client

```go
func (c *Client) SignalBatch(ctx context.Context, id string, items []SignalItem) error
```

Marshal each payload with codec into `journal.TypeSignalReceived` events.

### Memory

Extend lock scope of current `SendToInbox` to loop items; single ensure + notify.

### SQL backends

One `BEGIN`; `SELECT … FOR UPDATE` (or equivalent); per-item dedupe insert + inbox insert; one ensure; `COMMIT`; notify.

### Tests

- `backend/memory` or root `client_signal_batch_test.go`
- backendtest helper optional if pattern exists for signal dedupe

### Docs

`docs/03-api.md`, README, acceptance boxes
