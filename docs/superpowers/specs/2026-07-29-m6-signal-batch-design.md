# M6 Signal Batch Design

**Date:** 2026-07-29  
**Status:** Approved  
**Parent:** Workflow Update 後の Client 利便性; 同一インスタンスへの複数シグナル一括投入  
**Decisions:** Same-instance only; atomic `SendToInboxBatch`; per-item optional dedupe (hit = skip/success); `SignalBatch` + `SignalItem` API.

## Goal

Send multiple signals to one running (or non-terminal) instance in a **single atomic store operation**, with the same per-item dedupe semantics as `Signal` + `WithDedupeID`.

## Non-goals

- Multi-instance fan-out
- Partial-success result reporting (which items were skipped)
- Batch Cancel / Terminate
- Changing single `Signal` external behavior (may be implemented via 1-item batch internally)

## API

```go
type SignalItem struct {
    Name     string
    Payload  any
    DedupeID string // optional; empty = no dedupe for that item
}

err := c.SignalBatch(ctx, instanceID, []tasuki.SignalItem{
    {Name: "approve", Payload: p1, DedupeID: "a-1"},
    {Name: "note", Payload: p2},
})
```

- Empty / nil slice → `nil` (no-op).
- Unknown instance → `backend.ErrNotFound`.
- Dedupe hit for an item → that item is skipped; other items still applied; overall return `nil`.
- Batch larger than the store budget → `backend.ErrBatchTooLarge` (no partial apply).

## Backend

```go
type InboxItem struct {
    Event    journal.Event
    DedupeID string
}

// SendToInboxBatch appends all items atomically (or store-equivalent).
// Dedupe hits skip that item without failing the batch.
SendToInboxBatch(ctx context.Context, instanceID string, items []InboxItem) error
```

Semantics (one transaction / TransactWrite / equivalent):

1. Lock / load instance (else `ErrNotFound`).
2. For each item in order: if `DedupeID != ""`, insert dedupe key (conflict → skip item); else insert inbox row.
3. If instance `running` and at least one row was inserted (or always ensure is fine), ensure workflow task singleton once.
4. Commit; notify once if a task may be claimable.

`SendToInbox` may delegate to `SendToInboxBatch` with a single item.

### Size limits

| Store | Cap (v1) |
|---|---|
| memory / sqlite / postgres / mysql / spanner | 100 items (constant) |
| DynamoDB / Firestore | Derive from `Capabilities.MaxAdvancementEffects` with headroom (document exact formula in plan); if `len(items)` exceeds → `ErrBatchTooLarge` before writes |

## Tests

- memory: two signals appear in inbox; one dedupe duplicate skipped; empty batch no-op; unknown id → ErrNotFound
- Optional: postgres/sqlite one atomicity smoke if cheap
- Client: `SignalBatch` marshals payloads via codec

## Docs

- `docs/03-api.md` SignalBatch
- README one-liner
- Spec acceptance checkboxes

## Acceptance

- [ ] `Backend.SendToInboxBatch` on all stores
- [ ] `Client.SignalBatch` + `SignalItem`
- [ ] Atomic apply + per-item dedupe skip
- [ ] `ErrBatchTooLarge` for oversized batches
- [ ] Tests + docs
