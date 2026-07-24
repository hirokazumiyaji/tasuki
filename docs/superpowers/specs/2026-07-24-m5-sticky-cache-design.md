# M5 Sticky Journal Cache Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（スティッキーキャッシュ）, [docs/02-architecture.md](../../02-architecture.md) §履歴の肥大  
**Decisions:** Per-Worker cache; invalidate/compare via `next_seq`; new required `LoadWorkflowHead` (no journal) + existing `GetJournal(afterSeq)`; default on.

## Goal

When the same Worker repeatedly runs the same instance, avoid re-reading the full journal from the store. Assemble replay input from a sticky journal cache plus a fresh **head** (instance metadata, inbox, authoritative `Now`).

Replay still starts from the beginning of the (cached) event list; only **I/O** is reduced.

## Non-goals

- Cross-process or multi-Worker shared cache
- LRU / max entry limits (follow-up)
- Skipping deterministic replay itself
- Removing `LoadWorkflow` (kept for fallback and simpler callers)

## Backend API

Add to required `Backend`:

```go
// LoadWorkflowHead returns instance metadata, inbox, next_seq, and store Now.
// Journal must be empty / unused; callers fill journal via cache or GetJournal.
LoadWorkflowHead(ctx context.Context, instanceID string) (*WorkflowState, error)
```

Semantics:

- Same NotFound behavior as `LoadWorkflow` / `GetInstance`
- `WorkflowState.Journal` is nil or empty
- `Inbox`, `NextSeq`, `Instance`, `Now` match what `LoadWorkflow` would return for those fields

Implement on: memory, postgres, sqlite, mysql, spanner, dynamodb, firestore.

`GetJournal(ctx, id, afterSeq)` already exists (events with `seq > afterSeq`).

Optional refactor: `LoadWorkflow` may call `LoadWorkflowHead` + `GetJournal(id, 0)` internally (DRY), but not required.

## Worker sticky cache

```go
type stickyEntry struct {
	journal []journal.Event // contiguous, ascending seq
	nextSeq int64           // instance next_seq at cache time
}
```

Map `instanceID → stickyEntry` guarded by Worker mutex. Default enabled (no opt-out flag in this slice).

### Load path (`handleWorkflow`)

1. `head, err := LoadWorkflowHead(ctx, id)`
2. If status ≠ `running`: drop cache entry; return (same as today)
3. Resolve journal:
   - **No cache:** `GetJournal(id, 0)` (or `LoadWorkflow` once); store entry `{journal, nextSeq: head.NextSeq}`
   - **Cache hit, `head.NextSeq == entry.nextSeq`:** use `entry.journal`
   - **Cache hit, `head.NextSeq > entry.nextSeq`:** `extra := GetJournal(id, lastSeq)` where `lastSeq` is last event seq (0 if empty); append; if resulting next expected seq ≠ `head.NextSeq`, **invalidate and full reload** (`GetJournal(id,0)`)
   - **`head.NextSeq < entry.nextSeq`:** invalidate; full reload
4. Build `WorkflowState{Instance, Journal, Inbox: head.Inbox, NextSeq, Now}` and continue existing engine path

### Update / invalidate

| Event | Action |
|---|---|
| Successful `CommitAdvancement` | Set cache to the journal used for this run **plus** committed new events (or re-read head+journal); `nextSeq` = post-commit next |
| `ErrConflict` | Delete entry for id |
| Terminal status after commit | Delete entry |
| Shutdown | Clear map |

Practical update after success: start from the in-memory `events` slice used for `engine.RunAt` (history + ingested inbox events that were assigned seqs) plus any `res.NewCommands` that were committed; set `nextSeq` accordingly. If that is error-prone, post-commit `GetJournal(id,0)` once to refresh (still wins on subsequent loads). Prefer precise update from committed advancement when straightforward.

## Correctness

- Inbox always from store (`LoadWorkflowHead`) so signals/completions are not stale
- `next_seq` fence detects another Worker’s commit or lost lease races
- Full reload on any gap / inconsistency
- Conformance suite must still pass; add a small test that `LoadWorkflowHead` returns empty journal and matching inbox/next_seq vs `LoadWorkflow`

## Docs

- README: sticky journal cache on Worker (per-process, next_seq validated)
- Optionally one sentence in architecture “履歴の肥大” pointing to M5 done for sticky I/O

## Acceptance

- [ ] `LoadWorkflowHead` on Backend + all store implementations
- [ ] Worker sticky load/update/invalidate as above
- [ ] Conform (all backends that CI/local can run) green
- [ ] README note
- [ ] No shared cache / LRU / replay skip

## Follow-ups

- LRU / max sticky entries
- Metrics: sticky hit/miss / full reload
- Opt-out `WorkerOptions.StickyCache`
