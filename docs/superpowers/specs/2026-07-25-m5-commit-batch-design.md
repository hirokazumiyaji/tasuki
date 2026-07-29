# M5 Commit Batching Design

**Date:** 2026-07-25  
**Status:** Approved (autonomous continuation)  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（獲得と追記のバッチ化）, [2026-07-24-m5-claim-limit-design.md](./2026-07-24-m5-claim-limit-design.md) follow-up  
**Decisions:** Optional `backend.AdvancementBatcher`; worker defers workflow commits then flushes once per claim wave; memory + postgres real batch first; others fall back to loop; Approach = collect-then-flush.

## Goal

Reduce per-tick `CommitAdvancement` round-trips by committing multiple workflow advancements in one backend call when supported.

## Non-goals

- Batching activity completions into CommitAdvancement
- Changing fencing / next_seq semantics per instance
- DynamoDB TransactWrite multi-instance mega-tx (use fallback loop; DDB 100-item limit)
- Changing ClaimLimit / concurrency defaults

## API

```go
// Optional. If absent, workers loop CommitAdvancement.
type AdvancementBatcher interface {
	CommitAdvancements(ctx context.Context, advs []Advancement) error
}
```

Semantics:

- Empty slice → nil
- Each advancement keeps its own `ExpectedNextSeq` CAS and effects
- **All-or-nothing** for implementers that use one transaction (memory, postgres)
- On conflict / error, no partial apply (txn rollback)
- Fallback loop (non-batcher): stop at first error; prior commits in the loop remain (document this difference) — prefer implementing batcher on all SQL stores in follow-ups

## Worker

After claiming workflow tasks for a tick:

1. Run handlers (honour `WorkflowConcurrency` + instance locks) so each produces an `Advancement` (or error) **without** calling `CommitAdvancement`
2. `WaitGroup` for the wave
3. Collect successful advancements; if any handler error, still attempt commit of successes? **No** — on any handler failure, do not batch-commit that instance; release/skip as today. Successes from the same wave **are** committed (partial wave OK at worker level)
4. Flush: if `AdvancementBatcher`, `CommitAdvancements(ctx, advs)`; else loop `CommitAdvancement`
5. Update sticky cache per successful commit as today
6. Then activity phase (unchanged)

Refactor `handleWorkflow` into prepare + commit sticky update, or return `(adv, err)`.

No new `WorkerOptions` flag: always collect-then-flush for workflows (behavior change is internal; single-advancement batch of len=1 must match today’s commit). Activity path unchanged.

## Backends (this slice)

| Backend | Behavior |
|---|---|
| memory | One lock; apply all advs sequentially inside lock; rollback via… memory has no txn — apply all or on error leave? **Use apply-all with stop-on-error and document**, OR snapshot-restore. Prefer: apply under lock; on error return without applying remaining; already-applied stay (same as loop). Better: apply to private copy then swap — too heavy. **Match loop semantics for memory first:** for-loop CommitAdvancement inside existing method as `CommitAdvancements` helper. Real multi-apply optimization = postgres only this slice. |
| postgres | Single SQL transaction; each adv as today; rollback all on any failure |
| others | `CommitAdvancements` not implemented → worker fallback loop |

Actually for a clean optional interface: only postgres (+ maybe sqlite/mysql later) implement it. Memory can implement as loop under one mutex for less churn (optional).

## Verification

- Unit: memory worker with 2 workflow tasks in one claim → both committed (existing e2e)
- Postgres: test `CommitAdvancements` two instances in one txn; second conflicts → neither? or first-only depending on implementation — **all-or-nothing**
- Conform: add optional test if backend implements AdvancementBatcher
- Bench: no new flags required; throughput may improve with high ClaimLimit

## Docs

README / plan note: workflow commits may be batched per tick via `AdvancementBatcher`.

## Acceptance

- [x] `AdvancementBatcher` interface
- [x] Worker collect-then-flush for workflow wave
- [x] Postgres transactional `CommitAdvancements`
- [x] Tests (postgres batch + worker still green)
- [x] Fallback for non-batching backends

## Follow-ups

- sqlite/mysql/spanner batchers
- True all-or-nothing memory
- Activity-side batching
