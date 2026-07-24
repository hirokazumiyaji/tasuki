# M5 Claim Batch Size Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（獲得と追記のバッチ化）  
**Decisions:** Scope = claim batch size only (sequential execution); Approach B = `WorkerOptions.ClaimLimit` + `cmd/bench -claim-limit`.

## Goal

Allow Workers to claim more tasks per poll tick via a configurable limit, reducing claim round-trips. Execution remains **sequential** within a tick. No Backend API changes (`ClaimRequest.Limit` already exists).

## Non-goals

- Parallel task execution inside the Worker
- Batching multiple `CommitAdvancement` calls into one transaction
- Changing the required `Backend` interface
- Separate workflow vs activity limits
- CI numeric throughput gates

## API

```go
type WorkerOptions struct {
	// existing fields...
	ClaimLimit int // max tasks per ClaimTasks call; <=0 means default 10
}
```

`withDefaults`: if `ClaimLimit <= 0`, set to `10` (preserves today’s hardcoded behavior).

## Worker

In `tick`, replace hardcoded `Limit: 10` for both workflow and activity claims with `w.opts.ClaimLimit`.

Processing loops stay sequential (`handleWorkflow` / `handleActivity` one at a time). Lease tracking unchanged.

## Bench

- `bench.Config.ClaimLimit int` passed into `WorkerOptions.ClaimLimit`
- `cmd/bench -claim-limit` (default `0` → Worker default 10)

## Verification

- Existing unit / bench / memory tests remain green with default limit
- Optional small test or assert that `ClaimLimit` flows into claim requests (fake backend or existing worker test with Limit=1)
- Manual: `go run ./cmd/bench -backend=memory -claim-limit=50 -instances=200`

## Docs

README: note `-claim-limit` / `WorkerOptions.ClaimLimit` (default 10).

## Acceptance

- [ ] `ClaimLimit` on `WorkerOptions` with default 10
- [ ] Worker tick uses it for workflow and activity claims
- [ ] Bench flag `-claim-limit`
- [ ] README one-liner
- [ ] No parallel execution; no Backend interface change

## Follow-ups

- Parallel executor pool
- Commit batching
- Sticky journal cache
