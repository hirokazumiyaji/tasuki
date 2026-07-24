# M5 Workflow Parallelism Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5; follow-up to activity parallelism  
**Decisions:** Approach A — `WorkflowConcurrency` (default 1), tick-local semaphore, per-`instance_id` mutex; bench flag; workflows complete before activities in the same tick.

## Goal

Run claimed **workflow** tasks concurrently within a Worker tick, bounded by `WorkflowConcurrency`. Serialize the same `instance_id` with a per-Worker mutex. Default `1` preserves sequential behavior.

## Non-goals

- Cross-process instance locks
- Redesigning sticky cache locking
- Interleaving workflow and activity pools in one wait group
- Backend API changes
- CI throughput gates

## API

```go
type WorkerOptions struct {
	// ...
	WorkflowConcurrency int // max concurrent handleWorkflow; <=0 → 1
}
```

`withDefaults`: if `WorkflowConcurrency <= 0`, set to `1`.

## Instance mutex

Worker holds `map[string]*sync.Mutex` (guarded by a small meta-mutex, or `sync.Map` of `*sync.Mutex`).

Each workflow goroutine:
1. Acquire instance mutex for `t.InstanceID`
2. `track` → `handleWorkflow` → `untrack`
3. Release instance mutex

## Worker.tick

1. FireDueTimers / ClaimDueSchedules (unchanged)
2. Claim workflow tasks → semaphore of size `WorkflowConcurrency` + per-instance lock + `handleWorkflow`; `WaitGroup.Wait`
3. Claim activity tasks → existing `ActivityConcurrency` path

Do **not** start activities until all workflow handlers from this claim batch have finished (keeps prior tick ordering).

## Shutdown

Unchanged: cancel loop, wait for tick (including workflow/activity WGs), `releaseInFlight`.

## Bench

- `bench.Config.WorkflowConcurrency`
- `cmd/bench -workflow-concurrency` (0 → Worker default 1)

## Verification

- Default concurrency 1: existing tests green
- Manual: `-workflow-concurrency=8 -activity-concurrency=8 -claim-limit=50`

## Docs

README: note `WorkflowConcurrency` / `-workflow-concurrency` (default 1); same-instance serialized in-process.

## Acceptance

- [ ] Option + default 1
- [ ] Parallel workflows with per-instance mutex
- [ ] Activities still after workflows in tick
- [ ] Bench flag + README
- [ ] No Backend changes

## Follow-ups

- Evict idle instance mutexes
- Optional interleaved WF/activity pools
