# M5 Activity Parallelism Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（獲得と追記のバッチ化の延長）  
**Decisions:** Parallelize **activities only** inside tick; `WorkerOptions.ActivityConcurrency` (default 1); Approach C = semaphore + bench flag.

## Goal

Run claimed activity tasks concurrently within a Worker tick, bounded by `ActivityConcurrency`, to improve throughput when many activities are ready. Default `1` preserves today’s sequential behavior.

## Non-goals

- Parallel workflow-task execution
- Persistent worker pool outside `tick`
- New lease-extension strategy
- Backend API changes
- CI throughput gates

## API

```go
type WorkerOptions struct {
	// ...
	ActivityConcurrency int // max concurrent handleActivity; <=0 → 1
}
```

`withDefaults`: if `ActivityConcurrency <= 0`, set to `1`.

## Worker.tick

1. `FireDueTimers` / `ClaimDueSchedules` (unchanged)
2. Claim workflow tasks → sequential `handleWorkflow` (unchanged)
3. Claim activity tasks → run with semaphore of size `ActivityConcurrency`:

```go
sem := make(chan struct{}, concurrency)
var wg sync.WaitGroup
for _, t := range atasks {
	wg.Add(1)
	go func(t backend.Task) {
		defer wg.Done()
		sem <- struct{}{}
		defer func() { <-sem }()
		w.track(t.ID)
		_ = w.handleActivity(ctx, t)
		w.untrack(t.ID)
	}(t)
}
wg.Wait()
```

Each goroutine tracks/untracks its own task ID for Shutdown lease release.

## Shutdown

Existing loop cancel + wait for tick to finish (`wg.Wait` inside tick) then `releaseInFlight`. No new Shutdown API.

## Bench

- `bench.Config.ActivityConcurrency`
- `cmd/bench -activity-concurrency` (0 → Worker default 1)

## Verification

- Default path: existing unit/bench tests green
- Manual: `-activity-concurrency=8 -claim-limit=50` on memory

## Docs

README: note `ActivityConcurrency` / `-activity-concurrency` (default 1).

## Acceptance

- [x] Option + default 1
- [x] Parallel activities in tick; workflows sequential
- [x] Bench flag
- [x] README
- [x] No Backend changes

## Follow-ups

- Parallel workflows (per-instance serialization)
- Background executor pool
