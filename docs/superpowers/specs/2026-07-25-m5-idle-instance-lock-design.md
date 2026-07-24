# M5 Idle Instance Mutex Eviction Design

**Date:** 2026-07-25  
**Status:** Approved (autonomous)  
**Parent:** [2026-07-24-m5-workflow-parallel-design.md](./2026-07-24-m5-workflow-parallel-design.md)

## Goal

Prevent unbounded growth of `Worker.instLock` for long-lived workers handling many distinct instance IDs.

## Approach

Track last-used time per instance mutex. After each workflow wave (or on a tick), evict entries unused for `IdleInstanceLockTTL` (default 10m) that are not currently locked.

Implementation sketch:

- `instLock` map value becomes `struct { mu sync.Mutex; lastUsed atomic/time }`
- `instanceMutex` updates `lastUsed`
- `evictIdleInstanceLocks(now)` called at end of workflow phase in `tick`
- Only delete if `TryLock` succeeds (not held) and idle past TTL; unlock immediately after delete decision

No new exported option required if default 10m is fine; optional `WorkerOptions.IdleInstanceLockTTL` with `<=0` meaning default 10m, and a sentinel or very large value to disable — use `<=0` → default; no disable needed for v1.

## Acceptance

- [ ] Eviction helper + call from tick
- [ ] Unit test: create many locks, advance clock/fake now, evict unused
