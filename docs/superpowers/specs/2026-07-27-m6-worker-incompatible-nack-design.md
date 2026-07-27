# M6 Worker Incompatible-Task Nack Design

**Date:** 2026-07-27  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) 将来候補「ワーカーのバージョン管理」  
**Decisions:** Runtime detection after Claim (no build-ID routing); treat determinism violations and unregistered workflow/activity as incompatible; Nack with configurable delay (default short); no Backend schema beyond a kind-agnostic reschedule API.

## Goal

During rolling deploys, an older Worker must not mark an instance terminal `stuck` (or fail an activity) when it cannot execute the current history or registry. It should return the task so a newer Worker can make progress.

## Non-goals

- Build-ID / deployment-based Claim routing
- Automatic repair of determinism bugs
- Changing `GetVersion` semantics
- Sticky-cache version affinity beyond existing sticky journal cache

## Behavior

After claiming a task, the Worker classifies these outcomes as **incompatible** (not a durable failure of the workflow):

| Case | Today | After |
|---|---|---|
| Workflow name not in this Worker’s registry | Error; task held until lease expiry | Nack |
| Replay determinism violation (`engine` stuck + determinism) | Terminal status `stuck` | Nack |
| Activity name not registered | Activity failed event | Nack |

Other panics / non-determinism business failures remain unchanged (still stuck / fail as today where applicable).

Nack means: clear the lease and set `visible_at` to `now + IncompatibleRetryDelay` so another Worker can claim later. No journal / inbox mutation.

## Options

```go
WorkerOptions{
    IncompatibleRetryDelay time.Duration
}
```

In `withDefaults`:

- If `IncompatibleRetryDelay == 0` (unset), use `5 * time.Second`.
- If `IncompatibleRetryDelay < 0`, treat as immediate (`visible_at = now`).

## Backend

Add a kind-agnostic reschedule used by both workflow and activity nacks:

```go
NackTask(ctx context.Context, t Task, visibleAt time.Time) error
```

Semantics: task exists → set `visible_at = visibleAt`, clear `worker_id` (same effect as today’s activity retry / lease release, for any kind). Missing task → `ErrNotFound`.  
`Task` (not only `taskID`) is required so stores with instance-keyed workflow task PKs (DynamoDB, Firestore) can update the correct row.

`RetryActivity` may delegate to `NackTask` after checking `kind == activity` (optional refactor).

No new tables.

## Worker paths

1. **Workflow unregistered** (`registry.workflow` error): `NackTask`, Warn log, metric; return no pending commit.
2. **`res.Stuck` and error is determinism** (engine stuck from determinism panic): Nack instead of terminal `stuck` Advancement.
3. **Activity unregistered**: Nack instead of `failActivity`.

Log fields: `instance_id`, `task_id`, `reason` (`unregistered_workflow` / `determinism` / `unregistered_activity`).
Metric: counter for incompatible nacks (optional attribute by reason).

## Semantics note

True application determinism bugs are also Nack’d while mixed versions exist. After the fleet converges, repeated nacks without progress indicate a code defect—operators should inspect logs/metrics. Document this trade-off.

## Tests

- Memory Worker: old registry missing workflow → instance stays `running`; second Worker with registration completes.
- Determinism mismatch path Nacks (no `stuck` status).
- Unregistered activity Nacks (no `activity_failed`).
- Default delay sets future `visible_at`; negative option → immediate.

## Docs

- `docs/03-api.md` GetVersion / rolling deploy paragraph
- `docs/04-plan.md` future list (worker versioning → done or narrowed)
- README one-liner if space fits

## Acceptance

- [ ] `NackTask` on all backends
- [ ] Worker incompatible paths (WF / determinism / activity)
- [ ] `IncompatibleRetryDelay` defaults
- [ ] Tests
- [ ] Docs
