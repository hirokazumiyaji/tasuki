# M6 Activity StartToClose Timeout Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** Add `workflow.WithStartToCloseTimeout` so each activity attempt is bounded; timeout fails the attempt and follows existing retry rules.

**Architecture:** Persist `start_to_close_timeout_ms` on `activity_scheduled` and activity task JSON payload. Worker wraps activity ctx with `context.WithTimeout`. No schema migration.

**Spec:** [docs/superpowers/specs/2026-07-29-m6-activity-start-to-close-timeout-design.md](../specs/2026-07-29-m6-activity-start-to-close-timeout-design.md)

### Task 1: Spec + Plan
### Task 2: API + schedule payload + unit tests
### Task 3: Backend Task field + payload JSON (all stores) + Worker timeout + integration tests
### Task 4: Docs

## Notes

### API (`workflow/retry.go` or `workflow/execute.go`)

```go
func WithStartToCloseTimeout(d time.Duration) ExecuteOption {
    return func(o *executeOptions) { o.startToClose = d }
}
```

`executeOptions` gains `startToClose time.Duration`.

`Execute` / `ExecuteAsync` set `ActivitySchedule.StartToCloseTimeoutMs` when `> 0`.

### Schedule / Task

```go
// workflow.ActivitySchedule
StartToCloseTimeoutMs int64 `json:"start_to_close_timeout_ms,omitempty"`

// backend.NewTask / backend.Task
StartToCloseTimeout time.Duration
```

Activity payload JSON structs (postgres/sqlite/mysql/spanner/dynamodb/firestore/memory as applicable) gain:

```go
StartToCloseTimeoutMs int64 `json:"start_to_close_timeout_ms,omitempty"`
```

Wire in: marshal on enqueue (`CommitAdvancement` / insert activity task), unmarshal on Claim / task decode.

`worker.attachEffects`: copy from schedule onto `NewTask.StartToCloseTimeout`.

### Worker (`handleActivity`)

```go
actCtx := ctx
var cancel context.CancelFunc
if t.StartToCloseTimeout > 0 {
    actCtx, cancel = context.WithTimeout(ctx, t.StartToCloseTimeout)
    defer cancel()
}
// WithEnv on actCtx; run act.fn
// if errors.Is(err, context.DeadlineExceeded) || actCtx.Err() == context.DeadlineExceeded:
//   err = fmt.Errorf("activity start-to-close timeout")
```

Prefer detecting deadline even when activity returns a wrapped error or nil-with-canceled-ctx.

### Tests

- `workflow/execute_timeout_test.go` (or extend existing): option lands in NewCommands payload
- `worker_activity_timeout_test.go`: sleep > timeout → fail with MaxAttempts 1; optional retry count

### Docs

- `docs/03-api.md` ExecuteOption table / retry section
- `docs/04-plan.md` M6 note if present
- Spec acceptance checkboxes
