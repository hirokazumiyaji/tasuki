# M6 Activity StartToClose Timeout Design

**Date:** 2026-07-29  
**Status:** Approved  
**Parent:** [docs/03-api.md](../../03-api.md) リトライ節「MaxAttempts やタイムアウトを与える」; Local Activity 後の運用ギャップ  
**Decisions:** `WithStartToCloseTimeout` on `Execute` / `ExecuteAsync`; timeout is a normal failure subject to existing retry rules; enforce via Worker `context.WithTimeout`; persist timeout in schedule + activity task JSON payload (no schema migration).

## Goal

Allow callers to bound a single activity **attempt** from start to completion. When the deadline is exceeded, the attempt fails like any other error and follows `RetryPolicy` / `MaxAttempts` / `NonRetryable`.

## Non-goals

- ScheduleToCloseTimeout (across retries)
- HeartbeatTimeout
- StartToClose for `ExecuteLocal`
- Dedicated `errors.Is` timeout sentinel (v1 treats as ordinary failure string)
- Changing lease / heartbeat semantics beyond racing with the attempt deadline

## API

```go
out, err := workflow.Execute[In, Out](ctx, "Charge", in,
    workflow.WithStartToCloseTimeout(30*time.Second),
    workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 3}),
)
```

- `WithStartToCloseTimeout(d time.Duration)` is an `ExecuteOption`.
- Same option applies to `ExecuteAsync`.
- `d <= 0` means unset (no StartToClose limit; current behavior).
- Unset remains the default so existing workflows are unchanged.

## Persistence

### Journal (`activity_scheduled` payload)

Extend `workflow.ActivitySchedule`:

```go
type ActivitySchedule struct {
    Input                   json.RawMessage  `json:"input"`
    Retry                   *RetryPolicyJSON `json:"retry,omitempty"`
    StartToCloseTimeoutMs   int64            `json:"start_to_close_timeout_ms,omitempty"`
}
```

### Activity task payload JSON

Existing backends store name / input / retry inside the task `payload` JSON (no column for retry intervals). Add the same `start_to_close_timeout_ms` field there.

- `backend.NewTask` / `backend.Task` gain `StartToCloseTimeout time.Duration` (0 = unset).
- Worker `attachEffects` copies from `ActivitySchedule` onto `NewTask`.
- Claim paths that already unmarshal retry JSON also restore `StartToCloseTimeout`.

**No DDL / schema migration.**

## Worker

In `handleActivity`:

1. If `t.StartToCloseTimeout > 0`, wrap the activity context with `context.WithTimeout(..., t.StartToCloseTimeout)`.
2. Run the registered activity as today (heartbeat env, lease extend loop unchanged).
3. On return:
   - If `ctx.Err() == context.DeadlineExceeded` (or `errors.Is(err, context.DeadlineExceeded)`), treat as failure with message `"activity start-to-close timeout"` (stable string for logs / journal).
   - Otherwise existing error / success paths.
4. Failure still goes through: NonRetryable → fail permanently; else RetryActivity until MaxAttempts; else fail permanently.

Activities that ignore context may keep running after the deadline (same contract as any canceled `context.Context`). The Worker still completes or retries the **task** based on the timeout.

## Tests

- Unit: `WithStartToCloseTimeout` appears on new `activity_scheduled` command payload; replay matches.
- Integration (memory Worker): activity that sleeps longer than timeout fails the attempt; with `MaxAttempts: 1` workflow sees failure; with retries, attempts > 1 possible.
- Unset timeout: long activity still completes when lease is extended (existing heartbeat / extend loop).

## Docs

- `docs/03-api.md`: document `WithStartToCloseTimeout`; tighten the retry paragraph that already mentions timeouts.
- README one-liner optional.
- `docs/04-plan.md`: note under implemented M6 items if useful.

## Acceptance

- [x] `WithStartToCloseTimeout` on Execute / ExecuteAsync
- [x] Timeout persisted in schedule + task JSON; restored on Claim
- [x] Worker enforces via `context.WithTimeout`; timeout → retryable failure path
- [x] Tests + docs
