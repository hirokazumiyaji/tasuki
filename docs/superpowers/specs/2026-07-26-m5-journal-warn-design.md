# M5 Journal Size Warning + ContinueAsNew Guidance Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5 リスク「ジャーナルの肥大」; [docs/02-architecture.md](../../02-architecture.md) §履歴の肥大への対策  
**Decisions:** Warn-only (no hard fail); check in Worker after load; once per workflow task over threshold; `0` → default 10000, `<0` → disabled; document ContinueAsNew operational guidance.

## Goal

Make long-lived journal growth visible via worker logs and metrics, and document when to use existing `workflow.ContinueAsNew` to truncate history. Execution must not stop solely because the threshold is exceeded.

## Non-goals

- Changing ContinueAsNew API, ID strategy (`{id}~{seq}`), or terminal status (`continued`)
- Hard upper bound / stuck / fail on oversized journals
- Persistent “warn once per instance” state across workers
- Journal compaction, snapshots, or truncation besides ContinueAsNew
- Backend-side warning in stores

## Behavior

### Configuration

```go
type WorkerOptions struct {
    // ...
    JournalWarnThreshold int // 0 → default 10000; <0 → disabled
}
```

`withDefaults`: if `JournalWarnThreshold == 0`, set to `10000`. Negative values are left as-is (disabled).

### Check site

In `handleWorkflow`, after loading state and before `engine.RunAt`:

1. Let `n = len(state.Journal)` (persisted journal only; inbox not yet ingested for this turn is excluded).
2. Let `threshold = w.opts.JournalWarnThreshold` (after defaults).
3. If `threshold > 0` and `n >= threshold`:
   - Log Warn: `"journal size warning"` with attrs `instance_id`, `workflow`, `journal_events`, `threshold`
   - `Metrics.AddJournalWarning(ctx, 1)` (no-op if Metrics nil)
4. Continue normal processing (replay, commit, ContinueAsNew, etc.).

Frequency: at most once per workflow task that meets the condition (no in-memory dedupe across tasks).

### Metrics

| Name | Type | When |
|---|---|---|
| `tasuki.workflow.journal_warnings` | Counter | Each time the worker emits the journal size warning |

Helper: `(*observability.Metrics).AddJournalWarning(ctx, n int64)` matching existing counter helpers.

## Documentation

| File | Change |
|---|---|
| `docs/02-architecture.md` | Update §履歴の肥大: sticky cache is implemented (M5); describe warn threshold (`0`→10000, `<0` off); point to ContinueAsNew |
| `docs/03-api.md` | Document `JournalWarnThreshold` semantics; short ContinueAsNew guidance (loops / long-lived → truncate near ~10k events) |
| `docs/05-observability.md` | Add Warn row + journal_warnings counter |
| `docs/04-plan.md` | Risk row: note warning threshold + guidance delivered in M5 |
| `README.md` | 1–2 sentences near sticky/observability: threshold warn + ContinueAsNew; link docs |

Guidance content (keep short):

- Journals are append-only and grow without bound for looping workflows.
- Consider ContinueAsNew when event count approaches thousands–~10k (aligned with default warn).
- ContinueAsNew starts a new instance with new input and terminates the old run as `continued` (ID `{id}~{seq}`).
- Warnings do not stop execution; ignoring them increases replay latency and store pressure.

## Verification

- Worker test with small `JournalWarnThreshold` (e.g. `3`): after journal grows past threshold, processing a task emits Warn (test logger) and/or increments metrics.
- `JournalWarnThreshold < 0`: no warning on large journal.
- Zero / default: small journals do not warn.
- Existing `TestWorker_ContinueAsNew` stays green.

## Acceptance

- [ ] `JournalWarnThreshold` on `WorkerOptions` with defaults above
- [ ] Worker warn + `tasuki.workflow.journal_warnings` as specified
- [ ] Docs/README updated
- [ ] Tests cover warn / disable / non-warn paths
- [ ] No change to ContinueAsNew runtime semantics
