# M6 Workflow Update Design

**Date:** 2026-07-29  
**Status:** Approved  
**Parent:** Local Activity / StartToClose 後の表現力ギャップ; Temporal Update 相当  
**Decisions:** Sync Update via Worker (Query-like); handlers have full workflow powers (`Execute` / `Sleep`); optional `WithUpdateID` for idempotency; single-goroutine cooperative model (in-flight Update blocks main WF command progress).

## Goal

Let a caller send a named request to a **running** workflow instance, run a registered handler that may schedule activities / sleep, and **wait for a typed response**—without Terminal-ending the workflow.

## Non-goals

- Validator API (`SetUpdateHandler` + separate validator)
- True parallel coroutines (main WF + Update concurrently creating commands)
- Client-only Update without an embedded Worker
- Async-only Update API (accept-without-wait) as the primary surface
- List / search on Updates
- Cross-process Update RPC separate from Worker embedding

## Constraint: single goroutine

tasuki workflows run on one goroutine. v1 serializes Updates with the main workflow:

- While an Update is **accepted but not completed**, each workflow task replays the main function only to its last wait point (no new main commands), then **re-enters the Update handler from the top** (journal replay supplies activity/timer results).
- New `update_requested` events stay in inbox / wait until the current Update finishes (or until the main WF first reaches a wait point with handlers registered).

## API

```go
// Workflow (deterministic registration every replay):
workflow.SetUpdateHandler[I, O](ctx, "revise", func(ctx *workflow.Context, in I) (O, error) {
    // May call Execute, Sleep, UpsertMemo, etc.
    return out, nil
})

// Caller (same process as Worker with the workflow registered):
out, err := tasuki.Update[I, O](ctx, w, instanceID, "revise", in,
    tasuki.WithUpdateID("rev-42"), // optional
)
```

- `SetUpdateHandler` replaces any prior handler with the same name on that Context (like Query).
- Handler receives `*workflow.Context` so it shares the same journal cursor / codec as the main workflow.
- If `WithUpdateID` is omitted, `Update` generates a UUID for correlation (returned path still keyed by that id in the journal).
- Unknown handler name after replay → error to caller (`workflow.ErrUnknownUpdate` or similar).
- Terminal instances → error (do not accept new Updates).

### Idempotency (`WithUpdateID`)

Same `(instanceID, updateID)`:

| State | Behavior |
|---|---|
| Already `update_completed` in journal | Return the stored result / error (no new inbox row) |
| Accepted, not completed | Wait until completed (do not enqueue duplicate request) |
| Never seen | Enqueue `update_requested` and wait |

Reuse signal-dedupe ideas where useful; completed results are authoritative from the journal. Optional `wf_update_dedupe` / reuse `wf_signal_dedupe` only if enqueue races require it—prefer journal scan + conditional inbox insert when sufficient.

## Journal

| Type | Class | Fields |
|---|---|---|
| `update_requested` | Completion (inbox → journal) | `Name` = handler name; payload `{ "id", "input" }` |
| `update_accepted` | Command | `Name` = handler name; payload `{ "id" }` (and optionally echo input) |
| `update_completed` | Command | `Name` = handler name; payload `{ "id", "result"?, "error"? }` |

- `IsCompletion()` includes `update_requested` (ingested like signals).
- `IsCommand()` includes `update_accepted` and `update_completed`.
- `MatchCommand` for accepted/completed: type + Name (id is in payload; replay order ties them).

## Execution (Worker)

1. `Update` marshals input, ensures `updateID`, checks journal for prior completion / in-flight accept; else `SendToInbox(update_requested)`.
2. Drive the Worker (`PollOnce` / existing wakeup) until `update_completed` for that id appears (or ctx cancel / terminal failure).
3. On each workflow task (unchanged ingest of inbox into journal):
   - Replay main workflow with `SetUpdateHandler` registrations.
   - After replay reaches suspend (or before terminal commit):  
     - If an accepted Update is incomplete → invoke that handler again with original input (from accepted/requested payload).  
     - Else if a new `update_requested` is available and handlers are registered → record `update_accepted`, invoke handler.
   - Handler return → append `update_completed` (result or error string).
   - Handler waits on activity/timer → suspend; commit commands including accepted (if new) but not completed.
4. Do **not** advance main WF past its wait with new commands while an Update is in flight.

Query mode: Update handlers must not run as part of Query; `Update` is a mutating path only.

## Errors

| Case | Error |
|---|---|
| Unknown instance | `backend.ErrNotFound` |
| Not running | clear error (e.g. `ErrNotRunning`) |
| Unknown update name | `workflow.ErrUnknownUpdate` |
| Handler error | returned to `Update` caller; also stored on `update_completed` |
| Determinism / incompatible Worker | existing Nack path |

## Tests

- Unit: accept → complete payload shape; replay does not double-accept; handler Execute suspend/resume
- Integration (memory Worker): Update mutates memo/search attrs or runs a short activity and returns value; `WithUpdateID` replay returns same result; unknown name fails; Query unchanged

## Docs

- `docs/03-api.md` — `SetUpdateHandler` / `tasuki.Update`
- README one-liner
- `docs/04-plan.md` M6 note
- Architecture journal type table

## Acceptance

- [x] Journal types + `SetUpdateHandler` + `tasuki.Update` + `WithUpdateID`
- [x] Worker cooperative dispatch; handler may `Execute` / `Sleep`
- [x] Idempotent completed Update ID
- [x] Tests + docs
