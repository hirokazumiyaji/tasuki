# M6 Local Activity Design

**Date:** 2026-07-29  
**Status:** Approved  
 
**Parent:** Short, same-worker activity execution without the activity task queue  
**Decisions:** Sync run on the workflow Worker; no retries in v1; dedicated `workflow.ExecuteLocal`; SideEffect-like single journal command (approach A).

## Goal

Run a registered activity function **inside the workflow task** on the same Worker, without enqueueing an activity task. Record the result (or failure) in the journal so replay is deterministic. Intended for short, reliable work where task-queue round-trips are wasteful.

## Non-goals

- RetryPolicy / automatic retries for local activities
- `ExecuteLocalAsync` / Await integration in v1
- Heartbeat / lease extension for local activities
- Separate registration API (reuse `RegisterActivity`)
- Changing normal `Execute` semantics

## API

```go
tasuki.RegisterActivity(w, LookupCustomer, tasuki.WithName("lookup"))

out, err := workflow.ExecuteLocal[LookupIn, LookupOut](ctx, "lookup", in)
```

- Name-based, same registry as normal activities.
- Unregistered name → error (do not panic the workflow goroutine as determinism stuck unless the runner is missing entirely).
- Query mode: same as other side effects (`recordOrReplay` suspend / reject).

## Semantics

| Case | Behavior |
|---|---|
| First record | Marshal input → LocalRunner(name, input) → marshal result or error → journal command |
| Replay | Unmarshal recorded payload; do **not** call LocalRunner |
| Activity returns error | Error string recorded; `ExecuteLocal` returns that error to workflow code |
| LocalRunner unset (e.g. unit engine without Worker) | Return a clear error when recording a new local activity |

Local activities do **not** create `wf_tasks` rows, do not use Claim/CompleteActivity, and do not appear as activity tasks in metrics beyond an optional counter later.

## Journal

- New type: `local_activity` (`journal.TypeLocalActivity`)
- `IsCommand() == true`
- `Event.Name` = activity name
- Payload (JSON), e.g.:

```json
{"input": <raw>, "result": <raw>, "error": ""}
```

or on failure:

```json
{"input": <raw>, "error": "customer not found"}
```

`MatchCommand` matches type + name (like activity_scheduled). Input is stored for audit/debug; replay uses `result` / `error` only.

## Worker / Context

Worker sets a runner on the workflow `Context` before `engine.Run` / `RunQuery`:

```go
type LocalActivityRunner func(name string, input []byte) (result []byte, err error)

func (c *Context) SetLocalActivityRunner(r LocalActivityRunner)
```

Runner implementation: look up `w.reg.activity(name)`, invoke with a background/`context.Background()` (or workflow-task-scoped ctx without heartbeat), codec marshal/unmarshal around the registered fn. No activity task / IdempotencyKey required in v1 (optional: stable key `"{instanceID}/local/{seq}"` in ActivityInfo if Info is set — YAGNI unless cheap).

`CommitAdvancement` needs no special case: local activity events are plain journal appends in `NewEvents` like SideEffect / search attrs.

## Tests

- Unit: record + replay without calling runner twice; failure path returns error
- Integration (memory Worker): RegisterActivity + ExecuteLocal succeeds; unregistered name fails
- Query mode does not persist a new local activity

## Docs

- `docs/03-api.md` — ExecuteLocal table row + short note vs Execute
- README one-liner if space
- Architecture journal type list if enumerated

## Acceptance

- [x] `workflow.ExecuteLocal` + `TypeLocalActivity`
- [x] Worker LocalRunner from activity registry
- [x] No activity task enqueue
- [x] Replay skips runner
- [x] Tests + docs
