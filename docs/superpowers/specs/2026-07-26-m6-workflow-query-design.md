# M6 Workflow Query Handlers Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) 将来候補「クエリハンドラ」  
**Decisions:** Queries run on a Worker (needs workflow registry); replay journal (+ visible inbox) in read-only query mode; no Backend schema change; `SetQueryHandler` is not journaled (re-registered every replay).

## Goal

Ask a running (or terminal) workflow instance for derived state without signaling or advancing history.

## Non-goals

- Cross-process query RPC separate from embedding Worker
- Strong consistency beyond last committed journal (+ currently visible inbox snapshot)
- Mutating workflow state from a query handler
- Search attributes / List filters (separate candidate)

## API

```go
// In workflow code (deterministic, same point every replay):
workflow.SetQueryHandler[I, O](ctx, "name", func(in I) (O, error) { ... })

// Caller (same process as Worker with registered workflows):
out, err := tasuki.Query[I, O](ctx, w, instanceID, "name", in)
```

`SetQueryHandler` replaces any prior handler with the same name on that Context.

## Execution

1. `LoadWorkflow` for `instanceID` (journal, inbox, next_seq, now).
2. Build event list = journal + inbox items with provisional seqs (same as a workflow task, but **do not claim/commit**).
3. `engine.RunQuery(events, now, queryName, argBytes, fn)`:
   - Context `queryMode=true`: attempting to record a **new** command suspends (Goexit) instead of appending.
   - Workflow registers query handlers during replay.
   - After suspend/complete/stuck: invoke named handler with arg; marshal result.
4. Discard any speculative commands; return handler result or `ErrUnknownQuery` / stuck error.

Query handlers must be read-only (no `Execute` / `Sleep` / etc. that would need new commands). If the handler itself calls APIs that need new commands, query fails.

## Errors

| Case | Error |
|---|---|
| Unknown instance | `backend.ErrNotFound` |
| Unknown query name | `workflow.ErrUnknownQuery` |
| Determinism / panic during replay | wrapped stuck/panic error |
| Handler error | returned to caller |

## Tests

- Suspended workflow with local state; Query returns updated values after signals/progress
- Unknown query name
- Query does not create tasks / change next_seq

## Docs

- `docs/03-api.md`, `docs/04-plan.md` future list, README one-liner

## Acceptance

- [ ] `SetQueryHandler` + queryMode
- [ ] `engine.RunQuery` / `tasuki.Query`
- [ ] Tests
- [ ] Docs
