# M6 Memo Design

**Date:** 2026-07-28  
**Status:** Approved  
 
**Parent:** Display-only instance annotations (alongside search attributes)  
**Decisions:** Start + in-workflow upsert; `map[string]string` only; no List filter; merge upsert with empty string = delete; store as JSON map column `memo` on `wf_instances` (approach A).

## Goal

Attach small string key/value notes to workflow instances for visibility on `Get` (and UI later), settable at start and updatable from deterministic workflow code. Unlike search attributes, memos are **not** used for `List` filtering.

## Non-goals

- List / `InstanceFilter` matching on memo keys
- Non-string types or nested JSON
- Merging memo into search attributes
- Rich UI editors beyond reading `Instance.Memo`

## API

```go
h, err := tasuki.Start(ctx, c, "order", in,
    tasuki.WithID("o-1"),
    tasuki.WithMemo(map[string]string{
        "note": "vip",
    }),
)

workflow.UpsertMemo(ctx, map[string]string{
    "note":  "vip-shipped",
    "debug": "", // empty string removes the key
})

inst, err := c.Get(ctx, "o-1")
_ = inst.Memo // map[string]string
```

`backend.Instance` and `backend.NewInstance` gain `Memo map[string]string` (nil/empty = none).  
`InstanceFilter` is unchanged (no memo fields).

## Semantics

| Operation | Behavior |
|---|---|
| Start with memo | Persisted on `CreateInstance` |
| Upsert | Merge into current map; empty value deletes that key |
| Replay | Upsert is journaled; payload is the **full merged** map after apply |
| Get / LoadWorkflow | Returns current map |
| List | Ignores memo (status/name/search attributes only) |

Query mode (`SetQueryHandler` / `tasuki.Query`) must not allow Upsert (same as other mutating APIs).

## Storage

Column / field on instance document: `memo` JSON object (default `{}`).

| Store | Notes |
|---|---|
| postgres / mysql / sqlite / spanner | JSON column + migrate ALTER |
| memory | `map[string]string` on instance |
| dynamodb / firestore | map/JSON attribute on instance item/doc |

`CommitAdvancement`: if `NewEvents` contains `memo_updated`, set instance `memo` from the **last** such event’s payload.

## Journal

- New `journal.Type`: `memo_updated` (`TypeMemoUpdated`)
- `IsCommand() == true`
- Payload: JSON object of the merged map after apply (`{}` allowed)
- `MatchCommand` treats it like other recorded commands

## Workflow / Worker

- `workflow.UpsertMemo(ctx, attrs map[string]string)`
- Context holds in-memory memo; seed via `SetMemo` from `Instance.Memo` in Worker and Query (mirror search attributes)
- `NewContext` also applies historical `memo_updated` payloads from the journal

## Tests

- Start memo visible on Get; absent from List filter behavior (List unchanged)
- Upsert merge + empty-string delete
- Replay does not duplicate upsert effects
- backendtest: Create → Get; Commit upsert → Get (memory + sqlite minimum via suite)
- Integration: Start → Upsert → Get

## Docs

- `docs/03-api.md` (WithMemo, UpsertMemo, Instance.Memo)
- `docs/02-architecture.md` (schema column)
- `docs/04-plan.md` note if needed
- README one-liner

## Acceptance

- [ ] `WithMemo` on Start
- [ ] `workflow.UpsertMemo` + journal
- [ ] Persist on all backends; Get returns Memo
- [ ] No List filter on memo
- [ ] Tests
- [ ] Docs
