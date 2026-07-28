# M6 Search Attributes Design

**Date:** 2026-07-27  
**Status:** Approved  
 
**Parent:** [docs/04-plan.md](../../04-plan.md) 将来候補「検索属性」  
**Decisions:** Start-time + in-workflow upsert; string keys/values only; List exact-match AND filters; merge upsert with empty string = delete; store as JSON map on `wf_instances` (approach A).

## Goal

Attach small string key/value metadata to workflow instances for visibility (`List` / `Get`), settable at start and updatable from deterministic workflow code.

## Non-goals

- Non-string types (int, bool, time, keyword lists)
- Comparison operators (`>=`, prefix, full-text)
- Registered attribute schema / search-attribute catalog service
- Separate visibility store
- Rich UI filtering beyond Client `List` (contrib UI can follow later)

## API

```go
h, err := tasuki.Start(ctx, c, "order", in,
    tasuki.WithID("o-1"),
    tasuki.WithSearchAttributes(map[string]string{
        "tenant":   "acme",
        "order_id": "42",
    }),
)

workflow.UpsertSearchAttributes(ctx, map[string]string{
    "phase":    "shipped",
    "order_id": "", // empty string removes the key
})

list, err := c.List(ctx, tasuki.InstanceFilter{
    Status: tasuki.StatusRunning,
    SearchAttributes: map[string]string{"tenant": "acme"},
})
```

`backend.Instance` gains `SearchAttributes map[string]string` (nil/empty = none).  
`InstanceFilter.SearchAttributes` is AND of exact key/value matches (in addition to existing Status/Name).

## Semantics

| Operation | Behavior |
|---|---|
| Start with attrs | Persisted on `CreateInstance` |
| Upsert | Merge into current map; empty value deletes that key |
| Replay | Upsert is journaled; replay must not re-apply side effects twice |
| List | Exact match per provided key; missing key on instance → no match |
| Get | Returns current map |

Upsert must be recorded in the journal (new event type, e.g. `search_attributes_updated`) with enough payload to rebuild the post-merge map (or a deterministic delta). Worker `CommitAdvancement` updates `wf_instances.search_attributes` when such events are committed.

Query mode (`SetQueryHandler` / `tasuki.Query`) must not allow Upsert (same as other mutating APIs: queryMode suspend / side-effect rejection).

## Storage

Column / field on instance document: `search_attributes` JSON object (default `{}`).

| Store | Notes |
|---|---|
| postgres / mysql / sqlite / spanner | JSON column + migrate |
| memory | `map[string]string` on instance |
| dynamodb / firestore | map attribute on instance item/doc |

List filtering: use native JSON operators where practical; otherwise filter after fetch within the existing Limit semantics (document any scan caveats for DynamoDB/Firestore). Prefer correct AND semantics over perfect index use in v1.

## Journal

- New `journal.Type` for search-attribute updates (command + event).
- `MatchCommand` treats it like other recorded commands.
- Payload: JSON object of the **merged** map after apply (simplest replay: replace instance map from payload on commit; workflow Context also updates in-memory view during replay from recorded events).

## Tests

- Start attrs visible on Get / List
- Upsert merge + empty-string delete
- List AND across multiple keys; Status+attr combination
- Replay does not duplicate upsert effects
- backendtest coverage (memory + sqlite at minimum; others via suite)

## Docs

- `docs/03-api.md` (Start option, Upsert, List filter, Instance field)
- `docs/04-plan.md` future list
- README one-liner
- Architecture visibility index note if needed

## Acceptance

- [x] `WithSearchAttributes` on Start
- [x] `workflow.UpsertSearchAttributes` + journal
- [x] Persist + List filter on all backends
- [x] Tests
- [x] Docs
