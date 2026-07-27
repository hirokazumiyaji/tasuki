# M6 Search Attributes Implementation Plan

> Inline execution. One commit + PR per task. Every commit/merge subject includes `[skip ci]`.

**Goal:** String search attributes on instances: set at Start, upsert from workflow, filter via `List`.

**Architecture:** Persist `search_attributes` JSON map on `wf_instances`. Upserts are journal commands (`search_attributes_updated`) whose payload is the **full merged map** after apply; empty string values delete keys. `CommitAdvancement` updates the instance column when such events are present.

**Spec:** [docs/superpowers/specs/2026-07-27-m6-search-attributes-design.md](../specs/2026-07-27-m6-search-attributes-design.md)

### Task 1: Spec + Plan
### Task 2: Types + journal + memory + workflow Upsert + Start option + unit tests
### Task 3: SQL backends (postgres / sqlite / mysql) schema + paths
### Task 4: Spanner + DynamoDB + Firestore
### Task 5: backendtest + integration tests + docs

## Notes

### Types (`backend/types.go`)

```go
type InstanceFilter struct {
	Status           string
	Name             string
	SearchAttributes map[string]string // AND exact match; empty = ignore
	Limit            int
	Offset           int
}

type NewInstance struct {
	// ...existing...
	SearchAttributes map[string]string
}

type Instance struct {
	// ...existing...
	SearchAttributes map[string]string
}
```

### Journal (`journal/event.go`)

- `TypeSearchAttributesUpdated Type = "search_attributes_updated"`
- `IsCommand() == true` (like side_effect / version_marker)
- Payload: JSON object of the **merged** map (`{}` allowed)

### Merge helper (`backend/search_attributes.go` or small util)

```go
// MergeSearchAttributes returns a copy of base with patch applied.
// Empty patch values delete the key. Nil base treated as empty.
func MergeSearchAttributes(base, patch map[string]string) map[string]string
```

Also `MatchesSearchAttributes(inst, filter map[string]string) bool` for List post-filter / shared AND logic.

### Workflow API (`workflow/search_attributes.go`)

```go
func UpsertSearchAttributes(ctx *Context, attrs map[string]string)
```

- Maintain `ctx.searchAttributes map[string]string` (seed from journal on `NewContext` by applying each `search_attributes_updated` payload in order; or seed from `WorkflowInfo` / worker — prefer rebuild from journal so replay stays self-contained).
- Call `recordOrReplay(Command{Type: TypeSearchAttributesUpdated}, mergedJSON)`.
- On replay, set `ctx.searchAttributes` from recorded payload (authoritative).
- Query mode: `recordOrReplay` already suspends / rejects side effects — no special case beyond that.

### Client (`options.go`, `client.go`)

```go
func WithSearchAttributes(attrs map[string]string) StartOption
```

Pass into `CreateInstance` via `NewInstance.SearchAttributes` (copy map; nil-safe).

### CommitAdvancement (all stores)

After inserting journal events, if any `NewEvents` has `TypeSearchAttributesUpdated`, take the **last** such event’s payload as the new map and UPDATE `wf_instances.search_attributes`. (Multiple upserts in one tick: last wins; each event still journaled.)

### List

- Filter AND: every `filter.SearchAttributes[k] == instance.SearchAttributes[k]`.
- Missing key on instance → no match.
- SQL: prefer JSON operators where easy; otherwise fetch+filter within Limit (document scan caveats for DynamoDB/Firestore). Correctness over index perfection in v1.

### Schema

| Store | Change |
|---|---|
| postgres / mysql / sqlite | `search_attributes` JSON/JSONB/TEXT NOT NULL DEFAULT `'{}'` on `wf_instances` (idempotent `ALTER` / recreate in embed schema as existing pattern) |
| spanner | JSON column on `wf_instances` |
| dynamodb / firestore | map field on instance item/doc (omit or `{}` when empty) |
| memory | field on in-memory instance |

### Tests

- Unit: merge helper; Upsert merge/delete + replay; Start option → CreateInstance attrs
- backendtest: Create with attrs → Get; Commit upsert → Get; List AND + Status combo (memory + sqlite minimum via suite)
- Integration: Start → Upsert → List sees attrs; query mode does not persist upsert

### Docs

- `docs/03-api.md`, `docs/04-plan.md` (remove 検索属性), README, architecture note if Instance shape is documented
