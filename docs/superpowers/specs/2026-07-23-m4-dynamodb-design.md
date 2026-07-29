# M4 DynamoDB Backend Design

**Date:** 2026-07-23  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M4 item 4, [docs/02-architecture.md](../../02-architecture.md)  
**Decisions:** Local = DynamoDB Local (docker); schema = multi-table; approach = dedicated `backend/dynamodb` with conditional-update claims + `TransactWriteItems`.

## Goal

Add Amazon DynamoDB as the first backend that declares a **write-count cap** via `Capabilities.MaxAdvancementEffects` (TransactWriteItems ≤ 100). Pass shared conformance and chaos against DynamoDB Local. No new engine API beyond honouring Capabilities already in the contract.

## Non-goals

- Single-table design / DynamoDB access-pattern optimisation beyond correctness
- Real AWS in CI (Local only for this milestone)
- DynamoDB Streams notify (M5)
- Firestore (next M4 item)

## Module and dependencies

| Item | Choice |
|---|---|
| Module path | `github.com/hirokazumiyaji/tasuki/backend/dynamodb` |
| Client | `github.com/aws/aws-sdk-go-v2` (`service/dynamodb`) |
| Workspace | Add `./backend/dynamodb` to `go.work` |
| Endpoint | `TASUKI_DYNAMODB_ENDPOINT` (e.g. `http://localhost:8000`) |
| Region / creds | `AWS_REGION=us-east-1`, dummy keys for Local (`AWS_ACCESS_KEY_ID=local`, `AWS_SECRET_ACCESS_KEY=local`) |
| Table prefix | Optional `TASUKI_DYNAMODB_TABLE_PREFIX` (default `tasuki_`) |

`New(ctx, cfg)` builds a DynamoDB client. Tests/examples call `Migrate` to create tables if missing.

## Local topology

`docker-compose.yml` adds:

```yaml
dynamodb:
  image: amazon/dynamodb-local:2.5.2
  command: ["-jar", "DynamoDBLocal.jar", "-sharedDb", "-inMemory"]
  ports:
    - "8000:8000"
```

Env example:

```bash
docker compose up -d dynamodb
export TASUKI_DYNAMODB_ENDPOINT=http://localhost:8000
export AWS_ACCESS_KEY_ID=local
export AWS_SECRET_ACCESS_KEY=local
export AWS_REGION=us-east-1
```

## Schema (multi-table)

Logical tables mirror RDB backends. Attribute names use snake_case strings.

### `wf_instances`

- PK: `id` (S)
- Attrs: `name`, `queue`, `status`, `input`/`result`/`failure` (S JSON or B), `parent_id`, `parent_seq`, `next_seq` (N), timestamps (S RFC3339Nano or N epoch ms)

### `wf_journal`

- PK: `instance_id` (S), SK: `seq` (N)
- Attrs: `type`, `name`, `ref_seq`, `payload`, `recorded_at`

### `wf_inbox`

- PK: `instance_id` (S), SK: `id` (N) — client-assigned monotonic-ish INT64 (crypto/rand positive)
- Attrs: `type`, `ref_seq`, `payload`, `created_at`

### `wf_tasks`

- PK: `task_pk` (S)
  - Workflow tasks: `WF#<instance_id>` (singleton by key)
  - Activity tasks: `ACT#<ulid-or-rand>`
- Attrs: `id` (N) — engine-facing task ID (same int64 space as other backends); `kind`, `queue`, `instance_id`, `ref_seq`, `payload`, `attempt`, `max_attempts`, `visible_at`, `worker_id`, `created_at`
- GSI `claim_gsi`: PK `gsi_pk` = `kind#queue` (S), SK `visible_at` (S lexicographic RFC3339Nano with fixed fractional digits, or N epoch µs)

Claim Query: `gsi_pk = kind#queue AND visible_at <= now`, Limit N; then conditional Update on base table (`visible_at = :old`).

### `wf_timers`

- PK: `instance_id` (S), SK: `seq` (N)
- Attrs: `fire_at`
- GSI `fire_gsi`: PK constant `TIMER` (or shard), SK `fire_at` — Query due timers

### `wf_schedules`

- PK: `id` (S)
- Attrs: `cron`, `workflow`, `queue`, `input`, `next_run_at`, `paused`
- GSI `due_gsi`: PK constant `SCHED` (or shard), SK `next_run_at`; filter `paused = false`

## Claim strategy (conditional update)

Same guarantee as Spanner path:

1. Query GSI for up to `Limit` due task keys.
2. For each: `UpdateItem` with `ConditionExpression` on previous `visible_at` (and optionally `task_pk`).
3. Skip ConditionalCheckFailed; return successfully claimed items (bump `attempt`, set `worker_id`, advance `visible_at` by lease).

Timers / schedules: Query due → conditional delete or conditional `next_run_at` advance.

## Advancement, transactions, Capabilities

`CommitAdvancement` builds a `TransactWriteItems` list:

1. Update instance `next_seq` with condition `next_seq = ExpectedSeq` (fail → `ErrConflict`).
2. Condition: workflow task item exists for `TaskID` / instance.
3. Put journal events; Put activity tasks; Put timers; Update terminal; Delete drained inbox; Put children + child journal/task; Parent notify inbox + enqueue parent WF task; Delete committing WF task; ensure WF task if inbox remains.

**Hard limit:** DynamoDB TransactWriteItems max **100** actions. Declare:

```go
Capabilities{MaxAdvancementEffects: 80} // headroom under 100 for protocol overhead
```

As of this writing the engine does **not** yet read `MaxAdvancementEffects` when packing advancements. This milestone **must** add that clamp in the worker/commit path (inbox drain / effect packing) so DynamoDB transactions stay under the limit. Without it, large inbox workflows can exceed TransactWriteItems and fail at runtime.

**I1:** ensure inside the transaction where possible; **post-commit ensure** in a follow-up conditional Put of `WF#instance_id` if inbox non-empty (Spanner/MySQL pattern). Retry on transaction conflicts.

**SendToInbox:** Put inbox + conditional Put WF task; post-commit ensure.

## Clock

Client clock (`time.Now().UTC()`). Lease / timer correctness relies on CAS/delete exclusivity, not clock authority (per architecture). No `ClockSetter` required for core suite (schedule dedup may skip).

## Testing

| Layer | How |
|---|---|
| Migrate | CreateTable if not exists; idempotent |
| Conformance | `backendtest.Run` |
| Chaos | worker `TASUKI_BACKEND=dynamodb` + Local endpoint |
| Example | `examples/m4-dynamodb` |
| CI | `[skip ci]` while Actions minutes exhausted |

## Delivery shape (PR sequence)

1. Design/spec + plan  
2. Compose Local + migrate/bootstrap  
3. Backend core (create/load/claim/commit/activity/timers) + Capabilities wiring  
4. Inbox / schedules / List / children  
5. Conformance green  
6. Chaos + example + README (next: Firestore)

## Risks

| Risk | Mitigation |
|---|---|
| Transact 100 overflow | Capabilities + engine clamp; conform with large inbox |
| GSI eventual consistency on Local | Use consistent reads on base table after claim; Local is usually immediate — still assert ClaimExclusive |
| Lexicographic time on GSI SK | Fixed-width RFC3339Nano or epoch integer SK |
| Workflow singleton races | PK `WF#id` + attribute_not_exists on Put |
| Engine ignores MaxAdvancementEffects | Probe codebase; implement clamp if needed |

## Acceptance

- [x] DynamoDB Local up via compose; migrate idempotent  
- [x] `backendtest` green  
- [x] Chaos kill-workers green  
- [x] `Capabilities.MaxAdvancementEffects` > 0 and enforced  
- [x] README marks DynamoDB done; next = Firestore  

## Open implementation notes (non-blocking)

- Exact GSI SK encoding (epoch µs vs fixed RFC3339) after Local smoke  
- Whether activity `id` (N) is also the mutation key or only an attribute alongside `task_pk`
