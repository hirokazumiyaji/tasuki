# M4 Spanner Backend Design

**Date:** 2026-07-23  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M4 item 3, [docs/02-architecture.md](../../02-architecture.md)  
**Decisions:** Local = Spanner Emulator; schema = flat (non-interleaved); approach = dedicated `backend/spanner` module with conditional-update claims.

## Goal

Add Cloud Spanner as the first backend that uses the **conditional-update claim strategy** (no `FOR UPDATE SKIP LOCKED`). Pass the shared conformance suite and chaos tests against the official emulator. Do not introduce a new engine API surface.

## Non-goals

- Production multi-region tuning / interleaving optimization
- Real GCP Spanner in CI (emulator only for this milestone)
- DynamoDB / Firestore (later M4 items)
- Sticky cache, LISTEN-style notify (M5)

## Module and dependencies

| Item | Choice |
|---|---|
| Module path | `github.com/hirokazumiyaji/tasuki/backend/spanner` |
| Client | `cloud.google.com/go/spanner` + database admin API for DDL |
| Workspace | Add to `go.work` like mysql/sqlite/postgres |
| Env | `TASUKI_SPANNER_DSN` = `projects/{p}/instances/{i}/databases/{d}` |
| Emulator | `SPANNER_EMULATOR_HOST=localhost:9010` (compose service) |

`New(ctx, dsn)` opens a `spanner.Client`. Test/example helpers may create instance + database via admin APIs when talking to the emulator.

## Emulator topology

`docker-compose.yml` adds:

```yaml
spanner:
  image: gcr.io/cloud-spanner-emulator/emulator
  ports:
    - "9010:9010"  # gRPC
    - "9020:9020"  # REST (optional)
```

Bootstrap (tests / example):

1. Set `SPANNER_EMULATOR_HOST`
2. Create instance `tasuki` (emulator accepts free-form IDs)
3. Create database `tasuki` with schema DDL
4. Run `backendtest` / chaos / example against that DB

DSN example:

```text
projects/tasuki/instances/tasuki/databases/tasuki
```

## Schema (flat)

Same logical tables as PostgreSQL / MySQL. No `INTERLEAVE IN PARENT`.

| Table | PK | Notes |
|---|---|---|
| `wf_instances` | `id STRING(255)` | status, queue, next_seq, JSON payloads, parent refs, timestamps |
| `wf_journal` | `(instance_id, seq)` | type, name, ref_seq, payload JSON, recorded_at |
| `wf_inbox` | `id INT64` | instance_id, type, ref_seq, payload, created_at |
| `wf_tasks` | `id INT64` | kind, queue, instance_id, payload, attempt, visible_at, worker_id, **wf_singleton** |
| `wf_timers` | `(instance_id, seq)` | fire_at |
| `wf_schedules` | `id STRING(255)` | cron, workflow, queue, input, next_run_at, paused |

### Types

- Timestamps: `TIMESTAMP` (UTC). Scheduling comparisons use backend clock (`CURRENT_TIMESTAMP()` in queries / transaction read timestamp as `WorkflowState.Now`).
- Payloads: Spanner `JSON` (emulator-supported). Store engine `[]byte` via JSON values.
- Booleans: `BOOL` for `paused`.
- IDs for tasks/inbox: `INT64`. Prefer Spanner `SEQUENCE` (`bit_reversed_positive`) with column `DEFAULT (GET_NEXT_SEQUENCE_VALUE(...))` if the emulator version supports it; otherwise client-assigned positive `INT64` (documented fallback in migrate helper).

### Workflow task singleton

```sql
wf_singleton STRING(255) AS (IF(kind = 'workflow', instance_id, NULL)) STORED,
-- ...
CREATE UNIQUE NULL_FILTERED INDEX wf_tasks_wf_singleton
  ON wf_tasks(wf_singleton);
```

`INSERT` of a second workflow task for the same instance fails uniquely; enqueue uses “insert or ignore” semantics (catch already-exists / abort and treat as success).

Secondary indexes (claim / fire / list):

- `wf_tasks(kind, queue, visible_at)`
- `wf_timers(fire_at)`
- `wf_schedules(next_run_at, paused)`
- `wf_instances(status, name, created_at)` for List
- `wf_inbox(instance_id, id)`

## Claim strategy (conditional update)

For `ClaimTasks` (and analogously timers / due schedules), inside **one** `ReadWriteTransaction` (reads before writes):

1. Query up to `Limit` candidate rows where `kind`/`queue` match and `visible_at <= now`.
2. For each candidate, update only if `id` and `visible_at` still equal the values just read.
3. If the condition fails (already claimed), skip and continue.
4. Return successfully updated rows (bumped `attempt`, set `worker_id`, `visible_at` advanced by lease).

Guarantees match the architecture table: lease exclusivity without `SKIP LOCKED`. Under contention, workers may do extra reads; correctness is unchanged.

`FireDueTimers` / `ClaimDueSchedules`: same pattern (read due keys → conditional delete or conditional `next_run_at` advance).

## Advancement and I1

`CommitAdvancement` runs in one `ReadWriteTransaction`:

1. Read instance; CAS `next_seq = ExpectedSeq` → new seq (abort → `ErrConflict`).
2. Verify workflow task row exists for `TaskID` / instance.
3. Insert journal / activities / timers / children; apply terminal; drain inbox IDs.
4. Parent notify: insert parent inbox + enqueue parent workflow task (singleton ignore).
5. Delete the committing workflow task.
6. `ensureWorkflowTaskIfInbox` inside the same transaction.

**Post-commit ensure** (second short RW txn): call `ensureWorkflowTaskIfInbox` again after commit, matching the MySQL/TiDB hardening for snapshot / concurrent `SendToInbox` races (I1).

`SendToInbox`: RW txn (insert inbox + enqueue) + post-commit ensure.

Aborted transactions due to Spanner optimistic concurrency: retry with backoff inside the backend helpers (bounded); surface persistent conflict as `ErrConflict` where the protocol expects it.

## Capabilities and clock

- `Capabilities()`: empty / unlimited for Spanner (write-count caps are for DynamoDB/Firestore later).
- Clock: Spanner commit / query timestamp; no `ClockSetter` required for core suite (schedule dedup case may skip like MySQL without virtual clock).

## Testing

| Layer | How |
|---|---|
| Migrate smoke | `TASUKI_SPANNER_DSN` + emulator; idempotent DDL |
| Conformance | `backendtest.Run` in `backend/spanner` |
| Chaos | `chaos` worker with `TASUKI_BACKEND=spanner` + DSN; kill-workers loop |
| Example | `examples/m4-spanner` quickstart |
| CI | `[skip ci]` while Actions minutes exhausted; emulator documented for later CI |

## Delivery shape (PR sequence)

Same discipline as MySQL/TiDB: one commit + one PR per task; merge before next; `[skip ci]` on commit and merge subject.

1. Design/spec + plan  
2. Compose emulator + migrate/bootstrap  
3. Backend core (create/load/claim/commit)  
4. Inbox / timers / schedules / list  
5. Conformance green  
6. Chaos + example + README (next store: DynamoDB)

## Risks

| Risk | Mitigation |
|---|---|
| Emulator vs production DDL gaps (sequences, generated columns) | Probe in Task 2; document fallbacks (client INT64, STORED AS) |
| Conditional claim starvation under extreme contention | Conform ClaimExclusive + chaos; tune candidate limit |
| Unique index enqueue races | Treat already-exists as success; post-commit ensure for I1 |
| JSON / TIMESTAMP precision | Pin UTC; round-trip conform cases |

## Acceptance

- [ ] Emulator up via compose; migrate idempotent  
- [ ] `backendtest` green on Spanner  
- [ ] Chaos kill-workers green  
- [ ] README marks Spanner done; next = DynamoDB  
- [ ] No new `backend/tidb`-style fork — Spanner is its own module as designed  

## Open implementation notes (non-blocking)

Resolved at implement time, not a design blocker:

- Exact SEQUENCE vs client INT64 choice after emulator smoke (schema stays `INT64` either way)
