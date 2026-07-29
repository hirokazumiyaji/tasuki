# M4 Firestore Backend Design

**Date:** 2026-07-23  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M4 item 5, [docs/02-architecture.md](../../02-architecture.md)  
**Decisions:** Local = Firestore Emulator; schema = multi-collection (top-level); approach = dedicated `backend/firestore` with conditional-update claims + Firestore transactions.

## Goal

Add Cloud Firestore as the final M4 store backend. Use query + transactional conditional updates (no SKIP LOCKED). Declare a write-count cap via `Capabilities.MaxAdvancementEffects` (Firestore txn ≤ 500 writes). Pass conformance and chaos against the official emulator. Reuse the worker inbox clamp already added for DynamoDB.

## Non-goals

- Nested subcollections under instances (except if a later optimisation needs them)
- Real GCP Firestore in CI (emulator only)
- Firestore listen/stream notify (M5)
- Composite index provisioning via Terraform (document required indexes; create via emulator auto or `firestore.indexes` note)

## Module and dependencies

| Item | Choice |
|---|---|
| Module path | `github.com/hirokazumiyaji/tasuki/backend/firestore` |
| Client | `cloud.google.com/go/firestore` |
| Workspace | Add `./backend/firestore` to `go.work` |
| Emulator | `FIRESTORE_EMULATOR_HOST=localhost:8080` (or 8086 — pin in compose) |
| Project | Dummy `TASUKI_FIRESTORE_PROJECT=tasuki` (emulator accepts any) |

`New(ctx, projectID)` opens a client. Emulator is selected automatically when `FIRESTORE_EMULATOR_HOST` is set.

## Local topology

`docker-compose.yml` adds a Firestore emulator service, e.g.:

```yaml
firestore:
  image: gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators
  command: gcloud beta emulators firestore start --host-port=0.0.0.0:8080
  ports:
    - "8080:8080"
```

(If the image/command differs in practice, Task 2 smoke adjusts; keep host port documented in README.)

Env:

```bash
docker compose up -d firestore
export FIRESTORE_EMULATOR_HOST=localhost:8080
export TASUKI_FIRESTORE_PROJECT=tasuki
```

## Schema (multi-collection)

Top-level collections (optional prefix via config, default none or `tasuki_`):

| Collection | Doc ID | Notes |
|---|---|---|
| `wf_instances` | instance id | status, next_seq, queue, payloads, parent refs |
| `wf_journal` | `{instance_id}_{seq}` or composite path `instance_id`+field `seq` with query | Prefer doc ID `{instanceID}:{seq}` for uniqueness |
| `wf_inbox` | `{instance_id}:{id}` | id = client INT64 |
| `wf_tasks` | workflow: `WF#{instance_id}`; activity: `ACT#{id}` | fields: kind, queue, instance_id, id (N), visible_at, … |
| `wf_timers` | `{instance_id}:{seq}` | fire_at |
| `wf_schedules` | schedule id | cron, next_run_at, paused |

### Indexes (claim / due)

Composite indexes (emulator often auto-creates; document for production):

- `wf_tasks`: `kind` ASC, `queue` ASC, `visible_at` ASC  
- `wf_timers`: `fire_at` ASC  
- `wf_schedules`: `paused` ASC, `next_run_at` ASC (or query `paused==false` + `next_run_at<=now`)

`visible_at` / `fire_at` / `next_run_at`: store as `time.Time` (Firestore timestamp) or int64 µs — prefer Firestore `Timestamp` for query range.

## Claim strategy

1. Query due tasks: `kind==… AND queue==… AND visible_at<=now` ordered by `visible_at`, Limit N.
2. In a transaction (or per-doc txn): re-read; if `visible_at` unchanged, update lease/attempt/worker; else skip.
3. Same pattern for timers (query `fire_at<=now`) and schedules (`paused==false AND next_run_at<=now` + conditional advance).

## Advancement and Capabilities

`CommitAdvancement` in one Firestore transaction:

- CAS `next_seq`
- Writes for journal / activities / timers / children / parent notify / drain inbox / delete WF task / ensure

**Limit:** Firestore max **500** writes per transaction. Declare:

```go
Capabilities{MaxAdvancementEffects: 400} // headroom under 500
```

Worker already clamps inbox drain using `MaxAdvancementEffects` (DynamoDB work). No further engine change unless packing of commands still overflows — keep headroom large.

**I1:** ensure inside txn + post-commit ensure (Put `WF#id` if inbox remains). Retry on contention.

**Clock:** client `time.Now().UTC()`.

## Testing

| Layer | How |
|---|---|
| Migrate | No-op or ensure collections exist (Firestore is schemaless); optional index doc |
| Conformance | `backendtest.Run` |
| Chaos | `TASUKI_BACKEND=firestore` |
| Example | `examples/m4-firestore` |
| CI | `[skip ci]` |

## Delivery shape (PR sequence)

1. Design/spec + plan  
2. Compose emulator + module scaffold + smoke New/Reset  
3. Backend core + Capabilities  
4. Inbox / schedules / List / children  
5. Conformance green  
6. Chaos + example + README → **M4 complete**

## Risks

| Risk | Mitigation |
|---|---|
| Emulator index lag / missing composite index | Create indexes in smoke; fail fast with clear error |
| Query+txn races on claim | Re-read in txn; ClaimExclusive conform |
| 500 write overflow | Capabilities 400 + existing inbox clamp |
| Doc ID encoding (`:` in ids) | Escape or use separate fields + random doc IDs with unique fields |

## Acceptance

- [x] Emulator up via compose  
- [x] `backendtest` green  
- [x] Chaos green  
- [x] `Capabilities.MaxAdvancementEffects` > 0  
- [x] README: M4 complete (all planned stores); next = M5 or backlog  

## Open implementation notes (non-blocking)

- Exact emulator Docker image/tag verified in Task 2  
- Whether journal uses composite doc IDs vs subcollection (prefer top-level composite IDs per multi-collection decision)
