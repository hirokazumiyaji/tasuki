# M5 Cross-Process Notify (DynamoDB / Firestore) Design

**Date:** 2026-07-26  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5 通知; deferred from in-process hub slice  
**Decisions:** Keep in-process `hub` for same Backend; add store-backed wake so **another process** with its own Backend can Subscribe. Hints only. Polling remains fallback.

## Goal

Workers / Clients in separate processes wake early when DynamoDB or Firestore backends mutate tasks or terminals.

## Non-goals

- Changing postgres LISTEN/NOTIFY
- Removing PollInterval
- Exactly-once delivery of wake hints
- Shared hub across processes

## Mechanism

### Shared

Emit sites stay the same (`notifyTasks` / `notifyTerminal`). Each call:

1. `hub.Notify*` (same-process subscribers)
2. Persist a wake marker (cross-process)

`Subscribe*` returns a channel fed by **hub OR store listener** (coalesced buffer-1).

### Firestore

| Kind | Document | Write |
|---|---|---|
| tasks | `wf_notify/tasks` | `{n: Increment(1), at: server time}` |
| terminal | `wf_notify/terminal` | `{n: Increment(1), id: instanceID, at: ...}` |

`Subscribe` / `SubscribeTerminal`: `Snapshots` on the doc; ignore initial snapshot; on change coalesce-send. Also fan-in hub channel.

### DynamoDB

| Table | Keys | Stream |
|---|---|---|
| `wf_wake` | `pk` (S) = `tasks` \| `terminal` | `NEW_IMAGE` enabled on create |

Writes: `UpdateItem` `ADD n :one` (and `SET id = :id` for terminal).

`Subscribe*`: in-process hub fan-in **plus** a DynamoDB Streams poller on `wf_wake` (shard iterators, GetRecords loop). Any record for the matching `pk` coalesces a wake. Stream setup failure → log/debug and rely on hub + PollInterval (Subscribe still succeeds).

## Tests

- Existing same-Backend notify tests remain green (hub path)
- New test (emulator): Backend A Subscribe, Backend B CreateInstance / Terminate → A wakes (skip without emulator)
- DynamoDB Local: skip Streams test if stream ARN unavailable

## Docs

- README: DDB/FS support cross-process wake via wake markers / Streams; postgres remains LISTEN/NOTIFY
- Update prior “out of scope” notes in older specs only if needed via README

## Acceptance

- [ ] Firestore snapshot cross-process wake
- [ ] DynamoDB wf_wake + Streams (or documented skip)
- [ ] Hub path unchanged for same-process
- [ ] README note
