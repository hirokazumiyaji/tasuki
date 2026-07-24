# M5 Benchmark Foundation Design

**Date:** 2026-07-24  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) M5（性能と拡張）  
**Decisions:** Primary metric = E2E throughput (completed instances/s); stores = memory + postgres; harness = `cmd/bench` CLI; workers = in-process (Approach A).

## Goal

Provide a stable, repeatable **end-to-end throughput** harness so later M5 work (store notify, batch claim/commit, sticky journal cache) can be compared against a baseline. This slice does **not** set numeric SLOs; it only makes measurement possible.

## Non-goals

- CI always-on gate or regression budgets (revisit after the harness exists)
- All M4 backends (mysql, tidb, spanner, dynamodb, firestore)
- Multi-process workers (chaos-style spawn) — optional later
- Full latency distribution analysis (p50/p99 as primary); wall time and completed/failed are enough as secondary fields
- Engine public API changes or production notify hooks
- Encryption Codec / Web UI

## Primary metric

| Metric | Definition |
|---|---|
| Throughput | `completed / wall_seconds` (instances per second) |

**Secondary (always reported):** `wall`, `completed`, `failed`, `workers`, `instances`, `steps`, `backend`, `poll`.

## Workload

Fixed workflow name `bench-chain` (chaos-shaped):

- Workflow runs a lightweight activity `steps` times in series, then returns.
- Activity body is near no-op so the store + engine dominate.
- Default `steps=3`.

## Measurement protocol

1. Open backend: `memory` or `postgres`.
2. For postgres: `Migrate` then `Reset` (same as chaos). DSN from `TASUKI_POSTGRES_DSN` (required when `-backend=postgres`).
3. Start `workers` in-process Workers in the same process (distinct `WorkerID` each). Shared `PollInterval` from `-poll` (default `20ms`).
4. Client `Start`s `instances` workflows with IDs `bench-<i>`.
5. Wait until all complete, or until `-duration` elapses (if non-zero). On duration cut-off, count only completed instances toward throughput; report `failed` / incomplete separately as needed.
6. Shut down workers gracefully; print results.

No warmup in the first slice (`--warmup` may be added later).

## Defaults

| Flag / knobs | Default |
|---|---|
| `-backend` | `memory` |
| `-workers` | `4` |
| `-instances` | `200` |
| `-steps` | `3` |
| `-duration` | `0` (wait for all) |
| `-poll` | `20ms` |
| `-json` | off |

## Layout

| Path | Role |
|---|---|
| `bench/` | Runner, workload registration, `Result` struct (callable outside CLI) |
| `cmd/bench` | Flag parsing, backend selection, stdout / JSON output |

Root module imports `backend/postgres` via existing `go.work` (same pattern as `chaos/`).

## Output

Human-readable (default):

```
backend=memory workers=4 instances=200 steps=3
completed=200 failed=0 wall=1.234s throughput=162.07/s
```

`-json`: one JSON object with the same fields (`backend`, `workers`, `instances`, `steps`, `completed`, `failed`, `wall_seconds`, `throughput`).

## Acceptance

- `go run ./cmd/bench` (memory defaults) prints throughput and exits 0 when all complete.
- With `TASUKI_POSTGRES_DSN` set, `-backend=postgres` does the same after Migrate/Reset.
- Non-zero exit if Start fails broadly or if `failed > 0` when waiting for all completions (duration=0).
- No changes to public engine APIs required for this slice.
- Short README usage example for both backends.

## Out of scope follow-ups (document only)

- Multi-process worker mode
- Additional backends behind the same `-backend` switch
- Warmup iterations
- Latency histograms / OpenTelemetry export from the harness

## Implementation order (high level)

1. Spec (this doc) + implementation plan  
2. `bench` package + memory path in `cmd/bench`  
3. Postgres path + README snippet  
4. Smoke both locally; PR per task as usual with `[skip ci]`
