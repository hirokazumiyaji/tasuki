# Quality Sprint Design

**Date:** 2026-07-29  
**Status:** Approved  
**Parent:** [docs/04-plan.md](../../04-plan.md) post-M6 quality  
**Decisions:** Approach = vertical tracks (docs → CI matrix → cover report → coverage fill → bench baseline memo → spec checkbox cleanup). Coverage = report-only (no fail gate). Scope = continuous sprint (option C).

## Goal

Raise post-M6 quality across documentation freshness, CI breadth, measurement, targeted tests, and design-meta hygiene—without adding features or coverage failure gates.

## Non-goals

- Failing CI on coverage thresholds
- New public APIs or backend capabilities
- Always-on multi-backend bench in CI
- Large refactors unrelated to testability
- Changing runtime semantics

## Tracks and PR order

Keep existing workflow: one task → one commit → one PR; subjects include `[skip ci]` except when the change itself must exercise CI (CI workflow PRs run normally).

| # | Track | Deliverable | Est. PRs |
|---|---|---|---|
| 1 | Docs freshness | README status reflects M4 done + M5/M6 shipped; `04-plan.md` “next actions” → quality sprint | 1 |
| 2 | CI matrix | Split/matrix jobs: root (+race), `backend/{sqlite,postgres,mysql,spanner,dynamodb,firestore}`; services/env match `docker-compose.yml`. Chaos: postgres remains; other-store chaos jobs with env set (no silent skip). TiDB optional later | 1–2 |
| 3 | Cover report | Root `-coverprofile`; Job Summary via `go tool cover -func`; upload `cover.out` artifact. **Never fail on %**. Exclude noise: examples, `cmd/*`, `backendtest` from summary emphasis | 1 |
| 4 | Coverage fill | Priority: `internal/engine` → `observability` → `internal/backendopen` → `backend` helpers (0% funcs) → thin `workflow` branches. Package-scoped PRs; note before/after in PR body | many |
| 5 | Bench baseline | Document memory repro flags + one captured result memo under `docs/` (no SLO gate). Postgres = manual steps only | 1 |
| 6 | Spec checkbox cleanup | Mark implemented design acceptance boxes `[x]`; do not touch unimplemented items. Batch by file groups | many |

**Dependencies:** Track 1 independent. 2 then 3. Track 4 may re-prioritize using cover numbers from 3. Tracks 5 and 6 can run in parallel with 4.

## CI details

### Backend jobs

For each store module under `backend/<name>`:

- `go test ./... -count=1 -timeout 120s` in that module directory
- Provide the same env vars / emulator hosts as local examples and existing skip helpers
- SQLite needs no service container

Images/env should align with `docker-compose.yml` (postgres:17, mysql:8.4, Spanner emulator, DynamoDB Local, Firestore emulator).

### Chaos

- Keep current postgres chaos job
- Add (or enable) chaos jobs for mysql / spanner / dynamodb / firestore with required env always set in CI (tests must not skip for missing DSN)
- Longer timeout than unit jobs; failures block merge
- TiDB chaos deferred unless added in a late optional PR

### Root

- Preserve `go test ./... -race -count=1` on the main module
- Cover profile job may be separate from `-race` for speed/stability

## Cover report

- Produce `cover.out` from root module tests
- Print package list + total to GitHub Actions Job Summary
- Upload artifact `cover.out`
- No percentage threshold in the workflow
- Examples / cmd / backendtest remain low-signal; summary may filter or footnote them

## Test fill strategy

- Prefer existing styles: table tests, memory integration, public API paths
- `internal/engine`: uncovered branches (update continuation, stuck, timer edges) via `run_test`-style unit tests; keep fuzz as-is
- `observability`: nil-safe helpers and each counter/gauge path with default (noop) meter
- `backendopen` / `backend` helpers: pure functions and error branches
- Local verification: package-scoped `go test` with `-race` and `-timeout`; avoid hanging on full `./...` while editing

## Bench baseline

- Memory: fixed flags (e.g. instances/workers/steps/poll matching README defaults or a documented subset)
- Store one human-readable result memo (date, machine class optional, throughput + secondaries)
- No CI regression budget in this sprint
- Postgres: document manual command only

## Spec checkbox cleanup

- Only mark boxes for work already merged
- Leave unchecked any still-open acceptance items
- Prefer grouping by milestone/theme to keep PRs reviewable

## Acceptance criteria

- [ ] README and `04-plan.md` match current product status and point next work at this quality sprint
- [ ] CI runs `go test` for sqlite, postgres, mysql, spanner, dynamodb, and firestore backends every push/PR
- [ ] Cover profile appears in Job Summary and as an artifact; low coverage never fails the job
- [ ] Priority packages receive intentional new tests (perfect % not required; PRs note before/after)
- [ ] Docs include a reproducible memory bench procedure and one captured result memo
- [ ] Implemented design specs have acceptance checkboxes marked done; unimplemented ones untouched

## Success signals (non-gating)

- Root statement coverage rises meaningfully vs sprint start (~54% total including noise packages)
- Fewer “fails only locally / only on one store” regressions
- Docs no longer contradict shipped M5/M6 features when starting release prep
