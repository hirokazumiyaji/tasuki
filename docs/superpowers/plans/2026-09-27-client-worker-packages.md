# Client and worker package migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the public Client and Worker APIs into `client` and `worker` packages and remove the Go package from the module root.

**Architecture:** `client` and `worker` are sibling public packages that depend on lower-level repository packages and do not import each other. Move implementation, package-private tests, and API-specific options/errors together, then migrate consumers and documentation.

**Tech Stack:** Go, existing module packages, Markdown docs.

**Spec:** `docs/superpowers/specs/2026-09-27-client-worker-packages-design.md`

## Global Constraints

- The current root `tasuki` API is not yet in external use. This migration may break all root-package import paths and names.
- Do not keep compatibility wrappers or aliases in the module root.
- Keep backend interfaces, workflow execution semantics, persisted workflow data, and task names unchanged.
- Keep `workflow` independent from both `client` and `worker`.
- `client` and `worker` must not import each other.

## Review Focus

- Client option names, status values, and errors remain available in `client`; move and run Client behavior tests such as start, signal, listing, memo, and result continuation.
- Worker registration names and options remain available in `worker`; move and run `registry_name_test.go` and the Worker schema tests.
- Non-retryable activity errors remain recognized after ownership moves; move and run `errors_test.go` and `worker_retry_test.go`.
- Test helpers compose both new packages correctly; run `wftest`, `backendtest`, and Worker/Client integration tests.
- Removing the root package leaves no stale import or API reference; run full package tests, `go list ./...`, and source-reference scans.

---

### Task 1: Move Client and Worker APIs into responsibility packages

**Files:**
- Create: `client/client.go`, `client/options.go`, and `client/errors.go` from the current root Client implementation and Client-owned declarations.
- Create: `worker/worker.go`, `worker/worker_sticky.go`, `worker/worker_instance_lock.go`, `worker/registry.go`, `worker/query.go`, `worker/update.go`, `worker/options.go`, and `worker/errors.go` from the current Worker implementation and Worker-owned declarations.
- Move: Client-specific root tests into `client/` and Worker-specific root tests, including the 25 behavior-named regression files, into `worker/`.
- Modify: integration tests and Go consumers in `wftest`, `backendtest`, `bench`, `examples`, `chaos`, and `contrib` to import `client` and `worker` as needed.
- Delete: old root Go source and test files after their contents have moved; leave the module root without `.go` files.

**Interfaces:**
- Produces: `tasuki/client` owns `Client`, `Handle`, client operations/options/errors; `tasuki/worker` owns `Worker`, registration, query/update, Worker options/errors, validation, recovery, and retry classification.
- Consumes: existing lower-level package APIs; neither new package imports the module root or the sibling package.

- [x] Create `client/` and `worker/`; move each production file to its owning package and split mixed option/error declarations by responsibility.
- [x] Move Client and Worker tests beside their implementation, updating package clauses and imports while preserving test assertions.
- [x] Migrate every Go source consumer and integration test to explicit `client` and/or `worker` imports.
- [x] Remove obsolete root Go files after confirming their declarations and tests have moved.

Expected: Go sources use the new package paths only; root contains no `.go` files; the new packages have no dependency cycle.

### Task 2: Migrate documentation and usage examples

**Files:**
- Modify: `README.md`, `README.ja.md`, and `docs/**/*.md` prose and snippets that use the old root API. Go source comments are updated with their source files in Task 1.
- Preserve: `observability.MeterName` as a stable metrics identifier.

**Interfaces:**
- Consumes: the `client` and `worker` API paths from Task 1.
- Produces: examples and documentation that consistently qualify Client APIs through `client` and Worker APIs through `worker`.

- [x] Update snippets and prose for moved APIs, including `ValidateSchema`, `WithName`, `NonRetryable`, `Query`, and `Update`.
- [x] Search Markdown and Go sources for old root-package imports or moved API paths; update every actual usage.
- [x] Review English and Japanese docs for consistent imports and API names.

Expected: no documentation example imports the module root as a Go package or refers to a moved API through that root path.

### Task 3: Verify the package boundary and full repository

**Files:**
- Verify: changed packages, module root, docs, examples, and complete diff.

**Interfaces:**
- Consumes: completed source and documentation migration.
- Produces: evidence that package ownership, import direction, and existing behavior are preserved.

- [x] Run `go test ./...`.
- [x] Run `go list ./...`; confirm the module root is absent and `client` and `worker` are listed.
- [ ] Search Go and Markdown files for `worker_codex317`, old root imports, and stale root API paths; distinguish prose/module branding from API references.
- [x] Review the diff to confirm tests retain their assertions and runtime/persistence code has no unrelated edits.

Expected: all tests pass, the root package is absent, both new packages are present, and scans find no stale API references.
