# Client and worker package design

## Goal

Organize the public API and implementation by responsibility under `github.com/hirokazumiyaji/tasuki/client` and `github.com/hirokazumiyaji/tasuki/worker`, leaving the module root without a Go package.

## Context and constraints

- The repository is a Go module at `github.com/hirokazumiyaji/tasuki`.
- The current root `tasuki` API is not yet in external use. This migration may break all root-package import paths and names.
- Do not keep compatibility wrappers or aliases in the module root.
- Keep backend interfaces, workflow execution semantics, persisted workflow data, and task names unchanged.
- Keep `workflow` independent from both `client` and `worker`.
- `client` and `worker` must not import each other.

## Package responsibilities

### `tasuki/client`

Own client construction and operations: `Client`, `Handle`, `NewClient`, `Start`, `Result`, the `Client.Signal`, `Client.SignalBatch`, and `Client.List` methods, filters and result/status types, client/start/signal options, and client-facing errors. The package may depend on backend, codec, journal, and other lower-level packages needed by those operations.

### `tasuki/worker`

Own Worker construction and lifecycle, polling and task execution, workflow/activity registration, Worker configuration, registration options, worker-bound `Query` and `Update`, schema validation, recovery interfaces, Worker-specific errors, and activity retry classification (`NonRetryable`, `IsNonRetryable`). It may depend on backend, codec, journal, workflow, observability, activity, and the internal engine. It must not depend on `client` or the module root.

### `tasuki/wftest`

Continue to provide test helpers by composing `client` for workflow start/result operations and `worker` for Worker construction and registration. Its public package-level API stays stable where it does not expose root-package types.

### Existing lower-level packages

Keep `backend`, `workflow`, `journal`, `codec`, `observability`, and `internal/engine` in their current packages. In particular, do not introduce a new shared `core` package: current Client and Worker dependencies already flow through backend and workflow types.

## API ownership after migration

Move Client APIs and options to `client`, including:

- `Client`, `Handle`, `NewClient`, `Start`, `Result`
- `InstanceFilter`, status constants, `SignalItem`, and Client list/signal methods
- `ClientOption`, `StartOption`, `SignalOption` and their constructors such as `WithCodec`, `WithID`, `WithQueue`, `WithSearchAttributes`, `WithMemo`, and `WithDedupeID`
- Client-facing errors such as `ErrAlreadyStarted`

Move Worker APIs and options to `worker`, including:

- `Worker`, `WorkerOptions`, `NewWorker`
- `RegisterWorkflow`, `RegisterActivity`, `RegisterOption`, `WithName`
- `Query`, `Update`, `UpdateOption`, `WithUpdateID`, and `ErrNotRunning`
- `ValidateSchema`, `TaskRecoverer`
- `ErrWorkflowNotRegistered`, `ErrActivityNotRegistered`, `ErrWorkerAlreadyRunning`, and `ErrWorkerShuttingDown`
- `NonRetryable` and `IsNonRetryable`

The module root contains no Go source package after the migration. Client and Worker options with the same conceptual name remain scoped to their owning packages, so callers qualify them through `client` or `worker`.

## Source and test organization

Create `client/` and `worker/` packages. Move Client implementation and Client-specific tests into `client/`; move Worker implementation and Worker-specific tests into `worker/`. Split the current mixed `options.go` and `errors.go` along these package responsibilities. Tests that exercise both Client and Worker use external test packages and import both packages. Keep unexported implementation tests beside their production package.

The existing behavior-named replacements for `worker_codex317_*` belong under `worker/`; preserve their test bodies and coverage while removing the round-number test-name prefixes already selected.

Update all Go consumers, including `wftest`, backend test helpers, examples, benchmarks, chaos commands, and `contrib/ui`, to import the relevant package explicitly. Update README files, docs, and code snippets to use `client` and `worker`. Update comments that name the removed `tasuki.ValidateSchema` path. Keep the metrics instrument name stable because it is an external observability identifier, not a Go package dependency.

## Migration shape

```go
import (
    "github.com/hirokazumiyaji/tasuki/client"
    "github.com/hirokazumiyaji/tasuki/worker"
)

w := worker.NewWorker(b, worker.WorkerOptions{})
worker.RegisterWorkflow(w, OrderWorkflow, worker.WithName("order"))

c := client.NewClient(b)
h, err := client.Start(ctx, c, "order", input)
```

Worker-bound query and update operations use `worker.Query` and `worker.Update`; workflow start and result retrieval use `client.Start` and `client.Result`.

## Verification criteria

- All Go packages compile with `go test ./...`; the module root is absent from the package list and no import cycle exists.
- Worker-specific tests move with Worker internals; Client tests move with Client; integration tests import both packages.
- All examples, docs, benchmarks, and helpers use the new import paths.
- No Go source or documentation snippet imports the module root as a Go package or refers to removed root API paths.
- `worker_codex317_*` names no longer exist; the behavior-named regression tests live under `worker/`.
- Persisted workflow data, task naming, backend behavior, and the observability meter name remain unchanged.

## Non-goals

- Renaming the module path or any existing lower-level package.
- Changing Client, Worker, backend, workflow, or persistence behavior.
- Adding a compatibility root package, aliases, wrappers, or a shared `core` package.
