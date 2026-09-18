# Modules, Versions, and Publishing

[English] | [日本語](ja/10-modules.md)

Module hierarchy, dependencies, versioning, and publishing workflow.

## Module Layout

| Module | Path | Target |
|---|---|---|
| root | `github.com/hirokazumiyaji/tasuki` | Core library (`workflow`, `activity`, `client`, `worker`, `memory`, `bench`, `contrib/ui`). Builds independently with `GOWORK=off` |
| dynamodb | `.../backend/dynamodb` | DynamoDB backend |
| firestore | `.../backend/firestore` | Firestore backend |
| mysql | `.../backend/mysql` | MySQL / MariaDB / TiDB backend |
| postgres | `.../backend/postgres` | PostgreSQL reference backend |
| spanner | `.../backend/spanner` | Cloud Spanner backend |
| sqlite | `.../backend/sqlite` | SQLite backend |

The root module does not depend on database drivers or cloud SDKs by default. `internal/backendopen` embeds only the in-memory backend by default; other backends are registered via `-tags tasuki_all` builds (within the multi-module workspace). Non-memory examples (`m1`, `m3`, `m4`) and `chaos/cmd/worker` are tagged with `tasuki_all`, allowing standard `GOWORK=off go list ./...` to build the root module cleanly.

## Minimum Go Version

- **Declaration**: Root `go 1.24`, backend modules `go 1.24`, `go.work` declares `go 1.24` (aligned with CI `1.24.x`).
- **Validation**: `GOTOOLCHAIN=local go build ./...` (verified outside workspace in a Go 1.24 environment).

## Dependency Versioning

- Indirect dependencies of the root module (`x/sync`, `x/mod`, etc.) are explicitly pinned in `go.mod` / `go.sum` without being shadowed by workspace replacements. The CI `gowork-check` job verifies this using `GOWORK=off GOPROXY=off go list ./...`.
- Database drivers and cloud SDKs (AWS, Google Cloud, pgx, mysql) remain confined within their respective `backend/<name>/go.mod` files and are never leaked into the root module.

## Versioning and Publishing Workflow

1. The root module and backend submodules are released with independent semantic version tags (e.g. `v0.2.0` for root, `backend/dynamodb/v0.2.0` for DynamoDB).
2. The `replace github.com/hirokazumiyaji/tasuki => ../..` directive in backend `go.mod` files is used for local workspace development. Before publishing a backend tag, ensure the target root version is published and update the backend `require` directive to `github.com/hirokazumiyaji/tasuki vX.Y.Z` if necessary.
3. Pre-release verification checklist:
   ```bash
   # Verify root independent build
   GOWORK=off GOPROXY=off go list ./...
   GOWORK=off go build ./...
   GOTOOLCHAIN=local go build ./...

   # Verify workspace build (with all backends)
   go build -tags tasuki_all ./...

   # Consumer smoke test (requires published tags)
   mkdir /tmp/smoke && cd /tmp/smoke && go mod init smoke
   go get github.com/hirokazumiyaji/tasuki@vX.Y.Z
   go get github.com/hirokazumiyaji/tasuki/backend/sqlite@vX.Y.Z # Optional
   ```
4. CLI tools (`cmd/bench`, `contrib/ui/cmd/tasuki-ui`) are packaged in the root module. Building CLI tools with non-memory backend support requires `-tags tasuki_all` within the workspace.

## Continuous Integration (CI)

- **`root` Job**: `go test ./... -race` across the workspace (fuzz seed corpora run here as unit tests).
- **`gowork-check` Job**: Validates root module independence via `GOWORK=off GOPROXY=off go list ./...` and `GOTOOLCHAIN=local go build ./...`; also runs `go test -tags tasuki_all ./contrib/... ./examples/...`.
- **Backend Jobs**: Executes `go test ./... -race` in each individual `backend/<name>` directory.
- **Chaos Jobs**: Executes the `kill -9` chaos suites with `-race`, including the zero-infrastructure `chaos-sqlite` job.
- **`lint` / `vuln` Jobs**: `staticcheck` (hard gate) and advisory `govulncheck` over the root and backend modules.
- **`fuzz-nightly` Job**: Runs only on the nightly schedule (or manual dispatch): 60s of `-fuzz -fuzztime` per codec/engine target.
