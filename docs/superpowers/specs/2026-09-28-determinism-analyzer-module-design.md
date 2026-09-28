# Determinism analyzer module separation

## Goal

Keep `golang.org/x/tools` out of the core Tasuki module's dependency graph while preserving the determinism analyzer, its import path, its command, and its test coverage.

## Current behavior

The analyzer implementation, tests, and command live under `analyzers/` but are currently part of the root Go module. The root `go.mod` therefore requires `golang.org/x/tools`, even though core runtime packages do not import it. The README commands run the analyzer from the repository root, and CI's root `go test ./...` currently includes analyzer tests.

## Design

Create a nested Go module rooted at `analyzers/` with module path `github.com/hirokazumiyaji/tasuki/analyzers`. Keep the existing package and command paths beneath that directory, so the analyzer package import path remains `github.com/hirokazumiyaji/tasuki/analyzers/determinism`. Put `golang.org/x/tools` and its analyzer-only module requirements in the nested module's `go.mod` and `go.sum`; remove those requirements from the root module through module tidying.

Add `./analyzers` to the repository `go.work` so workspace users can run and develop the analyzer alongside Tasuki. Update English and Japanese README instructions to run the command from the analyzer module while targeting the parent repository's packages. Add explicit analyzer-module test, vet, lint, and tidy checks to CI because root-module wildcard commands will no longer include nested modules.

The analyzer's diagnostics and detection rules remain unchanged. No logger abstraction, cron parser, or other runtime dependency changes are part of this work.

## Alternatives considered

1. Keep the analyzer in the root module. This keeps current commands and test discovery intact, but leaves `x/tools` in every root-module dependency graph.
2. Move the analyzer to a separate repository or installable binary. This separates dependencies further, but changes the distribution and maintenance model beyond the stated goal.
3. Add a nested module under `analyzers/`. This removes analyzer-only dependencies from core users while preserving source organization and import paths, with small README, workspace, and CI updates. This is the selected approach.

## Verification

Run analyzer tests and vet with `GOWORK=off` from `analyzers/` to prove it is independently buildable. Run analyzer lint in CI. Verify the root module no longer lists `golang.org/x/tools`, the nested module does, and the documented workspace command analyzes the root packages. Run the existing root and backend CI checks to detect workspace regressions.
