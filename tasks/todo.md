# Root file cleanup plan

Goal: Make the root-level Worker regression tests discoverable by the behavior they cover while retaining tests that protect current worker behavior.

## Plan

- [x] Inventory `worker_codex317_*` test files, their test names, and repository references.
- [x] Map each file and its `RoundN` test names to stable behavioral names; retain all regression coverage.
- [x] Rename the files and test functions, then review the diff for accidental content changes.
- [x] Record review notes and verification limits.

## Review

All 25 `worker_codex317_roundN_test.go` files contain active Worker regression coverage and remain in the root package. Renamed by behavior and removed round-number prefixes from test function names. Compared every resulting file with its Git version before rename; contents match apart from those function-name prefixes. Confirmed the 177 root test names are unique and found no remaining `worker_codex317` references. Tests were not run because this change only renames test files and names.


# Client and worker package migration

- [x] Explore current Worker API, package dependencies, and external call sites.
- [x] Draft the package-boundary design for review (revised to include `client/`).
- [x] Review and approve the written design at `docs/superpowers/specs/2026-09-27-client-worker-packages-design.md`.
- [x] Write the implementation plan after design approval.
- [x] Task 1: Move Client and Worker implementations, APIs, tests, and Go consumers into `client/` and `worker/`; remove the root Go package.
- [x] Task 2: Migrate README and Markdown documentation to the new imports.
- [x] Task 3: Verify the module and review the final diff.

Design self-review: the spec covers current public Client and Worker API ownership, the root package removal, dependency direction, test relocation, and repository call-site migration. The package graph avoids a Client/Worker cycle and does not add a shared package without need. The user approved the revised design; the implementation plan is now written and ready for review.

Implementation plan created: `docs/superpowers/plans/2026-09-27-client-worker-packages.md`. Next: review plan and select execution approach before implementation.
Plan self-review: the approved spec maps to the three plan tasks: move all source/tests and Go call sites, migrate docs, then verify the package graph and complete suite. The new package API names are consistent with the spec. The source migration is intentionally one cohesive task because splitting Client and Worker would leave root-package imports and mixed integration tests temporarily unresolved. Review focus maps to existing Client, registry/schema, retry, helper, and full-package checks.

Execution method: Subagent-Driven. Worktree setup and baseline are complete.

## Review

Client and Worker APIs now live in `client/` and `worker/`; the module root contains no Go files. English/Japanese API docs use the new packages. Root-module tests and the six separate backend-module test suites pass. The code diff preserves implementation behavior and persisted formats; independent Task 1 and Task 2 reviews passed after one documentation fix. Final whole-branch review is pending.
