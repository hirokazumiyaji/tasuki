# M5 Contrib UI CLI sqlite + mysql Implementation Plan

> Prefer **inline execution**. One commit + one PR per task; merge to `main` before the next. `[skip ci]` on commits/merges.
>
> Merge: `gh pr merge --merge --subject "Merge pull request #N from branch [skip ci]"`

**Goal:** Add sqlite and mysql to `tasuki-ui` CLI.

**Spec:** [docs/superpowers/specs/2026-07-25-m5-contrib-ui-cli-sqlite-mysql-design.md](../specs/2026-07-25-m5-contrib-ui-cli-sqlite-mysql-design.md)

## Global Constraints

- Migrate on start; no Reset; env config; handler unchanged; `[skip ci]`

---

### Task 1: Spec + Plan

- [ ] Docs + todo; PR; merge

### Task 2: openBackend + tests + README

**Files:** `contrib/ui/cmd/tasuki-ui/main.go`, `main_test.go`, `README.md`

- [ ] Cases `sqlite` / `mysql` with `TASUKI_SQLITE_PATH` / `TASUKI_MYSQL_DSN`
- [ ] `TestOpenBackend_Unknown` and `TestOpenBackend_SQLiteTemp`
- [ ] README examples
- [ ] PR; merge

```go
case "sqlite":
	path := os.Getenv("TASUKI_SQLITE_PATH")
	if path == "" {
		return nil, nil, fmt.Errorf("TASUKI_SQLITE_PATH required")
	}
	sb, err := sqlite.New(path)
	// Migrate, return sb, sb.Close
case "mysql":
	dsn := os.Getenv("TASUKI_MYSQL_DSN")
	// mysql.New + Migrate
```
