# M5 SQL CommitAdvancements Plan

> Inline. `[skip ci]`. One PR per task.

**Spec:** [../specs/2026-07-25-m5-sql-commit-batch-design.md](../specs/2026-07-25-m5-sql-commit-batch-design.md)

### Task 1: Spec docs — PR

### Task 2: sqlite + mysql impl + tests — PR

```go
func (b *Backend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	return b.CommitAdvancements(ctx, []backend.Advancement{adv})
}
func (b *Backend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	if len(advs) == 0 { return nil }
	return withTx(ctx, b.db, func(conn *sql.Conn) error {
		for _, adv := range advs {
			if err := b.commitAdvancementConn(ctx, conn, adv); err != nil {
				return err
			}
		}
		return nil
	})
}
```

MySQL: after main txn, loop ensure second-pass per instance.
