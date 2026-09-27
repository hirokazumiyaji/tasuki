package spanner

import "testing"

// Chunked purge must keep every commit far below Cloud Spanner's 20,000
// mutations-per-commit limit no matter how many rows an instance holds
// (Codex round-16 on #291): the old single-transaction purge buffered one
// delete mutation per child row of every selected instance, so big batches
// exceeded the limit and failed on every retry. The per-table page and the
// shared transaction budget pin that invariant; raising either past the
// limit must fail here first.
func TestPurgeBatchBudgetBounds(t *testing.T) {
	const spannerMutationLimit = 20000
	if purgeChildBatchBudget <= 0 || purgeChildBatchPage <= 0 {
		t.Fatalf("purge budgets must be positive, got budget=%d page=%d", purgeChildBatchBudget, purgeChildBatchPage)
	}
	if purgeChildBatchPage > purgeChildBatchBudget {
		t.Fatalf("page %d exceeds shared budget %d: one table could consume the whole transaction", purgeChildBatchPage, purgeChildBatchBudget)
	}
	// One purge transaction buffers at most the shared budget (helpers take
	// purgePageLimit(remaining) each, and remaining only shrinks), plus the
	// parent/seq delete commits separately (2 mutations). Keep an order of
	// magnitude of headroom below the service limit.
	if purgeChildBatchBudget > spannerMutationLimit/10 {
		t.Fatalf("purge budget %d leaves no headroom below the %d-mutation commit limit", purgeChildBatchBudget, spannerMutationLimit)
	}
}

func TestPurgePageLimit(t *testing.T) {
	if got := purgePageLimit(1000); got != purgeChildBatchPage {
		t.Fatalf("purgePageLimit(1000) = %d, want page %d", got, purgeChildBatchPage)
	}
	if got := purgePageLimit(200); got != 200 {
		t.Fatalf("purgePageLimit(200) = %d, want 200 (remaining budget binds)", got)
	}
	if got := purgePageLimit(0); got != 0 {
		t.Fatalf("purgePageLimit(0) = %d, want 0 (budget exhausted: LIMIT 0 takes nothing)", got)
	}
}
