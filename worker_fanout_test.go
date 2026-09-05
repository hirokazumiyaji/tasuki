package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// budgetBackend wraps memory with a small MaxAdvancementEffects and rejects
// over-budget advancements like DynamoDB, to verify worker-side chunking.
type budgetBackend struct {
	*memory.Backend
	budget int
}

func (b *budgetBackend) Capabilities() backend.Capabilities {
	return backend.Capabilities{MaxAdvancementEffects: b.budget}
}

func (b *budgetBackend) CommitAdvancement(ctx context.Context, adv backend.Advancement) error {
	if ops := testAdvOps(adv); ops > b.budget {
		return &testBudgetError{ops: ops, budget: b.budget}
	}
	return b.Backend.CommitAdvancement(ctx, adv)
}

func (b *budgetBackend) CommitAdvancements(ctx context.Context, advs []backend.Advancement) error {
	for _, adv := range advs {
		if ops := testAdvOps(adv); ops > b.budget {
			return &testBudgetError{ops: ops, budget: b.budget}
		}
	}
	return b.Backend.CommitAdvancements(ctx, advs)
}

type testBudgetError struct {
	ops, budget int
}

func (e *testBudgetError) Error() string {
	return "budget exceeded"
}

func testAdvOps(adv backend.Advancement) int {
	n := 2 + len(adv.NewEvents) + len(adv.ActivityTasks) + len(adv.Timers) + len(adv.DrainedInbox) + 3*len(adv.Children)
	if adv.ParentNotify != nil {
		n++
	}
	return n
}

func TestWorker_LargeFanoutWithinBudget(t *testing.T) {
	for _, n := range []int{50, 100} {
		t.Run("", func(t *testing.T) {
			ctx := context.Background()
			mem := memory.New()
			mem.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			b := &budgetBackend{Backend: mem, budget: 80}
			w := tasuki.NewWorker(b, tasuki.WorkerOptions{
				PollInterval:        time.Millisecond,
				ActivityConcurrency: 8,
				WorkflowConcurrency: 4,
			})
			tasuki.RegisterActivity(w, func(ctx context.Context, n int) (int, error) {
				return n + 1, nil
			}, tasuki.WithName("inc"))
			fanout := n
			tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, _ struct{}) (int, error) {
				futs := make([]*workflow.Future[int], 0, fanout)
				for i := 0; i < fanout; i++ {
					futs = append(futs, workflow.ExecuteAsync[int, int](wctx, "inc", 0))
				}
				awaitables := make([]workflow.Awaitable, 0, len(futs))
				for _, f := range futs {
					awaitables = append(awaitables, f)
				}
				if err := workflow.AwaitAll(wctx, awaitables...); err != nil {
					return 0, err
				}
				sum := 0
				for _, f := range futs {
					v, err := f.Get(wctx)
					if err != nil {
						return 0, err
					}
					sum += v
				}
				return sum, nil
			}, tasuki.WithName("FAN"))
			w.Start(ctx)
			defer w.Shutdown(ctx)
			c := tasuki.NewClient(b)
			h, err := tasuki.Start(ctx, c, "FAN", struct{}{}, tasuki.WithID("fanout-test"))
			if err != nil {
				t.Fatal(err)
			}
			// Use distinct IDs per subtest via client Start ID override.
			_ = h
			rctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			out, err := tasuki.Result[int](rctx, h)
			if err != nil {
				t.Fatalf("n=%d: %v", n, err)
			}
			if out != n {
				t.Fatalf("n=%d got %d", n, out)
			}
		})
	}
}
