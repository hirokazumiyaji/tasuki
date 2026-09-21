package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
)

// TestConcurrentFairClaim hammers the fair claim path (ClaimRequest.
// MaxPerInstance) from N workers at once (issue #294): with FOR UPDATE SKIP
// LOCKED on the candidate SELECT plus the visibility re-check at UPDATE time,
// concurrent claimants must neither block each other into empty batches nor
// hand one task to two workers.
func TestConcurrentFairClaim(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const (
		instances = 12
		workers   = 4
		limit     = 4
		maxPer    = 2
	)
	for i := 0; i < instances; i++ {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: fmt.Sprintf("fcc-%d", i), Name: "WF", Queue: "fcc",
		}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu     sync.Mutex
		seen   = map[int64]string{} // taskID -> worker
		errors = []error{}
	)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := fmt.Sprintf("fcc-w%d", w)
			<-start
			for i := 0; i < 4*instances; i++ {
				tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
					Kind: "workflow", Queues: []string{"fcc"}, Limit: limit,
					Lease: time.Minute, WorkerID: worker, MaxPerInstance: maxPer,
				})
				if err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
					return
				}
				mu.Lock()
				if len(tasks) > 0 {
					perInst := map[string]int{}
					for _, task := range tasks {
						if prev, dup := seen[task.ID]; dup {
							errors = append(errors, fmt.Errorf("task %d claimed by %s and %s", task.ID, prev, worker))
						}
						seen[task.ID] = worker
						perInst[task.InstanceID]++
					}
					for inst, n := range perInst {
						if n > maxPer {
							errors = append(errors, fmt.Errorf("batch has %d tasks of %s, cap %d", n, inst, maxPer))
						}
					}
				}
				done := len(seen) >= instances
				mu.Unlock()
				if done {
					return
				}
			}
		}(w)
	}
	close(start)
	wg.Wait()

	if len(errors) > 0 {
		t.Fatalf("concurrent fair claim errors: %v", errors)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seen) != instances {
		t.Fatalf("claimed %d distinct tasks, want %d", len(seen), instances)
	}
}

// TestFairClaimRefillsPastLockedHead covers the issue #294 P1: when a
// concurrent claimer holds locks on every head row the picker accepts, the
// claim must refill from later candidates instead of returning an empty
// batch while claimable tasks remain.
func TestFairClaimRefillsPastLockedHead(t *testing.T) {
	dsn := dsnOrSkip(t)
	ctx := context.Background()
	b, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const instances = 4
	for i := 0; i < instances; i++ {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: fmt.Sprintf("refill-%d", i), Name: "WF", Queue: "refill",
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Blocker holds locks on the two head rows, like a concurrent fair
	// claimer that scanned the same IDs and locked them first.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	headRows, err := btx.Query(ctx, `
		SELECT id FROM wf_tasks
		WHERE kind = 'workflow' AND queue = 'refill'
		ORDER BY visible_at, id
		LIMIT 2
		FOR UPDATE`)
	if err != nil {
		t.Fatal(err)
	}
	head := map[int64]bool{}
	for headRows.Next() {
		var id int64
		if err := headRows.Scan(&id); err != nil {
			headRows.Close()
			t.Fatal(err)
		}
		head[id] = true
	}
	headRows.Close()
	if err := headRows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(head) != 2 {
		t.Fatalf("blocked %d head rows, want 2", len(head))
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"refill"}, Limit: 2,
		Lease: time.Minute, WorkerID: "refill-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("claimed %d tasks behind locked head rows, want 2", len(tasks))
	}
	for _, task := range tasks {
		if head[task.ID] {
			t.Fatalf("claimed locked head task %d", task.ID)
		}
	}
}
