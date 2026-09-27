package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/sqlite"
)

// TestConcurrentFairClaim hammers ClaimTasks with MaxPerInstance from N
// workers at once (issue #294: concurrent fair claim must not return empty
// batches while tasks remain, lose tasks, or hand one task to two workers).
//
// SQLite serializes writers, so this exercises the fair candidate paging +
// visibility re-check under contention rather than row-level SKIP LOCKED,
// which only exists on postgres/mysql.
func TestConcurrentFairClaim(t *testing.T) {
	ctx := context.Background()
	b, err := sqlite.New(filepath.Join(t.TempDir(), "fair_concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	const (
		instances = 8
		workers   = 4
		limit     = 4
		maxPer    = 2
	)
	for i := 0; i < instances; i++ {
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: fmt.Sprintf("conc-%d", i), Name: "WF", Queue: "conc",
		}); err != nil {
			t.Fatal(err)
		}
	}

	var (
		mu     sync.Mutex
		seen   = map[int64]string{} // taskID -> worker
		empty  = map[string]int{}   // worker -> empty batches
		errors = []error{}
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			worker := fmt.Sprintf("conc-w%d", w)
			for i := 0; i < 2*instances; i++ {
				tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
					Kind: "workflow", Queues: []string{"conc"}, Limit: limit,
					Lease: time.Minute, WorkerID: worker, MaxPerInstance: maxPer,
				})
				if err != nil {
					mu.Lock()
					errors = append(errors, err)
					mu.Unlock()
					return
				}
				mu.Lock()
				if len(tasks) == 0 {
					empty[worker]++
					if len(seen) >= instances {
						mu.Unlock()
						return
					}
					mu.Unlock()
					time.Sleep(time.Millisecond)
					continue
				}
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
				done := len(seen) >= instances
				mu.Unlock()
				if done {
					return
				}
			}
		}(w)
	}
	wg.Wait()

	if len(errors) > 0 {
		t.Fatalf("concurrent fair claim errors: %v", errors)
	}
	if len(seen) != instances {
		t.Fatalf("claimed %d distinct tasks, want %d (empty batches: %v)", len(seen), instances, empty)
	}
}
