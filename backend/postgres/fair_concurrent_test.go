package postgres_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/journal"
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

// TestFairClaimRevisitsRejectedWhenPickedLocksLost covers the issue #294
// follow-up: FIFO A1,A2,B1 with Limit=2 and MaxPerInstance=1 picks A1,B1 and
// rejects A2. If a concurrent claimer locks both picks before this claim's
// lock step, the refill must revisit the rejected A2 instead of resuming
// after B1 and returning empty.
func TestFairClaimRevisitsRejectedWhenPickedLocksLost(t *testing.T) {
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

	const queue = "rej"
	spawn := func(id string, activities int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "rej",
		})
		if err != nil || len(wf) != 1 {
			t.Fatalf("claim wf %s: %v %#v", id, err, wf)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		adv := backend.Advancement{InstanceID: id, TaskID: wf[0].ID, ExpectedSeq: st.NextSeq}
		for i := 0; i < activities; i++ {
			seq := st.NextSeq + int64(i)
			adv.NewEvents = append(adv.NewEvents, journal.Event{
				Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
			})
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind: "activity", Queue: queue, InstanceID: id, Name: "step",
				Seq: seq, Input: []byte(`{}`),
			})
		}
		if err := b.CommitAdvancement(ctx, adv); err != nil {
			t.Fatal(err)
		}
	}
	// FIFO order must be A1,A2,B1: flood instance enqueues two activities
	// before the victim enqueues one.
	spawn("rej-A", 2)
	spawn("rej-B", 1)

	rows, err := b.Pool().Query(ctx, `
		SELECT id, instance_id FROM wf_tasks
		WHERE kind = 'activity' AND queue = $1
		ORDER BY visible_at, id`, queue)
	if err != nil {
		t.Fatal(err)
	}
	type cand struct {
		id  int64
		ins string
	}
	var cands []cand
	for rows.Next() {
		var c cand
		if err := rows.Scan(&c.id, &c.ins); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		cands = append(cands, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(cands) != 3 || cands[0].ins != "rej-A" || cands[1].ins != "rej-A" || cands[2].ins != "rej-B" {
		t.Fatalf("FIFO order = %#v, want [rej-A rej-A rej-B]", cands)
	}

	// Blocker locks the two rows the picker accepts (A1,B1), like a
	// concurrent fair claimer that scanned the same IDs first.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = ANY($1) FOR UPDATE`, []int64{cands[0].id, cands[2].id}); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{queue}, Limit: 2,
		Lease: time.Minute, WorkerID: "rej-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) == 0 {
		t.Fatalf("claim returned empty despite unlocked rejected A2 (%d)", cands[1].id)
	}
	blocked := map[int64]bool{cands[0].id: true, cands[2].id: true}
	for _, task := range tasks {
		if blocked[task.ID] {
			t.Fatalf("claimed locked picked task %d", task.ID)
		}
	}
	found := false
	for _, task := range tasks {
		if task.ID == cands[1].id {
			found = true
		}
	}
	if !found {
		t.Fatalf("claim = %v, want rejected A2 %d to be revisited", tasks, cands[1].id)
	}
}
