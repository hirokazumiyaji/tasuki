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

// TestFairClaimRefillPreservesFIFOOrder covers the issue #294 round-11 P2:
// FIFO A1,A2,B1 with Limit=2 and MaxPerInstance=1 picks A1,B1 and rejects
// A2. A concurrent claimer locks only A1, so pass 1 secures B1 and the
// refill secures the revisited A2. The batch must come back in FIFO (scan)
// order [A2 B1], not lock order [B1 A2] (docs/08-fair-dispatch.md: the
// MaxPerInstance batch preserves FIFO ordering).
func TestFairClaimRefillPreservesFIFOOrder(t *testing.T) {
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

	const queue = "fifo"
	spawn := func(id string, activities int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "fifo",
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
	spawn("fifo-A", 2)
	spawn("fifo-B", 1)

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
	if len(cands) != 3 || cands[0].ins != "fifo-A" || cands[1].ins != "fifo-A" || cands[2].ins != "fifo-B" {
		t.Fatalf("FIFO order = %#v, want [fifo-A fifo-A fifo-B]", cands)
	}

	// Blocker locks only the first pick (A1), like a concurrent fair
	// claimer that scanned the same IDs first. Pass 1 secures B1; the
	// refill must revisit A2 and return the batch in FIFO order [A2 B1].
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = $1 FOR UPDATE`, cands[0].id); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{queue}, Limit: 2,
		Lease: time.Minute, WorkerID: "fifo-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("claimed %d tasks, want 2 (A2 plus B1)", len(tasks))
	}
	if tasks[0].ID != cands[1].id || tasks[1].ID != cands[2].id {
		t.Fatalf("claim order = [%d %d], want FIFO [%d %d] (A2 B1)",
			tasks[0].ID, tasks[1].ID, cands[1].id, cands[2].id)
	}
}
// TestFairClaimPreservesUnvisitedTail covers the issue #294 P1 follow-up:
// FIFO A1,A2,A3,B1 with Limit=2 and MaxPerInstance=1 picks A1,B1 and rejects
// A2,A3 on the first pass. A1 and A2 are locked by a concurrent claimer, so
// the first pass claims only B1 and the refill pass picks the rejected A2
// but loses it to the lock while A3 sits unvisited behind it (the pending
// offer loop breaks once the batch fills). The unvisited A3 must be carried
// forward alongside the pass's rejected rows; dropping it underfills the
// batch with just B1 despite A3 being unlocked and claimable.
func TestFairClaimPreservesUnvisitedTail(t *testing.T) {
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

	const queue = "tail"
	spawn := func(id string, activities int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "tail",
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
	// FIFO order must be A1,A2,A3,B1: the flood instance enqueues three
	// activities before the victim enqueues one.
	spawn("tail-A", 3)
	spawn("tail-B", 1)

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
	if len(cands) != 4 || cands[0].ins != "tail-A" || cands[1].ins != "tail-A" ||
		cands[2].ins != "tail-A" || cands[3].ins != "tail-B" {
		t.Fatalf("FIFO order = %#v, want [tail-A tail-A tail-A tail-B]", cands)
	}

	// Blocker locks A1 (the first pick) and A2 (the refill pick), like a
	// concurrent fair claimer that scanned the same IDs first. A3 stays
	// unlocked but unvisited behind A2 in the pending carry.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = ANY($1) FOR UPDATE`, []int64{cands[0].id, cands[1].id}); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{queue}, Limit: 2,
		Lease: time.Minute, WorkerID: "tail-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Fatalf("claimed %d tasks, want 2 (B1 plus unvisited A3)", len(tasks))
	}
	blocked := map[int64]bool{cands[0].id: true, cands[1].id: true}
	for _, task := range tasks {
		if blocked[task.ID] {
			t.Fatalf("claimed locked task %d", task.ID)
		}
	}
	want := map[int64]bool{cands[2].id: true, cands[3].id: true}
	for _, task := range tasks {
		delete(want, task.ID)
	}
	if len(want) != 0 {
		t.Fatalf("claim = %v, want unvisited A3 %d and B1 %d", tasks, cands[2].id, cands[3].id)
	}
}

// TestFairClaimRequeriesOverflowAfterCarrySuccess covers the issue #294
// round-14 P2 at the live-DB level: Limit=4, MaxPerInstance=1 over FIFO
// A1,C1,B1,C2,B2..B2001,A2 with A1 and C1 locked by a concurrent claimer.
// Pass 1 secures B1 and drops A2 past FairRejectedCap; the carry pass then
// secures C2 with no current loss (lost==0). Gating the scan-exhausted
// requery on the current pass alone breaks there and returns [B1 C2];
// gating on the cross-pass lostLock flag requeries from the pre-overflow
// snapshot and recovers A2 for a third slot.
func TestFairClaimRequeriesOverflowAfterCarrySuccess(t *testing.T) {
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

	const qa, qb, qc = "ovq-a", "ovq-b", "ovq-c"
	claimWF := func(id, queue string) backend.Task {
		t.Helper()
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "ov",
		})
		if err != nil || len(wf) != 1 || wf[0].InstanceID != id {
			t.Fatalf("claim wf %s: %v %#v", id, err, wf)
		}
		return wf[0]
	}
	commitActs := func(id, queue string, task backend.Task, inbox []backend.InboxEvent, n int) {
		t.Helper()
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		adv := backend.Advancement{InstanceID: id, TaskID: task.ID, ExpectedSeq: st.NextSeq}
		seq := st.NextSeq
		for _, item := range inbox {
			ev := item.Event
			ev.Seq = seq
			seq++
			adv.NewEvents = append(adv.NewEvents, ev)
			adv.DrainedInbox = append(adv.DrainedInbox, item.ID)
		}
		for i := 0; i < n; i++ {
			adv.NewEvents = append(adv.NewEvents, journal.Event{
				Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
			})
			adv.ActivityTasks = append(adv.ActivityTasks, backend.NewTask{
				Kind: "activity", Queue: queue, InstanceID: id, Name: "step",
				Seq: seq, Input: []byte(`{}`),
			})
			seq++
		}
		if err := b.CommitAdvancement(ctx, adv); err != nil {
			t.Fatal(err)
		}
	}
	spawn := func(id, queue string, n int) {
		t.Helper()
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: queue}); err != nil {
			t.Fatal(err)
		}
		commitActs(id, queue, claimWF(id, queue), nil, n)
	}
	appendActs := func(id, queue string, n int) {
		t.Helper()
		sig := journal.Event{Type: journal.TypeSignalReceived, Name: "more", Payload: []byte(`{}`)}
		if err := b.SendToInbox(ctx, id, sig, ""); err != nil {
			t.Fatal(err)
		}
		task := claimWF(id, queue)
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(st.Inbox) != 1 {
			t.Fatalf("%s inbox=%d want 1", id, len(st.Inbox))
		}
		commitActs(id, queue, task, st.Inbox, n)
	}
	// FIFO creation order: A1, C1, B1, C2, B2..B2001, A2.
	spawn("ov-A", qa, 1)
	spawn("ov-C", qc, 1)
	spawn("ov-B", qb, 1)
	appendActs("ov-C", qc, 1)
	appendActs("ov-B", qb, 2000)
	appendActs("ov-A", qa, 1)

	queues := []string{qa, qb, qc}
	rows, err := b.Pool().Query(ctx, `
		SELECT id, instance_id FROM wf_tasks
		WHERE kind = 'activity' AND queue = ANY($1)
		ORDER BY visible_at, id`, queues)
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
	if len(cands) != 2005 || cands[0].ins != "ov-A" || cands[1].ins != "ov-C" ||
		cands[2].ins != "ov-B" || cands[3].ins != "ov-C" || cands[2004].ins != "ov-A" {
		t.Fatalf("FIFO head/tail = %v..%v, want A,C,B,C,..,A over 2005 rows", cands[:4], cands[2004:])
	}

	// Blocker locks A1 and C1, like a concurrent fair claimer that scanned
	// the same head IDs first.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = ANY($1) FOR UPDATE`,
		[]int64{cands[0].id, cands[1].id}); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: queues, Limit: 4,
		Lease: time.Minute, WorkerID: "ov-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 3 {
		t.Fatalf("claim = %d tasks, want 3 [B1 C2 A2] (A2 stranded past the cap)", len(tasks))
	}
	want := []int64{cands[2].id, cands[3].id, cands[2004].id}
	for i, id := range want {
		if tasks[i].ID != id {
			t.Fatalf("claim[%d] = %d, want %d (FIFO [B1 C2 A2])", i, tasks[i].ID, id)
		}
	}
}

// TestFairClaimProbesLockedCarry covers the issue #294 round-15 P2: a
// retained carry over one locked instance must be batch-probed with a single
// SELECT ... FOR UPDATE SKIP LOCKED instead of retried one pick per pass
// (~2001 lock queries plus quadratic re-offers for A1..A2002 with A1..A2001
// locked, all inside one long txn). One instance holds 60 activity tasks
// with the first 59 locked by a concurrent claimer; the claim must return
// the unlocked FIFO tail — not a locked row, not an empty batch — and keep
// FIFO + cap semantics.
func TestFairClaimProbesLockedCarry(t *testing.T) {
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

	const queue = "probecarry"
	const total = 60
	const perAdv = 20
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: "probe-1", Name: "WF", Queue: queue}); err != nil {
		t.Fatal(err)
	}
	for adv := 0; adv < total/perAdv; adv++ {
		wf, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{queue}, Limit: 1,
			Lease: time.Minute, WorkerID: "probe-spawn",
		})
		if err != nil || len(wf) != 1 {
			t.Fatalf("claim wf round %d: %v %#v", adv, err, wf)
		}
		st, err := b.LoadWorkflow(ctx, "probe-1")
		if err != nil {
			t.Fatal(err)
		}
		commit := backend.Advancement{
			InstanceID:         "probe-1",
			TaskID:             wf[0].ID,
			ExpectedSeq:        st.NextSeq,
			EnsureWorkflowTask: true,
		}
		for i := 0; i < perAdv; i++ {
			seq := st.NextSeq + int64(i)
			commit.NewEvents = append(commit.NewEvents, journal.Event{
				Seq: seq, Type: journal.TypeActivityScheduled, Name: "step",
			})
			commit.ActivityTasks = append(commit.ActivityTasks, backend.NewTask{
				Kind: "activity", Queue: queue, InstanceID: "probe-1",
				Name: "step", Seq: seq, Input: []byte(`{}`),
			})
		}
		if err := b.CommitAdvancement(ctx, commit); err != nil {
			t.Fatal(err)
		}
	}

	rows, err := b.Pool().Query(ctx, `
		SELECT id FROM wf_tasks
		WHERE kind = 'activity' AND queue = $1
		ORDER BY visible_at, id`, queue)
	if err != nil {
		t.Fatal(err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(ids) != total {
		t.Fatalf("spawned %d activity tasks, want %d", len(ids), total)
	}

	// Blocker locks every head row, like a concurrent fair claimer that
	// scanned the same IDs and locked them first; only the FIFO tail stays
	// claimable.
	btx, err := b.Pool().Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer btx.Rollback(ctx)
	if _, err := btx.Exec(ctx, `SELECT id FROM wf_tasks WHERE id = ANY($1) FOR UPDATE`, ids[:len(ids)-1]); err != nil {
		t.Fatal(err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{queue}, Limit: 2,
		Lease: time.Minute, WorkerID: "probe-w", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].ID != ids[len(ids)-1] {
		got := make([]int64, 0, len(tasks))
		for _, task := range tasks {
			got = append(got, task.ID)
		}
		t.Fatalf("claim = %v, want unlocked FIFO tail [%d]", got, ids[len(ids)-1])
	}

	// Everything left is locked: a follow-up claim must come back empty
	// rather than hand out a row the blocker owns.
	again, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "activity", Queues: []string{queue}, Limit: 2,
		Lease: time.Minute, WorkerID: "probe-w2", MaxPerInstance: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Fatalf("claim over fully locked carry = %d tasks, want 0", len(again))
	}
}
