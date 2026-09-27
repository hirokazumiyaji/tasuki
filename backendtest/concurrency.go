package backendtest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// RunConcurrent adds exclusivity and I1 wakeup-race cases.
func RunConcurrent(t *testing.T, newBackend Factory) {
	t.Helper()
	t.Run("ClaimExclusive", func(t *testing.T) { testClaimExclusive(t, newBackend) })
	t.Run("WakeupRaceNoLostWakeup", func(t *testing.T) { testWakeupRace(t, newBackend) })
}

func testClaimExclusive(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	const n = 20
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("ex-%d", i)
		if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF", Queue: "default"}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			for {
				tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
					Kind: "workflow", Queues: []string{"default"}, Limit: 1,
					Lease: time.Minute, WorkerID: worker,
				})
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				if len(tasks) == 0 {
					return
				}
				mu.Lock()
				seen[tasks[0].ID]++
				mu.Unlock()
			}
		}(fmt.Sprintf("w%d", g))
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d unique tasks, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("task %d claimed %d times", id, c)
		}
	}
}

func testWakeupRace(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	const rounds = 40
	for r := 0; r < rounds; r++ {
		b := newBackend(t)
		setNow(b, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
		id := fmt.Sprintf("wake-%d", r)
		seq := scheduleActivity(t, b, id, "act")
		atasks, err := b.ClaimTasks(ctx, claimAct())
		if err != nil || len(atasks) != 1 {
			t.Fatalf("act claim: %v %#v", err, atasks)
		}

		// Two completers race; exactly one may win. Winner must leave a workflow task (I1).
		var wg sync.WaitGroup
		wg.Add(2)
		var errA, errB error
		go func() {
			defer wg.Done()
			errA = b.CompleteActivity(ctx, atasks[0], journal.Event{
				Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"a"`),
			})
		}()
		go func() {
			defer wg.Done()
			errB = b.CompleteActivity(ctx, atasks[0], journal.Event{
				Type: journal.TypeActivityCompleted, RefSeq: seq, Payload: []byte(`"b"`),
			})
		}()
		wg.Wait()
		okCount := 0
		if errA == nil {
			okCount++
		}
		if errB == nil {
			okCount++
		}
		if okCount != 1 {
			t.Fatalf("round %d: want exactly one complete success, got %d (a=%v b=%v)", r, okCount, errA, errB)
		}
		assertI1(t, b, id)
	}
}

func assertI1(t *testing.T, b backend.Backend, id string) {
	t.Helper()
	ctx := context.Background()
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Instance.Status != "running" || len(st.Inbox) == 0 {
		return
	}
	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 100,
		Lease: time.Second, WorkerID: "i1-check",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range tasks {
		if task.InstanceID == id {
			return
		}
	}
	t.Fatalf("I1 violated: inbox has %d events but no workflow task for %s", len(st.Inbox), id)
}
