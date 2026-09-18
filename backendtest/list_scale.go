package backendtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// ListInstances at scale with SearchAttributes (#299 item 6).
//
// With many rows present, Limit/Offset paging must walk the whole fleet and
// filtered queries must stay capped at limit. This is the functional half of
// the item; the efficiency half — server-side reads proportional to limit
// rather than a full scan (Firestore charges per document read; the SQL and
// Spanner ListInstances paths filter search attributes client-side) — needs
// backend read instrumentation to assert and is tracked as a follow-up to
// #299. This test pins the observable contract so a future indexed
// implementation cannot change paging semantics.
func testListInstancesScale(t *testing.T, newBackend Factory) {
	t.Helper()
	ctx := context.Background()
	b := newBackend(t)
	setNow(b, time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC))

	const total = 150
	prefix := instanceID("scale-", t) + "-"
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("%s%03d", prefix, i)
		attrs := map[string]string{"tenant": fmt.Sprintf("t%d", i%3)}
		if i%2 == 0 {
			attrs["parity"] = "even"
		} else {
			attrs["parity"] = "odd"
		}
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "WF", Queue: "default", SearchAttributes: attrs,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Complete every 5th instance so status filters have something to select.
	for i := 0; i < total; i += 5 {
		id := fmt.Sprintf("%s%03d", prefix, i)
		tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
			Kind: "workflow", Queues: []string{"default"}, Limit: total,
			Lease: time.Minute, WorkerID: "scale",
		})
		if err != nil {
			t.Fatal(err)
		}
		var own *backend.Task
		for j := range tasks {
			if tasks[j].InstanceID == id {
				own = &tasks[j]
				break
			}
		}
		if own == nil {
			t.Fatalf("no workflow task for %s", id)
		}
		st, err := b.LoadWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.CommitAdvancement(ctx, backend.Advancement{
			InstanceID:  id,
			TaskID:      own.ID,
			ExpectedSeq: st.NextSeq,
			NewEvents: []journal.Event{{
				Seq: st.NextSeq, Type: journal.TypeWorkflowCompleted, Payload: []byte(`"ok"`),
			}},
			Terminal: &backend.TerminalUpdate{Status: "completed", Result: []byte(`"ok"`)},
		}); err != nil {
			t.Fatal(err)
		}
		// Return the other claimed tasks to the queue.
		for j := range tasks {
			if tasks[j].InstanceID == id {
				continue
			}
			if err := b.ReleaseLease(ctx, tasks[j].ID); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Full walk with small pages must visit every instance exactly once.
	const page = 25
	seen := map[string]int{}
	for offset := 0; ; offset += page {
		list, err := b.ListInstances(ctx, backend.InstanceFilter{Limit: page, Offset: offset})
		if err != nil {
			t.Fatal(err)
		}
		if len(list) == 0 {
			break
		}
		if len(list) > page {
			t.Fatalf("page exceeds limit: %d > %d", len(list), page)
		}
		for _, inst := range list {
			seen[inst.ID]++
		}
		if len(list) < page {
			break
		}
	}
	if len(seen) != total {
		t.Fatalf("walk visited %d instances, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s visited %d times", id, n)
		}
	}

	// Filtered + capped queries must respect limit and match the filter.
	capped, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "t0"}, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capped) != 10 {
		t.Fatalf("capped SA query returned %d, want 10", len(capped))
	}
	for _, inst := range capped {
		if inst.SearchAttributes["tenant"] != "t0" {
			t.Fatalf("%s tenant=%q, want t0", inst.ID, inst.SearchAttributes["tenant"])
		}
	}
	andList, err := b.ListInstances(ctx, backend.InstanceFilter{
		Status:           "completed",
		SearchAttributes: map[string]string{"tenant": "t0", "parity": "even"},
		Limit:            100,
	})
	if err != nil {
		t.Fatal(err)
	}
	// i%5==0 (completed) && i%3==0 (t0) && i%2==0 (even): i multiple of 30.
	want := 0
	for i := 0; i < total; i += 30 {
		want++
	}
	if len(andList) != want {
		t.Fatalf("AND query returned %d, want %d", len(andList), want)
	}
	for _, inst := range andList {
		if inst.Status != "completed" || inst.SearchAttributes["tenant"] != "t0" ||
			inst.SearchAttributes["parity"] != "even" {
			t.Fatalf("AND mismatch: %+v", inst)
		}
	}
}
