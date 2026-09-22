package backendtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
)

func testSearchAttributes(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	id := instanceID("sa-", t)
	if err := b.CreateInstance(ctx, backend.NewInstance{
		ID: id, Name: "WF", Queue: "default",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "new"},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err := b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["tenant"] != "acme" || inst.SearchAttributes["phase"] != "new" {
		t.Fatalf("get attrs: %#v", inst.SearchAttributes)
	}

	list, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme"},
	})
	if err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("list: %+v err=%v", list, err)
	}
	miss, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "other"},
	})
	if err != nil || len(miss) != 0 {
		t.Fatalf("want empty, got %+v err=%v", miss, err)
	}

	tasks, err := b.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Second, WorkerID: "w1",
	})
	if err != nil || len(tasks) != 1 {
		t.Fatalf("claim: %+v err=%v", tasks, err)
	}
	st, err := b.LoadWorkflow(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Instance.SearchAttributes["tenant"] != "acme" {
		t.Fatalf("load attrs: %#v", st.Instance.SearchAttributes)
	}
	if err := b.CommitAdvancement(ctx, backend.Advancement{
		InstanceID: id, TaskID: tasks[0].ID, ExpectedSeq: st.NextSeq,
		NewEvents: []journal.Event{{
			Seq:     st.NextSeq,
			Type:    journal.TypeSearchAttributesUpdated,
			Payload: []byte(`{"tenant":"acme","phase":"shipped"}`),
		}},
	}); err != nil {
		t.Fatal(err)
	}
	inst, err = b.GetInstance(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if inst.SearchAttributes["phase"] != "shipped" || inst.SearchAttributes["tenant"] != "acme" {
		t.Fatalf("after upsert: %#v", inst.SearchAttributes)
	}
	andList, err := b.ListInstances(ctx, backend.InstanceFilter{
		Status:           "running",
		SearchAttributes: map[string]string{"tenant": "acme", "phase": "shipped"},
	})
	if err != nil || len(andList) != 1 || andList[0].ID != id {
		t.Fatalf("AND list: %+v err=%v", andList, err)
	}
}

// testSearchAttributesPagination inserts enough rows that a full-table scan
// would be observable, then pages a SearchAttributes-filtered listing with a
// small limit. Backends must filter in storage and honor limit/offset there:
// every page must hold only matches, concatenated pages must cover exactly
// the expected set, and an out-of-range offset must return empty.
func testSearchAttributesPagination(t *testing.T, newBackend Factory) {
	ctx := context.Background()
	b := newBackend(t)
	const total = 30
	var want []string
	for i := 0; i < total; i++ {
		tenant := "other"
		if i%2 == 0 {
			tenant = "acme"
		}
		id := fmt.Sprintf("sapage-%03d", i)
		if err := b.CreateInstance(ctx, backend.NewInstance{
			ID: id, Name: "WF", Queue: "default",
			SearchAttributes: map[string]string{"tenant": tenant, "idx": fmt.Sprintf("%03d", i)},
		}); err != nil {
			t.Fatal(err)
		}
		if tenant == "acme" {
			want = append(want, id)
		}
	}

	var got []string
	const pageSize = 5
	for offset := 0; ; offset += pageSize {
		page, err := b.ListInstances(ctx, backend.InstanceFilter{
			SearchAttributes: map[string]string{"tenant": "acme"},
			Limit:            pageSize,
			Offset:           offset,
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > pageSize {
			t.Fatalf("offset %d: page holds %d rows, want <= %d", offset, len(page), pageSize)
		}
		if len(page) == 0 {
			break
		}
		for _, inst := range page {
			if inst.SearchAttributes["tenant"] != "acme" {
				t.Fatalf("offset %d: non-match %s in page: %#v", offset, inst.ID, inst.SearchAttributes)
			}
			got = append(got, inst.ID)
		}
		if len(page) < pageSize {
			break
		}
	}
	if len(got) != len(want) {
		t.Fatalf("paged %d matches, want %d", len(got), len(want))
	}
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	for _, id := range want {
		if seen[id] != 1 {
			t.Fatalf("match %s appears %d times in pages, want 1", id, seen[id])
		}
	}

	// Page order must follow the listing order (created_at, id): with
	// zero-padded sequential ids created in order, pages concatenate exactly.
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("page order mismatch at %d: got %s want %s", i, got[i], want[i])
		}
	}

	// Out-of-range offset returns empty, not an error.
	empty, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme"},
		Limit:            pageSize,
		Offset:           total * 10,
	})
	if err != nil || len(empty) != 0 {
		t.Fatalf("want empty page, got %+v err=%v", empty, err)
	}

	// Multi-key AND narrows to a single row even with pagination set.
	one, err := b.ListInstances(ctx, backend.InstanceFilter{
		SearchAttributes: map[string]string{"tenant": "acme", "idx": "004"},
		Limit:            pageSize,
	})
	if err != nil || len(one) != 1 || one[0].ID != "sapage-004" {
		t.Fatalf("AND page: %+v err=%v", one, err)
	}

	// Status composes with the storage-side attribute filter.
	running, err := b.ListInstances(ctx, backend.InstanceFilter{
		Status:           "running",
		SearchAttributes: map[string]string{"tenant": "other"},
		Limit:            total,
	})
	if err != nil || len(running) != total-len(want) {
		t.Fatalf("status+SA: got %d, want %d (err=%v)", len(running), total-len(want), err)
	}
}
