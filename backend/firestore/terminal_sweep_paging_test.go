package firestore

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	gcf "cloud.google.com/go/firestore"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestResolveSweepCutoff pins the cutoff-read error policy (Codex
// round-22 P2 (c) on #291): only a missing instance (NotFound) falls back
// to the unbounded sweep; every other read error propagates so the caller
// retries instead of sweeping unbounded — a transient Get failure that
// silently zeroed the cutoff would delete an accepted post-commit send and
// report success.
func TestResolveSweepCutoff(t *testing.T) {
	flip := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	notFound := status.Error(codes.NotFound, "missing")
	unavailable := status.Error(codes.Unavailable, "transient")
	boom := errors.New("boom")
	cases := []struct {
		name       string
		exists     bool
		err        error
		updateTime time.Time
		wantCutoff time.Time
		wantErr    bool
	}{
		{"flip time kept", true, nil, flip, flip, false},
		{"notfound falls back to unbounded", false, notFound, flip, time.Time{}, false},
		{"missing doc falls back to unbounded", false, nil, flip, time.Time{}, false},
		{"transient error propagates", true, unavailable, flip, time.Time{}, true},
		{"transient error propagates without doc", false, unavailable, flip, time.Time{}, true},
		{"plain error propagates", true, boom, flip, time.Time{}, true},
	}
	for _, c := range cases {
		got, err := resolveSweepCutoff(c.exists, c.err, c.updateTime)
		if c.wantErr && err == nil {
			t.Errorf("%s: resolveSweepCutoff returned nil error, want propagation (unbounded sweep would delete accepted sends)", c.name)
		}
		if !c.wantErr && err != nil {
			t.Errorf("%s: resolveSweepCutoff returned error %v, want cutoff %v", c.name, err, c.wantCutoff)
		}
		if !got.Equal(c.wantCutoff) {
			t.Errorf("%s: cutoff = %v, want %v", c.name, got, c.wantCutoff)
		}
	}
}

// TestDeleteTerminalColDocs_PagesPastRetainedDocs covers Codex round-22 P2
// (d) on #291: when a full page fills with retained post-transition rows,
// the sweep must advance past them instead of declaring completion. Old
// code stopped when a page yielded zero deletions, so 400+ retained rows
// at the head of the collection shielded every older deletable row behind
// them permanently. The new code stops only when a page yields zero rows,
// with a document-ID cursor advancing past retained rows each page.
//
// Layout (document-ID order): 401 retained rows ("…:a…", committed after
// the flip) sort before 3 deletable rows ("…:z…", committed before the
// flip). The first page holds 400 retained rows and zero deletions — the
// old early return fired here and the "z" rows survived.
func TestDeleteTerminalColDocs_PagesPastRetainedDocs(t *testing.T) {
	ctx := context.Background()
	b := parentEnsureTestBackend(t)

	id := fmt.Sprintf("r22-page-past-%d", time.Now().UnixNano())
	if err := b.CreateInstance(ctx, backend.NewInstance{ID: id, Name: "WF"}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = b.ref("wf_instances", id).Delete(ctx)
		it := b.col("wf_inbox").Where("instance_id", "==", id).Documents(ctx)
		for {
			d, err := it.Next()
			if err != nil {
				break
			}
			_, _ = d.Ref.Delete(ctx)
		}
		it.Stop()
	})

	// Deletable rows first (predate the flip below)...
	for i := 0; i < 3; i++ {
		docID := fmt.Sprintf("%s:z%03d", id, i)
		if _, err := b.ref("wf_inbox", docID).Set(ctx, inboxDoc(id, int64(1000+i), int64(1000+i), journal.Event{Type: journal.TypeSignalReceived, Name: "s"}, nowUTC())); err != nil {
			t.Fatal(err)
		}
	}
	// ...then advance the instance flip past them...
	if _, err := b.ref("wf_instances", id).Update(ctx, []gcf.Update{{Path: "updated_at", Value: nowUTC()}}); err != nil {
		t.Fatal(err)
	}
	// ...then the retained post-transition rows. The sleep separates the
	// server update times across the second boundary: a retained row
	// sharing the flip's timestamp would (correctly) sweep, which would
	// fail the test for timing reasons rather than logic.
	time.Sleep(1100 * time.Millisecond)
	err := b.client.RunTransaction(ctx, func(ctx context.Context, tx *gcf.Transaction) error {
		for i := 0; i < terminalCleanupBatchSize+1; i++ {
			docID := fmt.Sprintf("%s:a%03d", id, i)
			if err := tx.Set(b.ref("wf_inbox", docID), inboxDoc(id, int64(2000+i), int64(2000+i), journal.Event{Type: journal.TypeSignalReceived, Name: "s"}, nowUTC())); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := b.cleanupTerminalDocs(ctx, id); err != nil {
		t.Fatal(err)
	}
	if n := countDocs(t, b, ctx, "wf_inbox", id); n != terminalCleanupBatchSize+1 {
		t.Errorf("inbox rows = %d, want %d (3 pre-flip rows swept, all retained rows preserved)", n, terminalCleanupBatchSize+1)
	}
	for i := 0; i < 3; i++ {
		s, err := b.ref("wf_inbox", fmt.Sprintf("%s:z%03d", id, i)).Get(ctx)
		if err == nil && s.Exists() {
			t.Errorf("pre-flip row %s:z%03d survived behind the retained page (false completion)", id, i)
		}
	}
}
