package tasuki

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

// TestWorker_StaleShutdownReleaseTreatedAsReleased is a regression test
// for the fenced-release finding at the worker layer: a renewal delayed
// past the lease lets a peer reclaim the task, and the stale holder's
// shutdown release hits the backend fence (ErrNotFound). The worker must
// treat that as already-released — no error, no warning, no store-error
// metric — and above all leave the peer's lease intact so no third worker
// executes concurrently with the peer.
func TestWorker_StaleShutdownReleaseTreatedAsReleased(t *testing.T) {
	ctx := context.Background()
	t0 := time.Now().UTC()
	mem := memory.New()
	mem.SetNow(t0)
	var logBuf bytes.Buffer
	w := NewWorker(mem, WorkerOptions{
		PollInterval:  time.Millisecond,
		LeaseDuration: time.Minute,
		WorkerID:      "w1",
		Logger:        slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err := mem.CreateInstance(ctx, backend.NewInstance{ID: "i1", Name: "WF", Queue: "default"}); err != nil {
		t.Fatal(err)
	}
	stale, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "w1",
	})
	if err != nil || len(stale) != 1 {
		t.Fatalf("claim: %v %#v", err, stale)
	}
	w.track(stale[0])

	// The lease lapses and a peer reclaims the task while the stale
	// holder still tracks its generation.
	mem.SetNow(t0.Add(2 * time.Minute))
	peer, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 1, Lease: time.Minute, WorkerID: "peer",
	})
	if err != nil || len(peer) != 1 {
		t.Fatalf("peer claim: %v %#v", err, peer)
	}

	// Shutdown's release of the stale generation: the backend fence
	// rejects it (ErrNotFound) and the worker swallows that quietly.
	w.releaseInFlight(context.Background())
	// The explicit abandon-path release behaves the same.
	w.releaseWorkflowLease(stale[0])

	if logs := logBuf.String(); strings.Contains(logs, "shutdown lease release failed") ||
		strings.Contains(logs, "store operation failed") {
		t.Fatalf("stale release logged as a failure; want quiet already-released handling:\n%s", logs)
	}
	// The peer's lease is intact: a third worker finds nothing claimable.
	third, err := mem.ClaimTasks(ctx, backend.ClaimRequest{
		Kind: "workflow", Queues: []string{"default"}, Limit: 10, Lease: time.Minute, WorkerID: "third",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("third worker claimed %d tasks, want 0 (peer lease must stay intact)", len(third))
	}
}
