package worker_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/worker"
)

// unfairStubBackend embeds backend.Backend (nil) and only overrides the
// methods the worker poll loop touches when the store is empty.
type unfairStubBackend struct {
	backend.Backend
	fair bool
}

func (s *unfairStubBackend) Capabilities() backend.Capabilities {
	return backend.Capabilities{FairDispatch: s.fair}
}

func (s *unfairStubBackend) ClaimTasks(context.Context, backend.ClaimRequest) ([]backend.Task, error) {
	return nil, nil
}

func (s *unfairStubBackend) FireDueTimers(context.Context, int) (int, error) {
	return 0, nil
}

func (s *unfairStubBackend) ClaimDueSchedules(context.Context, int) ([]backend.DueSchedule, error) {
	return nil, nil
}

func (s *unfairStubBackend) CountClaimableTasks(context.Context, string, []string) (map[string]int64, error) {
	return map[string]int64{}, nil
}

func startAndCaptureFairWarning(t *testing.T, b backend.Backend, maxPer int) string {
	t.Helper()
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	w := worker.NewWorker(b, worker.WorkerOptions{
		Queues:                  []string{"default"},
		PollInterval:            10 * time.Millisecond,
		MaxPerInstance:          maxPer,
		Logger:                  logger,
		DisableSchemaValidation: true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	_ = w.Shutdown(ctx)
	return buf.String()
}

func TestWorkerWarnsOnUnsupportedFairDispatch(t *testing.T) {
	out := startAndCaptureFairWarning(t, &unfairStubBackend{fair: false}, 1)
	if !strings.Contains(out, "MaxPerInstance") {
		t.Fatalf("expected MaxPerInstance warning, got %q", out)
	}
}

func TestWorkerSilentOnSupportedFairDispatch(t *testing.T) {
	b := memory.New()
	out := startAndCaptureFairWarning(t, b, 1)
	if strings.Contains(out, "MaxPerInstance") {
		t.Fatalf("unexpected MaxPerInstance warning: %q", out)
	}
}
