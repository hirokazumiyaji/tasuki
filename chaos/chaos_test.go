//go:build tasuki_all

package chaos_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/journal"
)

// waitKilledWorker reaps a chaos worker after the test deliberately
// terminated it (SIGKILL) and fails the test if the worker instead exited
// on its own with a nonzero status. The race detector exits with status 66
// by default, so discarding Wait results mistakes a race self-exit for the
// intended SIGKILL, replaces the worker, and still passes. Deliberate
// SIGKILL/SIGTERM terminations (and a clean exit) remain accepted.
func waitKilledWorker(t *testing.T, p *os.Process) {
	t.Helper()
	state, err := p.Wait()
	if err != nil {
		t.Errorf("wait chaos worker: %v", err)
		return
	}
	if state.Success() {
		return
	}
	status, ok := state.Sys().(syscall.WaitStatus)
	if !ok {
		t.Errorf("chaos worker exited unsuccessfully: %v", state)
		return
	}
	if status.Signaled() {
		switch status.Signal() {
		case syscall.SIGKILL, syscall.SIGTERM:
			return
		}
		t.Errorf("chaos worker killed by unexpected signal %v", status.Signal())
		return
	}
	t.Errorf("chaos worker exited with status %d (race detector exits 66); not a deliberate SIGKILL", status.ExitStatus())
}

func TestChaos_KillWorkers(t *testing.T) {
	dsn := os.Getenv("TASUKI_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TASUKI_POSTGRES_DSN not set")
	}
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

	const nInst = 20
	c := tasuki.NewClient(b)
	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-%d", i)
		if _, err := tasuki.Start(ctx, c, "chaos", 0, tasuki.WithID(id)); err != nil {
			t.Fatal(err)
		}
	}

	_, thisFile, _, _ := runtime.Caller(0)
	workerDir := filepath.Join(filepath.Dir(thisFile), "cmd", "worker")
	bin := filepath.Join(t.TempDir(), "chaos-worker")
	build := exec.Command("go", "build", "-race", "-tags", "tasuki_all", "-o", bin, ".")
	build.Dir = workerDir
	// Race-instrument the spawned workers too: -race on the test binary
	// only covers the test process, while workflow execution happens here.
	build.Env = append(os.Environ(), "CGO_ENABLED=1")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}

	type proc struct {
		cmd *exec.Cmd
		id  string
	}
	workers := make([]*proc, 0, 3)
	startWorker := func(i int) *proc {
		id := fmt.Sprintf("w-%d-%d", i, time.Now().UnixNano())
		cmd := exec.Command(bin)
		// GORACE=halt_on_error=1 makes a race report terminate the worker
		// immediately (race exit 66) so Wait observes it before any
		// deliberate SIGKILL, which waitKilledWorker accepts.
		cmd.Env = append(os.Environ(),
			"TASUKI_POSTGRES_DSN="+dsn,
			"WORKER_ID="+id,
			"GORACE=halt_on_error=1",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker: %v", err)
		}
		return &proc{cmd: cmd, id: id}
	}
	for i := 0; i < 3; i++ {
		workers = append(workers, startWorker(i))
	}
	defer func() {
		for _, w := range workers {
			if w.cmd.Process != nil {
				_ = w.cmd.Process.Kill()
				waitKilledWorker(t, w.cmd.Process)
			}
		}
	}()

	deadline := time.Now().Add(15 * time.Second)
	rng := rand.New(rand.NewSource(1))
	for time.Now().Before(deadline) {
		// Check completion
		done := 0
		for i := 0; i < nInst; i++ {
			info, err := c.Get(ctx, fmt.Sprintf("chaos-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if info.Status == "completed" {
				done++
			} else if info.Status == "failed" || info.Status == "stuck" {
				t.Fatalf("instance chaos-%d status=%s failure=%s", i, info.Status, string(info.Failure))
			}
		}
		if done == nInst {
			break
		}
		// Randomly kill and restart a worker
		if rng.Intn(3) == 0 && len(workers) > 0 {
			idx := rng.Intn(len(workers))
			w := workers[idx]
			_ = w.cmd.Process.Signal(syscall.SIGKILL)
			waitKilledWorker(t, w.cmd.Process)
			workers[idx] = startWorker(idx)
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-%d", i)
		info, err := c.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != "completed" {
			t.Fatalf("%s status=%s", id, info.Status)
		}
		h, err := tasuki.Start(ctx, c, "chaos", 0, tasuki.WithID(id))
		if err != nil && !errors.Is(err, tasuki.ErrAlreadyStarted) {
			t.Fatal(err)
		}
		result, err := tasuki.Result[int](ctx, h)
		if err != nil {
			t.Fatal(err)
		}
		if result != 2 {
			t.Fatalf("%s result=%d want 2", id, result)
		}
		events, err := b.GetJournal(ctx, id, 0)
		if err != nil {
			t.Fatal(err)
		}
		assertJournalContiguous(t, events)
	}
}

func assertJournalContiguous(t *testing.T, events []journal.Event) {
	t.Helper()
	if len(events) == 0 {
		t.Fatal("empty journal")
	}
	for i, e := range events {
		want := int64(i + 1)
		if e.Seq != want {
			t.Fatalf("seq gap: events[%d].Seq=%d want %d", i, e.Seq, want)
		}
	}
	seen := map[int64]journal.Type{}
	for _, e := range events {
		if prev, ok := seen[e.Seq]; ok {
			t.Fatalf("duplicate seq %d: %s and %s", e.Seq, prev, e.Type)
		}
		seen[e.Seq] = e.Type
	}
}
