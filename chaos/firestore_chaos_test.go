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

	"github.com/hirokazumiyaji/tasuki/backend/firestore"
	"github.com/hirokazumiyaji/tasuki/client"
)

func TestChaos_KillWorkers_Firestore(t *testing.T) {
	if os.Getenv("FIRESTORE_EMULATOR_HOST") == "" {
		t.Skip("FIRESTORE_EMULATOR_HOST not set")
	}
	ctx := context.Background()
	b, err := firestore.New(ctx, os.Getenv("TASUKI_FIRESTORE_PROJECT"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Reset(ctx); err != nil {
		t.Fatal(err)
	}

	const nInst = 10
	c := client.NewClient(b)
	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-fs-%d", i)
		if _, err := client.Start(ctx, c, "chaos", 0, client.WithID(id)); err != nil {
			t.Fatal(err)
		}
	}

	_, thisFile, _, _ := runtime.Caller(0)
	workerDir := filepath.Join(filepath.Dir(thisFile), "cmd", "worker")
	bin := filepath.Join(t.TempDir(), "chaos-worker-fs")
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
	}
	workers := make([]*proc, 0, 3)
	startWorker := func(i int) *proc {
		id := fmt.Sprintf("fw-%d-%d", i, time.Now().UnixNano())
		cmd := exec.Command(bin)
		// GORACE=halt_on_error=1 makes a race report terminate the worker
		// immediately (race exit 66) so Wait observes it before any
		// deliberate SIGKILL, which waitKilledWorker accepts.
		cmd.Env = append(os.Environ(),
			"TASUKI_BACKEND=firestore",
			"FIRESTORE_EMULATOR_HOST="+os.Getenv("FIRESTORE_EMULATOR_HOST"),
			"TASUKI_FIRESTORE_PROJECT="+envOr("TASUKI_FIRESTORE_PROJECT", "tasuki"),
			"WORKER_ID="+id,
			"GORACE=halt_on_error=1",
		)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker: %v", err)
		}
		return &proc{cmd: cmd}
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

	deadline := time.Now().Add(3 * time.Minute)
	rng := rand.New(rand.NewSource(6))
	for time.Now().Before(deadline) {
		done := 0
		for i := 0; i < nInst; i++ {
			info, err := c.Get(ctx, fmt.Sprintf("chaos-fs-%d", i))
			if err != nil {
				t.Fatal(err)
			}
			if info.Status == "completed" {
				done++
			} else if info.Status == "failed" || info.Status == "stuck" {
				t.Fatalf("instance status=%s failure=%s", info.Status, string(info.Failure))
			}
		}
		if done == nInst {
			break
		}
		// Kill less aggressively — Firestore emulator is contention-sensitive.
		if rng.Intn(8) == 0 && len(workers) > 0 {
			idx := rng.Intn(len(workers))
			w := workers[idx]
			_ = w.cmd.Process.Signal(syscall.SIGKILL)
			waitKilledWorker(t, w.cmd.Process)
			workers[idx] = startWorker(idx)
		}
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-fs-%d", i)
		info, err := c.Get(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if info.Status != "completed" {
			t.Fatalf("%s status=%s", id, info.Status)
		}
		h, err := client.Start(ctx, c, "chaos", 0, client.WithID(id))
		if err != nil && !errors.Is(err, client.ErrAlreadyStarted) {
			t.Fatal(err)
		}
		result, err := client.Result[int](ctx, h)
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
