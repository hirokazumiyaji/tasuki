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
	"github.com/hirokazumiyaji/tasuki/backend/spanner"
)

func TestChaos_KillWorkers_Spanner(t *testing.T) {
	dsn := os.Getenv("TASUKI_SPANNER_DSN")
	if dsn == "" {
		t.Skip("TASUKI_SPANNER_DSN not set")
	}
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set")
	}
	ctx := context.Background()
	if err := spanner.RecreateDatabase(ctx, dsn); err != nil {
		t.Fatal(err)
	}
	b, err := spanner.New(ctx, dsn)
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

	const nInst = 20
	c := tasuki.NewClient(b)
	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-spanner-%d", i)
		if _, err := tasuki.Start(ctx, c, "chaos", 0, tasuki.WithID(id)); err != nil {
			t.Fatal(err)
		}
	}

	_, thisFile, _, _ := runtime.Caller(0)
	workerDir := filepath.Join(filepath.Dir(thisFile), "cmd", "worker")
	bin := filepath.Join(t.TempDir(), "chaos-worker-spanner")
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
		id := fmt.Sprintf("sw-%d-%d", i, time.Now().UnixNano())
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"TASUKI_BACKEND=spanner",
			"TASUKI_SPANNER_DSN="+dsn,
			"SPANNER_EMULATOR_HOST="+os.Getenv("SPANNER_EMULATOR_HOST"),
			"WORKER_ID="+id,
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

	deadline := time.Now().Add(45 * time.Second)
	rng := rand.New(rand.NewSource(4))
	for time.Now().Before(deadline) {
		done := 0
		for i := 0; i < nInst; i++ {
			info, err := c.Get(ctx, fmt.Sprintf("chaos-spanner-%d", i))
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
		id := fmt.Sprintf("chaos-spanner-%d", i)
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
