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
	"github.com/hirokazumiyaji/tasuki/backend/dynamodb"
)

func TestChaos_KillWorkers_DynamoDB(t *testing.T) {
	ep := os.Getenv("TASUKI_DYNAMODB_ENDPOINT")
	if ep == "" {
		t.Skip("TASUKI_DYNAMODB_ENDPOINT not set")
	}
	ctx := context.Background()
	b, err := dynamodb.New(ctx, dynamodb.Config{Endpoint: ep})
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
		id := fmt.Sprintf("chaos-ddb-%d", i)
		if _, err := tasuki.Start(ctx, c, "chaos", 0, tasuki.WithID(id)); err != nil {
			t.Fatal(err)
		}
	}

	_, thisFile, _, _ := runtime.Caller(0)
	workerDir := filepath.Join(filepath.Dir(thisFile), "cmd", "worker")
	bin := filepath.Join(t.TempDir(), "chaos-worker-ddb")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = workerDir
	build.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v\n%s", err, out)
	}

	type proc struct {
		cmd *exec.Cmd
	}
	workers := make([]*proc, 0, 3)
	startWorker := func(i int) *proc {
		id := fmt.Sprintf("dw-%d-%d", i, time.Now().UnixNano())
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(),
			"TASUKI_BACKEND=dynamodb",
			"TASUKI_DYNAMODB_ENDPOINT="+ep,
			"AWS_ACCESS_KEY_ID="+envOr("AWS_ACCESS_KEY_ID", "local"),
			"AWS_SECRET_ACCESS_KEY="+envOr("AWS_SECRET_ACCESS_KEY", "local"),
			"AWS_REGION="+envOr("AWS_REGION", "us-east-1"),
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
				_, _ = w.cmd.Process.Wait()
			}
		}
	}()

	deadline := time.Now().Add(45 * time.Second)
	rng := rand.New(rand.NewSource(5))
	for time.Now().Before(deadline) {
		done := 0
		for i := 0; i < nInst; i++ {
			info, err := c.Get(ctx, fmt.Sprintf("chaos-ddb-%d", i))
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
			_, _ = w.cmd.Process.Wait()
			workers[idx] = startWorker(idx)
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < nInst; i++ {
		id := fmt.Sprintf("chaos-ddb-%d", i)
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

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
