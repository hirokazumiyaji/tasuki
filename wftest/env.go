package wftest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type Env struct {
	t      *testing.T
	backend *memory.Backend
	worker *tasuki.Worker
	client *tasuki.Client
}

type Option = tasuki.RegisterOption

func WithName(name string) Option { return tasuki.WithName(name) }

func New(t *testing.T) *Env {
	t.Helper()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Hour})
	return &Env{
		t:       t,
		backend: b,
		worker:  w,
		client:  tasuki.NewClient(b),
	}
}

func RegisterActivity[I, O any](env *Env, fn func(context.Context, I) (O, error), opts ...Option) {
	tasuki.RegisterActivity(env.worker, fn, opts...)
}

func Run[I, O any](env *Env, fn func(*workflow.Context, I) (O, error), input I) (O, error) {
	var zero O
	const wfName = "__wftest_workflow__"
	tasuki.RegisterWorkflow(env.worker, fn, tasuki.WithName(wfName))

	h, err := tasuki.Start(context.Background(), env.client, wfName, input, tasuki.WithID("wftest-"+env.t.Name()))
	if err != nil {
		return zero, err
	}
	_ = h

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		env.worker.PollOnce(context.Background())

		info, err := env.client.Get(context.Background(), "wftest-"+env.t.Name())
		if err != nil {
			return zero, err
		}
		switch info.Status {
		case "completed":
			var out O
			if err := json.Unmarshal(info.Result, &out); err != nil {
				return zero, err
			}
			return out, nil
		case "failed", "stuck":
			return zero, fmt.Errorf("workflow %s: %s", info.Status, string(info.Failure))
		}

		if !env.backend.HasRunnableTasks() {
			if next, ok := env.backend.NextTimerFireAt(); ok && next.After(env.backend.Now()) {
				env.backend.SetNow(next)
				continue
			}
		}
		time.Sleep(time.Millisecond)
	}
	return zero, fmt.Errorf("wftest timeout")
}
