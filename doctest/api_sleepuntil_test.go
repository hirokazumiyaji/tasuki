package doctest_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/wftest"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// SleepUntilWorkflow mirrors the documented `Sleep(ctx, d)` /
// `SleepUntil(ctx, t)` timer API in docs/03-api.md: SleepUntil suspends until
// an absolute time derived from the durable Now clock.
func SleepUntilWorkflow(ctx *workflow.Context, _ struct{}) (string, error) {
	deadline := workflow.Now(ctx).Add(time.Hour)
	if err := workflow.SleepUntil(ctx, deadline); err != nil {
		return "", err
	}
	return "woke", nil
}

func TestSleepUntil_DocExample(t *testing.T) {
	env := wftest.New(t)
	res, err := wftest.Run(env, SleepUntilWorkflow, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res != "woke" {
		t.Fatalf("got %q", res)
	}
}
