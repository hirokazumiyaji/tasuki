package wftest_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/wftest"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestRun_SevenDaySleepWithVirtualClock(t *testing.T) {
	env := wftest.New(t)
	wftest.RegisterActivity(env, func(ctx context.Context, _ struct{}) (string, error) {
		return "ping", nil
	}, wftest.WithName("ping"))

	res, err := wftest.Run(env, func(ctx *workflow.Context, _ struct{}) (string, error) {
		if _, err := workflow.Execute[struct{}, string](ctx, "ping", struct{}{}); err != nil {
			return "", err
		}
		if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
			return "", err
		}
		return "done", nil
	}, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	if res != "done" {
		t.Fatalf("got %q", res)
	}
}
