package wftest_test

import (
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki/wftest"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestEnv_Signal(t *testing.T) {
	env := wftest.New(t)
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(20 * time.Millisecond)
		env.Signal("go", "ping")
	}()
	res, err := wftest.Run(env, func(ctx *workflow.Context, _ struct{}) (string, error) {
		return workflow.ReceiveSignal[string](ctx, "go")
	}, struct{}{})
	<-done
	if err != nil {
		t.Fatal(err)
	}
	if res != "ping" {
		t.Fatalf("got %q", res)
	}
}
