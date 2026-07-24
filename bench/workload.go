package bench

import (
	"context"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

const (
	WorkflowName = "bench-chain"
	ActivityName = "bench-step"
)

// Register attaches the bench-chain workflow and bench-step activity to w.
func Register(w *tasuki.Worker) {
	tasuki.RegisterActivity(w, step, tasuki.WithName(ActivityName))
	tasuki.RegisterWorkflow(w, chain, tasuki.WithName(WorkflowName))
}

func chain(ctx *workflow.Context, steps int) (int, error) {
	v := 0
	for i := 0; i < steps; i++ {
		next, err := workflow.Execute[int, int](ctx, ActivityName, v)
		if err != nil {
			return 0, err
		}
		v = next
	}
	return v, nil
}

func step(_ context.Context, n int) (int, error) {
	return n + 1, nil
}
