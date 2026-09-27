package bench

import (
	"context"
	"time"

	"github.com/hirokazumiyaji/tasuki/worker"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

const (
	WorkflowName = "bench-chain"
	ActivityName = "bench-step"
	MixedName    = "bench-mixed"
	LongName     = "bench-long"
)

// Register attaches the bench-chain workflow and bench-step activity to w.
func Register(w *worker.Worker) {
	RegisterScenario(w, "chain")
}

// RegisterScenario registers workloads for a scenario (chain/long-history/mixed).
func RegisterScenario(w *worker.Worker, scenario string) {
	worker.RegisterActivity(w, step, worker.WithName(ActivityName))
	worker.RegisterActivity(w, slowStep, worker.WithName("bench-slow"))
	worker.RegisterWorkflow(w, chain, worker.WithName(WorkflowName))
	worker.RegisterWorkflow(w, chain, worker.WithName(LongName))
	worker.RegisterWorkflow(w, mixed, worker.WithName(MixedName))
}

// WorkflowNameFor maps a scenario to its workflow name.
func WorkflowNameFor(scenario string) string {
	switch scenario {
	case "mixed":
		return MixedName
	case "long-history":
		return LongName
	default:
		return WorkflowName
	}
}

func scenarioSteps(cfg Config) int {
	if cfg.Scenario == "long-history" && cfg.Steps < 20 {
		return 20
	}
	return cfg.Steps
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

// mixed interleaves fast and slow activities to model heterogeneous load.
func mixed(ctx *workflow.Context, steps int) (int, error) {
	v := 0
	for i := 0; i < steps; i++ {
		name := ActivityName
		if i%3 == 2 {
			name = "bench-slow"
		}
		next, err := workflow.Execute[int, int](ctx, name, v)
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

func slowStep(ctx context.Context, n int) (int, error) {
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(5 * time.Millisecond):
	}
	return n + 1, nil
}
