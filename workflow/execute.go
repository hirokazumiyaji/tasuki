package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// ActivitySchedule is the journal payload for activity_scheduled.
type ActivitySchedule struct {
	Input                 json.RawMessage  `json:"input"`
	Retry                 *RetryPolicyJSON `json:"retry,omitempty"`
	StartToCloseTimeoutMs int64            `json:"start_to_close_timeout_ms,omitempty"`
}

type RetryPolicyJSON struct {
	InitialIntervalMs  int64   `json:"initial_interval_ms"`
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	MaxIntervalMs      int64   `json:"max_interval_ms"`
	MaxAttempts        int     `json:"max_attempts"`
}

// Execute schedules an activity by name and waits for its completion event.
func Execute[I, O any](ctx *Context, activityName string, in I, opts ...ExecuteOption) (O, error) {
	var zero O
	var eo executeOptions
	for _, opt := range opts {
		opt(&eo)
	}
	input, err := ctx.codec.Marshal(in)
	if err != nil {
		return zero, err
	}
	sched := ActivitySchedule{Input: input}
	applyExecuteOptions(&sched, eo)
	payload, err := json.Marshal(sched)
	if err != nil {
		return zero, err
	}
	ev := ctx.recordOrReplay(journal.Command{
		Type: journal.TypeActivityScheduled,
		Name: activityName,
	}, payload)

	comp, ok := ctx.awaitCompletion(ev.Seq)
	if !ok {
		ctx.suspend()
		return zero, nil
	}
	if comp.Type == journal.TypeActivityFailed {
		var msg string
		_ = json.Unmarshal(comp.Payload, &msg)
		if msg == "" {
			msg = "activity failed"
		}
		return zero, fmt.Errorf("%s", msg)
	}
	var out O
	if err := ctx.codec.Unmarshal(comp.Payload, &out); err != nil {
		return zero, err
	}
	return out, nil
}

func applyExecuteOptions(sched *ActivitySchedule, eo executeOptions) {
	if eo.retry != (RetryPolicy{}) {
		r := eo.retry.withDefaults()
		sched.Retry = &RetryPolicyJSON{
			InitialIntervalMs:  r.InitialInterval.Milliseconds(),
			BackoffCoefficient: r.BackoffCoefficient,
			MaxIntervalMs:      r.MaxInterval.Milliseconds(),
			MaxAttempts:        r.MaxAttempts,
		}
	}
	if eo.startToClose > 0 {
		sched.StartToCloseTimeoutMs = eo.startToClose.Milliseconds()
	}
}
