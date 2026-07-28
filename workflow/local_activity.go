package workflow

import (
	"encoding/json"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// LocalActivityRunner invokes a registered activity by name with already-encoded input.
type LocalActivityRunner func(name string, input []byte) (result []byte, err error)

type localActivityPayload struct {
	Input  json.RawMessage `json:"input,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// ExecuteLocal runs a registered activity on the same Worker inside the workflow
// task and journals the result. On replay the runner is not called.
func ExecuteLocal[I, O any](ctx *Context, activityName string, in I) (O, error) {
	var zero O
	recorded := ctx.recordedCommands()
	if ctx.cmdIndex < len(recorded) {
		ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeLocalActivity, Name: activityName}, nil)
		var p localActivityPayload
		if err := json.Unmarshal(ev.Payload, &p); err != nil {
			return zero, err
		}
		if p.Error != "" {
			return zero, fmt.Errorf("%s", p.Error)
		}
		var out O
		if err := ctx.codec.Unmarshal(p.Result, &out); err != nil {
			return zero, err
		}
		return out, nil
	}
	if ctx.localRunner == nil {
		return zero, ErrLocalActivityRunnerMissing
	}
	input, err := ctx.codec.Marshal(in)
	if err != nil {
		return zero, err
	}
	result, runErr := ctx.localRunner(activityName, input)
	p := localActivityPayload{Input: input}
	if runErr != nil {
		p.Error = runErr.Error()
	} else {
		p.Result = result
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return zero, err
	}
	ctx.recordOrReplay(journal.Command{Type: journal.TypeLocalActivity, Name: activityName}, payload)
	if runErr != nil {
		return zero, runErr
	}
	var out O
	if err := ctx.codec.Unmarshal(result, &out); err != nil {
		return zero, err
	}
	return out, nil
}
