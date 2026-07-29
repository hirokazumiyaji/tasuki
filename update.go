package tasuki

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// ErrNotRunning is returned when Update targets a non-running instance.
var ErrNotRunning = errors.New("workflow not running")

type updateOptions struct {
	id string
}

// UpdateOption configures tasuki.Update.
type UpdateOption func(*updateOptions)

// WithUpdateID sets a caller-chosen idempotency / correlation id for an Update.
func WithUpdateID(id string) UpdateOption {
	return func(o *updateOptions) { o.id = id }
}

// Update sends a named update to a running workflow and waits for the handler result.
// The Worker must have the workflow registered (same process model as Query).
func Update[I, O any](ctx context.Context, w *Worker, instanceID, name string, in I, opts ...UpdateOption) (O, error) {
	var zero O
	var o updateOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.id == "" {
		var b [16]byte
		if _, err := rand.Read(b[:]); err != nil {
			return zero, err
		}
		o.id = hex.EncodeToString(b[:])
	}

	input, err := w.reg.codec.Marshal(in)
	if err != nil {
		return zero, err
	}
	reqPayload, err := json.Marshal(struct {
		ID    string          `json:"id"`
		Input json.RawMessage `json:"input"`
	}{ID: o.id, Input: input})
	if err != nil {
		return zero, err
	}

	inst, err := w.backend.GetInstance(ctx, instanceID)
	if err != nil {
		return zero, err
	}
	if inst.Status != "running" {
		return zero, fmt.Errorf("%w: %s", ErrNotRunning, inst.Status)
	}

	journalEvents, err := w.backend.GetJournal(ctx, instanceID, 0)
	if err != nil {
		return zero, err
	}
	if result, errMsg, ok := workflow.FindUpdateCompletion(journalEvents, o.id); ok {
		if errMsg != "" {
			return zero, workflow.FormatUpdateError(errMsg)
		}
		var out O
		if err := w.reg.codec.Unmarshal(result, &out); err != nil {
			return zero, err
		}
		return out, nil
	}

	if !workflow.UpdateAcceptedInFlight(journalEvents, o.id) {
		err = w.backend.SendToInbox(ctx, instanceID, journal.Event{
			Type:    journal.TypeUpdateRequested,
			Name:    name,
			Payload: reqPayload,
		}, o.id)
		if err != nil {
			return zero, err
		}
	}

	ticker := time.NewTicker(w.opts.PollInterval)
	defer ticker.Stop()

	var wake <-chan struct{}
	if n, ok := w.backend.(backend.TaskNotifier); ok {
		ch, err := n.Subscribe(ctx)
		if err == nil {
			wake = ch
		}
	}

	for {
		w.PollOnce(ctx)

		st, err := w.backend.LoadWorkflow(ctx, instanceID)
		if err != nil {
			return zero, err
		}
		if result, errMsg, ok := workflow.FindUpdateCompletion(st.Journal, o.id); ok {
			if errMsg != "" {
				return zero, workflow.FormatUpdateError(errMsg)
			}
			var out O
			if err := w.reg.codec.Unmarshal(result, &out); err != nil {
				return zero, err
			}
			return out, nil
		}
		if st.Instance.Status != "running" {
			return zero, fmt.Errorf("%w: %s", ErrNotRunning, st.Instance.Status)
		}

		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-wake:
		case <-ticker.C:
		}
	}
}
