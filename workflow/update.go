package workflow

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// ErrUnknownUpdate is returned when no handler is registered for the update name.
var ErrUnknownUpdate = errors.New("workflow: unknown update")

type updateHandler func(input []byte) (result []byte, err error)

type updateRequestPayload struct {
	ID    string          `json:"id"`
	Input json.RawMessage `json:"input"`
}

type updateAcceptedPayload struct {
	ID string `json:"id"`
}

type updateCompletedPayload struct {
	ID     string          `json:"id"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// SetUpdateHandler registers a mutating update handler (re-registered every replay).
func SetUpdateHandler[I, O any](ctx *Context, name string, fn func(*Context, I) (O, error)) {
	if ctx.updateHandlers == nil {
		ctx.updateHandlers = map[string]updateHandler{}
	}
	codec := ctx.codec
	ctx.updateHandlers[name] = func(input []byte) ([]byte, error) {
		var in I
		if len(input) > 0 {
			if err := codec.Unmarshal(input, &in); err != nil {
				return nil, err
			}
		}
		out, err := fn(ctx, in)
		if err != nil {
			return nil, err
		}
		return codec.Marshal(out)
	}
}

type updateJob struct {
	name     string
	id       string
	input    []byte
	accepted bool
}

// DispatchUpdates accepts or resumes at most one pending Update on ctx.
// Returns true if an update handler was invoked (may have suspended).
func DispatchUpdates(ctx *Context) bool {
	if ctx.queryMode || ctx.queryInvoking {
		return false
	}
	job := ctx.findUpdateJob()
	if job == nil {
		return false
	}
	h, ok := ctx.updateHandlers[job.name]
	if !ok {
		// Record failure completion so Client does not wait forever.
		if !job.accepted {
			acc, _ := json.Marshal(updateAcceptedPayload{ID: job.id})
			ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateAccepted, Name: job.name}, acc)
		} else {
			ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateAccepted, Name: job.name}, nil)
		}
		comp, _ := json.Marshal(updateCompletedPayload{ID: job.id, Error: ErrUnknownUpdate.Error()})
		ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateCompleted, Name: job.name}, comp)
		return true
	}

	if job.accepted {
		ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateAccepted, Name: job.name}, nil)
	} else {
		acc, _ := json.Marshal(updateAcceptedPayload{ID: job.id})
		ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateAccepted, Name: job.name}, acc)
	}

	result, err := h(job.input)
	if ctx.suspended {
		return true
	}
	cp := updateCompletedPayload{ID: job.id}
	if err != nil {
		cp.Error = err.Error()
	} else {
		cp.Result = result
	}
	payload, _ := json.Marshal(cp)
	ctx.recordOrReplay(journal.Command{Type: journal.TypeUpdateCompleted, Name: job.name}, payload)
	return true
}

func (c *Context) findUpdateJob() *updateJob {
	completed := map[string]bool{}
	var inFlight *updateJob
	var requested []updateJob

	consider := func(e journal.Event) {
		switch e.Type {
		case journal.TypeUpdateCompleted:
			var p updateCompletedPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.ID != "" {
				completed[p.ID] = true
				if inFlight != nil && inFlight.id == p.ID {
					inFlight = nil
				}
			}
		case journal.TypeUpdateAccepted:
			var p updateAcceptedPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.ID == "" || completed[p.ID] {
				return
			}
			job := updateJob{name: e.Name, id: p.ID, accepted: true}
			// Attach input from earlier request if we already saw it.
			for i := range requested {
				if requested[i].id == p.ID {
					job.input = requested[i].input
					break
				}
			}
			inFlight = &job
		case journal.TypeUpdateRequested:
			var p updateRequestPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.ID == "" || completed[p.ID] {
				return
			}
			job := updateJob{
				name:  e.Name,
				id:    p.ID,
				input: append([]byte(nil), p.Input...),
			}
			if inFlight != nil && inFlight.id == p.ID && len(inFlight.input) == 0 {
				inFlight.input = job.input
			}
			// Skip if already accepted (in flight).
			if inFlight != nil && inFlight.id == p.ID {
				return
			}
			for _, r := range requested {
				if r.id == p.ID {
					return
				}
			}
			requested = append(requested, job)
		}
	}
	for _, e := range c.events {
		consider(e)
	}
	for _, e := range c.commands {
		consider(e)
	}

	if inFlight != nil && !completed[inFlight.id] {
		if len(inFlight.input) == 0 {
			for _, e := range c.events {
				if e.Type != journal.TypeUpdateRequested {
					continue
				}
				var p updateRequestPayload
				_ = json.Unmarshal(e.Payload, &p)
				if p.ID == inFlight.id {
					inFlight.input = append([]byte(nil), p.Input...)
					break
				}
			}
		}
		return inFlight
	}
	for i := range requested {
		if !completed[requested[i].id] {
			j := requested[i]
			return &j
		}
	}
	return nil
}

// FindUpdateCompletion returns the completed payload for id, if present in events.
func FindUpdateCompletion(events []journal.Event, id string) (result []byte, errMsg string, ok bool) {
	for _, e := range events {
		if e.Type != journal.TypeUpdateCompleted {
			continue
		}
		var p updateCompletedPayload
		_ = json.Unmarshal(e.Payload, &p)
		if p.ID != id {
			continue
		}
		return p.Result, p.Error, true
	}
	return nil, "", false
}

// UpdateAcceptedInFlight reports whether id was accepted but not completed.
func UpdateAcceptedInFlight(events []journal.Event, id string) bool {
	accepted, completed := false, false
	for _, e := range events {
		switch e.Type {
		case journal.TypeUpdateAccepted:
			var p updateAcceptedPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.ID == id {
				accepted = true
			}
		case journal.TypeUpdateCompleted:
			var p updateCompletedPayload
			_ = json.Unmarshal(e.Payload, &p)
			if p.ID == id {
				completed = true
			}
		}
	}
	return accepted && !completed
}

// FormatUpdateError builds a stable error for completed updates with Error set.
func FormatUpdateError(msg string) error {
	if msg == "" {
		return fmt.Errorf("update failed")
	}
	return fmt.Errorf("%s", msg)
}
