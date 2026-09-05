package tasuki

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/codec"
)

var (
	ErrAlreadyStarted = errors.New("workflow already started")
	ErrTerminated     = errors.New("workflow terminated")
	ErrFailed         = errors.New("workflow failed")
	ErrStuck          = errors.New("workflow stuck")
	ErrCanceled       = errors.New("workflow canceled")
)

type Client struct {
	backend      backend.Backend
	codec        codec.Codec
	pollInterval time.Duration
}

func NewClient(b backend.Backend, opts ...ClientOption) *Client {
	c := &Client{backend: b, codec: codec.JSON(), pollInterval: 200 * time.Millisecond}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

type Handle struct {
	client *Client
	id     string
}

func (h *Handle) ID() string { return h.id }

func Start[I any](ctx context.Context, c *Client, workflowName string, input I, opts ...StartOption) (*Handle, error) {
	o := startOptions{queue: "default"}
	for _, opt := range opts {
		opt(&o)
	}
	if o.id == "" {
		o.id = newID()
	}
	payload, err := c.codec.Marshal(input)
	if err != nil {
		return nil, err
	}
	err = c.backend.CreateInstance(ctx, backend.NewInstance{
		ID:               o.id,
		Name:             workflowName,
		Queue:            o.queue,
		Input:            payload,
		SearchAttributes: o.searchAttributes,
		Memo:             o.memo,
	})
	h := &Handle{client: c, id: o.id}
	if errors.Is(err, backend.ErrAlreadyExists) {
		return h, ErrAlreadyStarted
	}
	if err != nil {
		return nil, err
	}
	return h, nil
}

func (c *Client) Get(ctx context.Context, id string) (*backend.Instance, error) {
	return c.backend.GetInstance(ctx, id)
}

// GetJournal returns the full journal for an instance (seq > 0).
func (c *Client) GetJournal(ctx context.Context, id string) ([]journal.Event, error) {
	return c.backend.GetJournal(ctx, id, 0)
}

// InstanceFilter is re-exported for Client.List callers.
type InstanceFilter = backend.InstanceFilter

const (
	StatusRunning    = "running"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusTerminated = "terminated"
	StatusCanceled   = "canceled"
	StatusStuck      = "stuck"
	StatusContinued  = "continued"
)

func (c *Client) List(ctx context.Context, f InstanceFilter) ([]backend.Instance, error) {
	return c.backend.ListInstances(ctx, f)
}

func (c *Client) Signal(ctx context.Context, id, name string, payload any, opts ...SignalOption) error {
	o := signalOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	return c.SignalBatch(ctx, id, []SignalItem{{
		Name:     name,
		Payload:  payload,
		DedupeID: o.dedupeID,
	}})
}

// SignalItem is one signal for SignalBatch.
type SignalItem struct {
	Name     string
	Payload  any
	DedupeID string // optional; empty means no dedupe for this item
}

// SignalBatch appends multiple signals to one instance atomically.
func (c *Client) SignalBatch(ctx context.Context, id string, items []SignalItem) error {
	if len(items) == 0 {
		return nil
	}
	batch := make([]backend.InboxItem, 0, len(items))
	for _, it := range items {
		body, err := c.codec.Marshal(it.Payload)
		if err != nil {
			return err
		}
		batch = append(batch, backend.InboxItem{
			Event: journal.Event{
				Type:    journal.TypeSignalReceived,
				Name:    it.Name,
				Payload: body,
			},
			DedupeID: it.DedupeID,
		})
	}
	return c.backend.SendToInboxBatch(ctx, id, batch)
}

func (c *Client) Cancel(ctx context.Context, id string) error {
	// Forward to the tail of a ContinueAsNew chain so waiting on the
	// original Handle observes cancellation.
	headErr := c.backend.SendToInbox(ctx, id, journal.Event{
		Type: journal.TypeCancelRequested,
	}, "")
	tail, terr := c.resolveContinuedTail(ctx, id)
	if terr == nil && tail != "" && tail != id {
		_ = c.backend.SendToInbox(ctx, tail, journal.Event{
			Type: journal.TypeCancelRequested,
		}, "")
		if headErr != nil {
			// Head may already be "continued" (still cancelable via tail).
			return nil
		}
	}
	return headErr
}

func (c *Client) Terminate(ctx context.Context, id string) error {
	if err := c.backend.TerminateInstance(ctx, id); err != nil {
		return err
	}
	// Also terminate the continued tail so Result does not hang on the child.
	if tail, err := c.resolveContinuedTail(ctx, id); err == nil && tail != "" && tail != id {
		_ = c.backend.TerminateInstance(ctx, tail)
	}
	return nil
}

// resolveContinuedTail follows "continued" statuses to the running tail.
// Returns "" when there is no continuation.
func (c *Client) resolveContinuedTail(ctx context.Context, id string) (string, error) {
	cur := id
	for i := 0; i < 32; i++ {
		inst, err := c.backend.GetInstance(ctx, cur)
		if err != nil {
			if cur == id {
				return "", err
			}
			return cur, nil
		}
		if inst.Status != StatusContinued && inst.Status != "continued" {
			if cur == id {
				return "", nil
			}
			return cur, nil
		}
		next, err := continuedChildID(ctx, c.backend, cur)
		if err != nil || next == "" {
			return cur, err
		}
		cur = next
	}
	return cur, nil
}

func (c *Client) UpsertSchedule(ctx context.Context, s backend.NewSchedule) error {
	return c.backend.UpsertSchedule(ctx, s)
}

func (c *Client) GetSchedule(ctx context.Context, id string) (*backend.Schedule, error) {
	return c.backend.GetSchedule(ctx, id)
}

func (c *Client) PauseSchedule(ctx context.Context, id string, paused bool) error {
	return c.backend.PauseSchedule(ctx, id, paused)
}

// Result polls until the workflow reaches a terminal status and returns the typed output.
func Result[O any](ctx context.Context, h *Handle) (O, error) {
	var zero O
	ticker := time.NewTicker(h.client.pollInterval)
	defer ticker.Stop()

	// Use a derived context for the terminal subscription so every exit path
	// releases the subscriber/goroutine (and Postgres LISTEN connection)
	// without cancelling the caller's context.
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wake <-chan string
	if n, ok := h.client.backend.(backend.TerminalNotifier); ok {
		ch, err := n.SubscribeTerminal(subCtx)
		if err == nil {
			wake = ch
		}
	}

	currentID := h.id
	for {
		inst, err := h.client.backend.GetInstance(ctx, currentID)
		if err != nil {
			return zero, err
		}
		switch inst.Status {
		case "completed":
			var out O
			if err := h.client.codec.Unmarshal(inst.Result, &out); err != nil {
				return zero, err
			}
			return out, nil
		case "failed":
			return zero, fmt.Errorf("%w: %s", ErrFailed, string(inst.Failure))
		case "terminated":
			return zero, ErrTerminated
		case "canceled":
			return zero, ErrCanceled
		case "stuck":
			return zero, fmt.Errorf("%w: %s", ErrStuck, string(inst.Failure))
		case StatusContinued:
			// Follow ContinueAsNew chains so the original Handle observes
			// the final result. The child ID is "<parent>~<continued-seq>";
			// resolve it via the journal to avoid guessing.
			next, cerr := continuedChildID(ctx, h.client.backend, currentID)
			if cerr != nil {
				// Journal not yet visible or transient: keep polling.
				break
			}
			if next != "" && next != currentID {
				currentID = next
				continue
			}
		}
		if wake == nil {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-ticker.C:
			}
			continue
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-ticker.C:
		case id := <-wake:
			if id != "" && id != currentID && id != h.id {
				// Ignore unrelated terminals but re-check current in case
				// the wake was coalesced.
				continue
			}
		}
	}
}

// continuedChildID resolves the ContinueAsNew child for a "continued"
// instance by reading its journal for TypeContinuedAsNew.
func continuedChildID(ctx context.Context, b backend.Backend, id string) (string, error) {
	j, err := b.GetJournal(ctx, id, 0)
	if err != nil {
		return "", err
	}
	var seq int64
	for _, e := range j {
		if string(e.Type) == string(journal.TypeContinuedAsNew) {
			if e.Seq > seq {
				seq = e.Seq
			}
		}
	}
	if seq == 0 {
		return "", nil
	}
	return fmt.Sprintf("%s~%d", id, seq), nil
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
