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
		ID:    o.id,
		Name:  workflowName,
		Queue: o.queue,
		Input: payload,
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

func (c *Client) Signal(ctx context.Context, id, name string, payload any) error {
	body, err := c.codec.Marshal(payload)
	if err != nil {
		return err
	}
	return c.backend.SendToInbox(ctx, id, journal.Event{
		Type:    journal.TypeSignalReceived,
		Name:    name,
		Payload: body,
	})
}

func (c *Client) Cancel(ctx context.Context, id string) error {
	return c.backend.SendToInbox(ctx, id, journal.Event{
		Type: journal.TypeCancelRequested,
	})
}

func (c *Client) Terminate(ctx context.Context, id string) error {
	return c.backend.TerminateInstance(ctx, id)
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
	for {
		inst, err := h.client.backend.GetInstance(ctx, h.id)
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
		}
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-ticker.C:
		}
	}
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
