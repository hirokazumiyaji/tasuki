package tasuki

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/codec"
)

var ErrAlreadyStarted = errors.New("workflow already started")

type Client struct {
	backend backend.Backend
	codec   codec.Codec
}

func NewClient(b backend.Backend) *Client {
	return &Client{backend: b, codec: codec.JSON()}
}

type Handle struct {
	client *Client
	id     string
}

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

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
