package tasuki

import (
	"log/slog"
	"time"

	"github.com/hirokazumiyaji/tasuki/codec"
	"github.com/hirokazumiyaji/tasuki/observability"
)

type WorkerOptions struct {
	Queues               []string
	PollInterval         time.Duration
	LeaseDuration        time.Duration
	ClaimLimit           int
	ActivityConcurrency  int
	WorkerID             string
	Codec                codec.Codec
	Logger               *slog.Logger
	Metrics              *observability.Metrics
}

func (o WorkerOptions) withDefaults() WorkerOptions {
	if len(o.Queues) == 0 {
		o.Queues = []string{"default"}
	}
	if o.PollInterval == 0 {
		o.PollInterval = time.Second
	}
	if o.LeaseDuration == 0 {
		o.LeaseDuration = 30 * time.Second
	}
	if o.ClaimLimit <= 0 {
		o.ClaimLimit = 10
	}
	if o.ActivityConcurrency <= 0 {
		o.ActivityConcurrency = 1
	}
	if o.WorkerID == "" {
		o.WorkerID = "worker"
	}
	if o.Codec == nil {
		o.Codec = codec.JSON()
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

type registerOptions struct {
	name string
}

type RegisterOption func(*registerOptions)

func WithName(name string) RegisterOption {
	return func(o *registerOptions) { o.name = name }
}

type ClientOption func(*Client)

// WithCodec sets the codec used for inputs, signals, and results.
// It must match the codec configured on the workers.
func WithCodec(c codec.Codec) ClientOption {
	return func(cl *Client) { cl.codec = c }
}

type startOptions struct {
	id    string
	queue string
}

type StartOption func(*startOptions)

func WithID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

func WithQueue(queue string) StartOption {
	return func(o *startOptions) { o.queue = queue }
}
