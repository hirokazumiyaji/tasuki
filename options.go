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
	WorkflowConcurrency  int
	IdleInstanceLockTTL  time.Duration // evict unused per-instance locks; <=0 → 10m
	StickyJournalTTL     time.Duration // evict unused sticky journal entries; <=0 → 10m
	WorkerID             string
	Codec                codec.Codec
	Logger               *slog.Logger
	Metrics              *observability.Metrics
	JournalWarnThreshold int // 0 → 10000; <0 disabled
	// IncompatibleRetryDelay is how long to hide a task after an incompatible Worker nacks it.
	// Unset (0) defaults to 5s; negative means immediate re-visibility.
	IncompatibleRetryDelay time.Duration
	// MaxPerInstance caps how many tasks of one instance a single claim batch
	// returns (fair dispatch; see docs/08-fair-dispatch.md). 0 disables the
	// cap and keeps strict FIFO claiming.
	MaxPerInstance int
	// DisableSchemaValidation skips the startup check against backends that
	// implement backend.SchemaValidator. Validation is on by default: when the
	// store is missing tables (e.g. migrations have not run), Start logs an
	// error and does not launch the poll loop.
	DisableSchemaValidation bool
	// ShutdownReleaseTimeout bounds lease release during Shutdown.
	// Unreleased leases expire via lease timeout and are reclaimed by peers.
	// <=0 defaults to 5s.
	ShutdownReleaseTimeout time.Duration
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
	if o.WorkflowConcurrency <= 0 {
		o.WorkflowConcurrency = 1
	}
	if o.IdleInstanceLockTTL <= 0 {
		o.IdleInstanceLockTTL = 10 * time.Minute
	}
	if o.StickyJournalTTL <= 0 {
		o.StickyJournalTTL = 10 * time.Minute
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
	if o.JournalWarnThreshold == 0 {
		o.JournalWarnThreshold = 10000
	}
	if o.IncompatibleRetryDelay == 0 {
		o.IncompatibleRetryDelay = 5 * time.Second
	} else if o.IncompatibleRetryDelay < 0 {
		o.IncompatibleRetryDelay = 0
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
	id               string
	queue            string
	searchAttributes map[string]string
	memo             map[string]string
}

type StartOption func(*startOptions)

func WithID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

func WithQueue(queue string) StartOption {
	return func(o *startOptions) { o.queue = queue }
}

// WithSearchAttributes sets string key/value visibility metadata on Start.
// Values are exact-match filters for Client.List. Pass a copy; the map is not retained.
func WithSearchAttributes(attrs map[string]string) StartOption {
	return func(o *startOptions) {
		if len(attrs) == 0 {
			return
		}
		o.searchAttributes = make(map[string]string, len(attrs))
		for k, v := range attrs {
			if v == "" {
				continue
			}
			o.searchAttributes[k] = v
		}
	}
}

// WithMemo sets display-only string notes on Start (visible on Get, not List filters).
func WithMemo(attrs map[string]string) StartOption {
	return func(o *startOptions) {
		if len(attrs) == 0 {
			return
		}
		o.memo = make(map[string]string, len(attrs))
		for k, v := range attrs {
			if v == "" {
				continue
			}
			o.memo[k] = v
		}
	}
}

type signalOptions struct {
	dedupeID string
}

// SignalOption configures Client.Signal.
type SignalOption func(*signalOptions)

// WithDedupeID makes Signal idempotent for this instance: retries with the same
// ID do not deliver a second copy. Empty ID is ignored (same as omitting the option).
func WithDedupeID(id string) SignalOption {
	return func(o *signalOptions) { o.dedupeID = id }
}
