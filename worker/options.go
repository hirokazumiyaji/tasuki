package worker

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
	// It also backs off failed workflow tasks: non-contention handle/commit
	// errors (store failures, oversized-advancement budget diagnostics) nack
	// with this delay instead of an immediate lease release, so a
	// persistently failing task does not reclaim-fail-notify in a tight loop.
	IncompatibleRetryDelay time.Duration
	// MaxPerInstance caps how many tasks of one instance a single claim batch
	// returns (fair dispatch; see docs/08-fair-dispatch.md). 0 disables the
	// cap and keeps strict FIFO claiming.
	MaxPerInstance int
	// DisableSchemaValidation skips the startup check against backends that
	// implement backend.SchemaValidator. Validation is on by default: when the
	// store is missing tables (e.g. migrations have not run), StartWithError
	// returns an error and the poll loop is not launched (Start logs the same
	// error and leaves the worker stopped; see Worker.Running).
	DisableSchemaValidation bool
	// ShutdownReleaseTimeout bounds lease release during Shutdown.
	// Unreleased leases expire via lease timeout and are reclaimed by peers.
	// <=0 defaults to 5s.
	ShutdownReleaseTimeout time.Duration
	// CommitTimeout bounds detached result commits (CompleteActivity,
	// RetryActivity, activity failure records, workflow advancement flush)
	// during normal operation. It is independent of ShutdownReleaseTimeout
	// so a short shutdown-only bound (e.g. 50ms) cannot cancel ordinary
	// commits and force re-execution. Once Shutdown has begun, commits
	// racing shutdown are instead bounded by ShutdownReleaseTimeout to keep
	// shutdown predictable. <=0 defaults to 30s.
	CommitTimeout time.Duration
	// LocalActivityTimeout bounds one ExecuteLocal invocation. The activity
	// runs with a cancellable context; a call that ignores cancellation may
	// continue in its goroutine, but its result is discarded after the limit.
	// <=0 disables the limit.
	LocalActivityTimeout time.Duration
	// BacklogSampleInterval throttles backlog gauge sampling (2x
	// CountClaimableTasks per sample when Metrics is set). 0 defaults to
	// 10s (coarser than the 1s PollInterval default); negative disables
	// sampling entirely. Set a smaller positive value (e.g. time.Second)
	// for tests or low-traffic stores that want fresher backlog gauges.
	BacklogSampleInterval time.Duration
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
	if o.BacklogSampleInterval == 0 {
		o.BacklogSampleInterval = 10 * time.Second
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
