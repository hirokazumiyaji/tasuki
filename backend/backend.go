package backend

import (
	"context"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Backend is the store contract used by Client and Worker.
type Backend interface {
	Migrate(ctx context.Context) error
	Capabilities() Capabilities

	CreateInstance(ctx context.Context, inst NewInstance) error
	GetInstance(ctx context.Context, id string) (*Instance, error)
	GetJournal(ctx context.Context, id string, afterSeq int64) ([]journal.Event, error)
	ListInstances(ctx context.Context, f InstanceFilter) ([]Instance, error)
	TerminateInstance(ctx context.Context, id string) error
	// SendToInbox appends an event to the instance inbox.
	// Non-empty dedupeID makes the send idempotent per (instanceID, dedupeID):
	// a duplicate returns nil without inserting another inbox row.
	SendToInbox(ctx context.Context, instanceID string, ev journal.Event, dedupeID string) error
	// SendToInboxBatch appends items atomically. Per-item dedupe hits are skipped
	// (not an error). Empty items is a no-op. Oversized batches return ErrBatchTooLarge.
	SendToInboxBatch(ctx context.Context, instanceID string, items []InboxItem) error

	ClaimTasks(ctx context.Context, req ClaimRequest) ([]Task, error)
	// CountClaimableTasks returns per-queue counts of tasks with visible_at <= store now
	// for kind among queues. Queues with zero may be omitted.
	CountClaimableTasks(ctx context.Context, kind string, queues []string) (map[string]int64, error)
	ExtendLease(ctx context.Context, taskID int64, d time.Duration) error
	// RecordHeartbeat extends the lease and stores details for GetHeartbeatDetails on later attempts.
	RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error
	ReleaseLease(ctx context.Context, taskID int64) error
	// NackTask clears the lease and defers visibility by delay (store clock).
	// Used when a Worker cannot process the task (incompatible code/registry).
	NackTask(ctx context.Context, t Task, delay time.Duration) error
	LoadWorkflow(ctx context.Context, instanceID string) (*WorkflowState, error)
	// LoadWorkflowHead returns instance metadata, inbox, next_seq, and store Now without journal.
	LoadWorkflowHead(ctx context.Context, instanceID string) (*WorkflowState, error)
	CommitAdvancement(ctx context.Context, adv Advancement) error
	CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error
	// RetryActivity clears the lease and defers activity visibility by delay (store clock).
	RetryActivity(ctx context.Context, taskID int64, delay time.Duration) error
	FireDueTimers(ctx context.Context, limit int) (int, error)

	UpsertSchedule(ctx context.Context, s NewSchedule) error
	GetSchedule(ctx context.Context, id string) (*Schedule, error)
	PauseSchedule(ctx context.Context, id string, paused bool) error
	ClaimDueSchedules(ctx context.Context, limit int) ([]DueSchedule, error)

	// PurgeInstances permanently deletes up to limit terminal instances whose
	// completion time is at least olderThan in the past, together with every
	// dependent row (journal events, inbox items, tasks, timers, signal
	// dedupe). An empty statuses slice defaults to
	// {completed, failed, terminated, canceled}; non-terminal or unknown
	// statuses are rejected. It returns the number of instances purged.
	PurgeInstances(ctx context.Context, olderThan time.Duration, statuses []string, limit int) (int, error)
}

// AdvancementBatcher optionally commits many workflow advancements in one round-trip.
// When unimplemented, workers fall back to looping CommitAdvancement.
type AdvancementBatcher interface {
	CommitAdvancements(ctx context.Context, advs []Advancement) error
}

// SchemaValidator is implemented by backends that can verify the store schema
// is ready (tables present, version current). Workers call it through
// tasuki.ValidateSchema at startup as a fail-safe for unmigrated databases.
type SchemaValidator interface {
	ValidateSchema(ctx context.Context) error
}
