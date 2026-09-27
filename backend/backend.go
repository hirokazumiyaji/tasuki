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
	// ExtendLease pushes a claimed task's visibility out by d (store clock).
	// The task carries kind/instance routing: DynamoDB/Firestore store
	// workflow tasks under WF#<instanceID>, not ACT#<id>, so renewing by
	// numeric ID alone misses workflow tasks (ErrNotFound) and long replays
	// lose their lease to a peer (duplicate execution). Callers pass the
	// claimed task, mirroring NackTask.
	ExtendLease(ctx context.Context, t Task, d time.Duration) error
	// RecordHeartbeat extends the lease and stores details for GetHeartbeatDetails on later attempts.
	RecordHeartbeat(ctx context.Context, taskID int64, lease time.Duration, details []byte) error
	// ReleaseLease makes a claimed task immediately reclaimable.
	// The task carries the claim ownership token (ID, Kind, InstanceID,
	// WorkerID, Attempt): backends route workflow tasks by
	// (Kind, InstanceID) — DynamoDB/Firestore store them under WF#<instanceID>,
	// not ACT#<id>, so releasing by numeric ID alone misses workflow tasks
	// (ErrNotFound) and a shutdown abandon stalls peers until lease expiry.
	// Callers pass the claimed task, mirroring ExtendLease/NackTask.
	//
	// Releases are fenced to the claimed generation (worker_id + attempt):
	// when the lease moved on (peer reclaim after a delayed renewal, or a
	// successor turn), the release has no effect and reports ErrNotFound.
	// Workers treat that as already-released, not an error. A zero WorkerID
	// falls back to unconditional release by ID for legacy callers.
	ReleaseLease(ctx context.Context, t Task) error
	// NackTask clears the lease and defers visibility by delay (store clock).
	// Used when a Worker cannot process the task (incompatible code/registry).
	// The task carries the claim ownership token (ID, Kind, InstanceID,
	// WorkerID, Attempt): backends nack conditionally on the token (worker +
	// attempt, plus numeric id on WF keys) so a stale worker never clears a
	// newer worker's lease after a reclaim race. A mismatch (reclaimed,
	// refreshed, or already committed task) reports ErrNotFound without
	// touching the peer lease, which callers ignore as best-effort. A zero
	// WorkerID falls back to unconditional nack by ID for legacy callers.
	NackTask(ctx context.Context, t Task, delay time.Duration) error
	LoadWorkflow(ctx context.Context, instanceID string) (*WorkflowState, error)
	// LoadWorkflowHead returns instance metadata, inbox, next_seq, and store Now without journal.
	LoadWorkflowHead(ctx context.Context, instanceID string) (*WorkflowState, error)
	CommitAdvancement(ctx context.Context, adv Advancement) error
	CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error
	// RetryActivity clears the lease and defers activity visibility by delay (store clock).
	// It addresses the task by ID alone (no worker/attempt fencing):
	// the worker-side lease-loss abandon (see handleActivity) is the
	// primary defense — a stale worker whose lease moved on skips the
	// call instead of touching a peer's task. A post-return race (loss
	// after the result but before the store op) can still reach the
	// store; release/nack/renewal are fenced on the claim token, while
	// Complete/Retry stay ID-only on every backend (changing them would
	// break the store contract) and rely on that abandon check.
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
