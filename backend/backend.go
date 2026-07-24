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
	SendToInbox(ctx context.Context, instanceID string, ev journal.Event) error

	ClaimTasks(ctx context.Context, req ClaimRequest) ([]Task, error)
	ExtendLease(ctx context.Context, taskID int64, d time.Duration) error
	ReleaseLease(ctx context.Context, taskID int64) error
	LoadWorkflow(ctx context.Context, instanceID string) (*WorkflowState, error)
	// LoadWorkflowHead returns instance metadata, inbox, next_seq, and store Now without journal.
	LoadWorkflowHead(ctx context.Context, instanceID string) (*WorkflowState, error)
	CommitAdvancement(ctx context.Context, adv Advancement) error
	CompleteActivity(ctx context.Context, taskID int64, ev journal.Event) error
	RetryActivity(ctx context.Context, taskID int64, visibleAt time.Time) error
	FireDueTimers(ctx context.Context, limit int) (int, error)

	UpsertSchedule(ctx context.Context, s NewSchedule) error
	GetSchedule(ctx context.Context, id string) (*Schedule, error)
	PauseSchedule(ctx context.Context, id string, paused bool) error
	ClaimDueSchedules(ctx context.Context, limit int) ([]DueSchedule, error)
}
