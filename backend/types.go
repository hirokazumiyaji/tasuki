package backend

import (
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type Capabilities struct {
	MaxAdvancementEffects int // 0 = unlimited
}

// InstanceFilter selects instances for ListInstances.
type InstanceFilter struct {
	Status string // empty = any
	Name   string // empty = any
	Limit  int    // 0 = default 100
	Offset int
}

type NewInstance struct {
	ID       string
	Name     string
	Queue    string
	Input    []byte
	ParentID string
	ParentSeq int64
}

type Instance struct {
	ID        string
	Name      string
	Queue     string
	Status    string
	Input     []byte
	Result    []byte
	Failure   []byte
	NextSeq   int64
	ParentID  string
	ParentSeq int64
}

type WorkflowState struct {
	Instance Instance
	Journal  []journal.Event
	Inbox    []InboxEvent
	NextSeq  int64
	Now      time.Time
}

type InboxEvent struct {
	ID      int64
	Event   journal.Event
}

type Task struct {
	ID          int64
	Kind        string
	Queue       string
	InstanceID  string
	Name        string
	Seq         int64
	Input       []byte
	Attempt     int
	MaxAttempts int
	Retry       RetryPolicy
	VisibleAt   time.Time
	WorkerID    string
}

// RetryPolicy is stored on activity tasks for worker-side backoff.
type RetryPolicy struct {
	InitialInterval    time.Duration
	BackoffCoefficient float64
	MaxInterval        time.Duration
	MaxAttempts        int
}

type ClaimRequest struct {
	Kind     string
	Queues   []string
	Limit    int
	Lease    time.Duration
	WorkerID string
}

type NewTask struct {
	Kind        string
	Queue       string
	InstanceID  string
	Name        string
	Seq         int64
	Input       []byte
	MaxAttempts int // 0 = unlimited
	Retry       RetryPolicy
}

type NewTimer struct {
	Seq    int64
	FireAt time.Time
}

type TerminalUpdate struct {
	Status  string
	Result  []byte
	Failure []byte
}

type Advancement struct {
	InstanceID    string
	TaskID        int64
	ExpectedSeq   int64
	DrainedInbox  []int64
	NewEvents     []journal.Event
	ActivityTasks []NewTask
	Timers        []NewTimer
	Children      []NewInstance
	ParentNotify  *journal.Event
	Terminal      *TerminalUpdate
	EnsureWorkflowTask bool // if true, enqueue workflow task after commit when inbox remains or always for M0 helpers
}
