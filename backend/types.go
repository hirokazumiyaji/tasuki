package backend

import (
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type Capabilities struct {
	MaxAdvancementEffects int // 0 = unlimited
	// FairDispatch reports whether ClaimTasks honors ClaimRequest.MaxPerInstance.
	// Backends without support ignore the cap (FIFO) and the conformance
	// fair-dispatch test skips them explicitly.
	FairDispatch bool
	// CleansTerminalState reports whether terminal transitions fully clean
	// up: TerminateInstance and terminal CommitAdvancement remove pending
	// tasks, timers, and inbox rows, and FireDueTimers never creates inbox
	// rows or workflow tasks for non-running instances (see #290 #291).
	CleansTerminalState bool
	// SupportsBulkCleanup reports whether TerminateInstance and
	// PurgeInstances handle instances with more child rows (inbox, dedupe,
	// tasks, timers) than a single backend write transaction allows
	// (Firestore ~500 writes, Spanner mutation limits) by chunking the
	// deletes (see #299).
	SupportsBulkCleanup bool
	// SweepsTerminalInbox reports whether terminal CommitAdvancement omits
	// per-row DrainedInbox deletes from the commit transaction, sweeping
	// inbox rows post-commit (Firestore, Spanner) or via a single
	// whole-inbox purge (SQL, memory) instead. Backends that report false
	// (DynamoDB) delete each drained inbox row inside the terminal
	// transaction, so the worker budget must keep charging one op per
	// drained row for them (see #291).
	SweepsTerminalInbox bool
}

// InstanceFilter selects instances for ListInstances.
type InstanceFilter struct {
	Status           string // empty = any
	Name             string // empty = any
	SearchAttributes map[string]string // AND exact match; nil/empty = ignore
	Limit            int    // 0 = default 100
	Offset           int
}

type NewInstance struct {
	ID               string
	Name             string
	Queue            string
	Input            []byte
	ParentID         string
	ParentSeq        int64
	SearchAttributes map[string]string
	Memo             map[string]string
}

type Instance struct {
	ID               string
	Name             string
	Queue            string
	Status           string
	Input            []byte
	Result           []byte
	Failure          []byte
	NextSeq          int64
	ParentID         string
	ParentSeq        int64
	SearchAttributes map[string]string
	Memo             map[string]string
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
	ID                int64
	Kind              string
	Queue             string
	InstanceID        string
	Name              string
	Seq               int64
	Input             []byte
	Attempt           int
	MaxAttempts       int
	Retry             RetryPolicy
	StartToCloseTimeout time.Duration // 0 = unset
	VisibleAt         time.Time
	WorkerID          string
	HeartbeatDetails  []byte // last RecordHeartbeat payload; may be nil
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
	// MaxPerInstance caps how many tasks of one instance a single claim
	// returns, interleaving instances instead of strict FIFO (fair dispatch).
	// 0 disables the cap. Backends that do not advertise
	// Capabilities.FairDispatch ignore it and fall back to FIFO.
	MaxPerInstance int
}

type NewTask struct {
	Kind                string
	Queue               string
	InstanceID          string
	Name                string
	Seq                 int64
	Input               []byte
	MaxAttempts         int // 0 = unlimited
	Retry               RetryPolicy
	StartToCloseTimeout time.Duration // 0 = unset
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

// InboxItem is one event for SendToInboxBatch.
type InboxItem struct {
	Event    journal.Event
	DedupeID string
}

// NewSchedule is the input for UpsertSchedule.
type NewSchedule struct {
	ID       string
	Cron     string
	Workflow string
	Queue    string
	Input    []byte
	Paused   bool
}

// Schedule is a persisted cron trigger.
type Schedule struct {
	ID        string
	Cron      string
	Workflow  string
	Queue     string
	Input     []byte
	NextRunAt time.Time
	Paused    bool
}

// DueSchedule is a claimed schedule fire that started (or deduped) an instance.
type DueSchedule struct {
	Schedule
	InstanceID  string
	ScheduledAt time.Time
	Created     bool // false if instance already existed (dedup)
}
