package backend

import (
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type Capabilities struct {
	MaxAdvancementEffects int // 0 = unlimited
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
	ID     string
	Name   string
	Queue  string
	Status string
	Input  []byte
	Result []byte
	Failure []byte
	NextSeq int64
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
	ID         int64
	Kind       string
	Queue      string
	InstanceID string
	Name       string
	Seq        int64
	Input      []byte
	Attempt    int
	VisibleAt  time.Time
	WorkerID   string
}

type ClaimRequest struct {
	Kind     string
	Queues   []string
	Limit    int
	Lease    time.Duration
	WorkerID string
}

type NewTask struct {
	Kind       string
	Queue      string
	InstanceID string
	Name       string
	Seq        int64
	Input      []byte
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
