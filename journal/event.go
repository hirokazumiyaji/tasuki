package journal

type Type string

const (
	TypeWorkflowStarted   Type = "workflow_started"
	TypeActivityScheduled Type = "activity_scheduled"
	TypeTimerCreated      Type = "timer_created"
	TypeChildScheduled    Type = "child_scheduled"
	TypeSideEffect               Type = "side_effect"
	TypeNowRecorded              Type = "now_recorded"
	TypeVersionMarker            Type = "version_marker"
	TypeSearchAttributesUpdated Type = "search_attributes_updated"
	TypeMemoUpdated             Type = "memo_updated"
	TypeActivityCompleted       Type = "activity_completed"
	TypeActivityFailed    Type = "activity_failed"
	TypeTimerFired        Type = "timer_fired"
	TypeChildCompleted    Type = "child_completed"
	TypeChildFailed       Type = "child_failed"
	TypeSignalReceived    Type = "signal_received"
	TypeCancelRequested   Type = "cancel_requested"
	TypeWorkflowCompleted Type = "workflow_completed"
	TypeWorkflowFailed    Type = "workflow_failed"
	TypeWorkflowCanceled  Type = "workflow_canceled"
	TypeContinuedAsNew    Type = "continued_as_new"
)

type Event struct {
	Seq     int64
	Type    Type
	Name    string // activity / signal / workflow name when applicable
	RefSeq  int64
	Payload []byte
}

func (t Type) IsCommand() bool {
	switch t {
	case TypeActivityScheduled, TypeTimerCreated, TypeChildScheduled, TypeSideEffect, TypeNowRecorded, TypeVersionMarker, TypeSearchAttributesUpdated, TypeMemoUpdated:
		return true
	default:
		return false
	}
}

func (t Type) IsCompletion() bool {
	switch t {
	case TypeActivityCompleted, TypeActivityFailed, TypeTimerFired, TypeChildCompleted, TypeChildFailed, TypeSignalReceived, TypeCancelRequested:
		return true
	default:
		return false
	}
}

func (t Type) IsTerminal() bool {
	switch t {
	case TypeWorkflowCompleted, TypeWorkflowFailed, TypeWorkflowCanceled, TypeContinuedAsNew:
		return true
	default:
		return false
	}
}
