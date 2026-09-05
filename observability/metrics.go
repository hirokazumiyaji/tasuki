package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const MeterName = "github.com/hirokazumiyaji/tasuki"

// Metrics holds OpenTelemetry instruments for the worker.
type Metrics struct {
	WorkflowTasks     metric.Int64Counter
	ActivityTasks     metric.Int64Counter
	WorkflowDone      metric.Int64Counter // completed / failed / stuck / canceled
	ActivityRetries   metric.Int64Counter
	JournalWarnings   metric.Int64Counter
	IncompatibleNacks metric.Int64Counter
	TaskBacklog       metric.Int64Gauge
	StoreErrors       metric.Int64Counter // worker store-operation failures by op
}

// NewMetrics creates counters on the global MeterProvider (noop if unset).
func NewMetrics() (*Metrics, error) {
	return NewMetricsWithMeter(otel.Meter(MeterName))
}

// NewMetricsWithMeter builds Metrics on an explicit Meter (test hook to use a
// ManualReader provider without touching global state).
func NewMetricsWithMeter(m metric.Meter) (*Metrics, error) {
	return newMetricsOn(m)
}

// newMetricsOn builds Metrics on an explicit Meter (avoids global state in tests).
func newMetricsOn(m metric.Meter) (*Metrics, error) {
	wt, err := m.Int64Counter("tasuki.workflow.tasks",
		metric.WithDescription("Workflow tasks processed"))
	if err != nil {
		return nil, err
	}
	at, err := m.Int64Counter("tasuki.activity.tasks",
		metric.WithDescription("Activity tasks processed"))
	if err != nil {
		return nil, err
	}
	wd, err := m.Int64Counter("tasuki.workflow.terminal",
		metric.WithDescription("Workflow terminal outcomes"),
		metric.WithUnit("{workflow}"))
	if err != nil {
		return nil, err
	}
	ar, err := m.Int64Counter("tasuki.activity.retries",
		metric.WithDescription("Activity retries scheduled"))
	if err != nil {
		return nil, err
	}
	jw, err := m.Int64Counter("tasuki.workflow.journal_warnings",
		metric.WithDescription("Workflow journal size warnings"))
	if err != nil {
		return nil, err
	}
	in, err := m.Int64Counter("tasuki.worker.incompatible_nacks",
		metric.WithDescription("Tasks nacked because this Worker cannot process them"))
	if err != nil {
		return nil, err
	}
	tb, err := m.Int64Gauge("tasuki.tasks.backlog",
		metric.WithDescription("Claimable tasks per queue"),
		metric.WithUnit("{task}"))
	if err != nil {
		return nil, err
	}
	se, err := m.Int64Counter("tasuki.worker.store_errors",
		metric.WithDescription("Worker store operation failures by op"))
	if err != nil {
		return nil, err
	}
	return &Metrics{
		WorkflowTasks:     wt,
		ActivityTasks:     at,
		WorkflowDone:      wd,
		ActivityRetries:   ar,
		JournalWarnings:   jw,
		IncompatibleNacks: in,
		TaskBacklog:       tb,
		StoreErrors:       se,
	}, nil
}

func (m *Metrics) AddWorkflowTask(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.WorkflowTasks.Add(ctx, n)
}

func (m *Metrics) AddActivityTask(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.ActivityTasks.Add(ctx, n)
}

func (m *Metrics) AddTerminal(ctx context.Context, status string) {
	if m == nil {
		return
	}
	m.WorkflowDone.Add(ctx, 1, metric.WithAttributes(attribute.String("status", status)))
}

func (m *Metrics) AddActivityRetry(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.ActivityRetries.Add(ctx, n)
}

func (m *Metrics) AddJournalWarning(ctx context.Context, n int64) {
	if m == nil {
		return
	}
	m.JournalWarnings.Add(ctx, n)
}

func (m *Metrics) AddIncompatibleNack(ctx context.Context, reason string) {
	if m == nil {
		return
	}
	m.IncompatibleNacks.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (m *Metrics) RecordBacklog(ctx context.Context, kind, queue string, n int64) {
	if m == nil {
		return
	}
	m.TaskBacklog.Record(ctx, n,
		metric.WithAttributes(
			attribute.String("kind", kind),
			attribute.String("queue", queue),
		),
	)
}

// AddStoreError counts a worker store-operation failure labeled by op.
// op is one of: fire_timers, claim_schedules, claim_workflow, claim_activity,
// commit_workflow, complete_activity, retry_activity, release_lease,
// extend_lease, heartbeat. Nil receiver is a no-op.
func (m *Metrics) AddStoreError(ctx context.Context, op string) {
	if m == nil {
		return
	}
	m.StoreErrors.Add(ctx, 1, metric.WithAttributes(attribute.String("op", op)))
}

// MustNewMetrics panics on instrument creation failure.
func MustNewMetrics() *Metrics {
	m, err := NewMetrics()
	if err != nil {
		panic(fmt.Errorf("observability: %w", err))
	}
	return m
}
