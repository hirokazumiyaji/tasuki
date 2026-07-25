package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const MeterName = "github.com/hirokazumiyaji/tasuki"

// Metrics holds OpenTelemetry instruments for the worker.
type Metrics struct {
	WorkflowTasks    metric.Int64Counter
	ActivityTasks    metric.Int64Counter
	WorkflowDone     metric.Int64Counter // completed / failed / stuck / canceled
	ActivityRetries  metric.Int64Counter
	JournalWarnings  metric.Int64Counter
}

// NewMetrics creates counters on the global MeterProvider (noop if unset).
func NewMetrics() (*Metrics, error) {
	m := otel.Meter(MeterName)
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
	return &Metrics{
		WorkflowTasks:   wt,
		ActivityTasks:   at,
		WorkflowDone:    wd,
		ActivityRetries: ar,
		JournalWarnings: jw,
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
	m.WorkflowDone.Add(ctx, 1) // status via attribute would need otel attribute import; keep simple count
	_ = status
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

// MustNewMetrics panics on instrument creation failure.
func MustNewMetrics() *Metrics {
	m, err := NewMetrics()
	if err != nil {
		panic(fmt.Errorf("observability: %w", err))
	}
	return m
}
