package workflow_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func replaySideEffectEvents(n int) []journal.Event {
	events := make([]journal.Event, 0, n+1)
	events = append(events, journal.Event{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"})
	for i := 0; i < n; i++ {
		events = append(events, journal.Event{
			Seq:     int64(i + 2),
			Type:    journal.TypeSideEffect,
			Payload: []byte(`1`),
		})
	}
	return events
}

func replayExecuteEvents(n int) []journal.Event {
	events := make([]journal.Event, 0, 2*n+1)
	events = append(events, journal.Event{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"})
	for i := 0; i < n; i++ {
		sched := int64(2 + 2*i)
		comp := sched + 1
		events = append(events, journal.Event{Seq: sched, Type: journal.TypeActivityScheduled, Name: "act"})
		events = append(events, journal.Event{
			Seq:     comp,
			Type:    journal.TypeActivityCompleted,
			RefSeq:  sched,
			Payload: []byte(`"ok"`),
		})
	}
	return events
}

func replaySignalEvents(n int) []journal.Event {
	events := make([]journal.Event, 0, n+1)
	events = append(events, journal.Event{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "WF"})
	for i := 0; i < n; i++ {
		events = append(events, journal.Event{
			Seq:     int64(i + 2),
			Type:    journal.TypeSignalReceived,
			Name:    "sig",
			Payload: []byte(`1`),
		})
	}
	return events
}

func benchmarkReplaySideEffect(b *testing.B, n int) {
	b.Helper()
	events := replaySideEffectEvents(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
			for j := 0; j < n; j++ {
				if _, err := workflow.SideEffect[int](ctx, func() int { return 1 }); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if res.Err != nil {
			b.Fatalf("replay err: %v", res.Err)
		}
		if res.Suspended {
			b.Fatal("unexpected suspend")
		}
	}
}

func benchmarkReplayExecute(b *testing.B, n int) {
	b.Helper()
	events := replayExecuteEvents(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
			for j := 0; j < n; j++ {
				if _, err := workflow.Execute[int, string](ctx, "act", 1); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if res.Err != nil {
			b.Fatalf("replay err: %v", res.Err)
		}
		if res.Suspended {
			b.Fatal("unexpected suspend")
		}
	}
}

func benchmarkReplaySignal(b *testing.B, n int) {
	b.Helper()
	events := replaySignalEvents(n)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res := engine.Run(events, func(ctx *workflow.Context) (any, error) {
			for j := 0; j < n; j++ {
				if _, err := workflow.ReceiveSignal[int](ctx, "sig"); err != nil {
					return nil, err
				}
			}
			return nil, nil
		})
		if res.Err != nil {
			b.Fatalf("replay err: %v", res.Err)
		}
		if res.Suspended {
			b.Fatal("unexpected suspend")
		}
	}
}

func BenchmarkReplaySideEffect1000(b *testing.B)  { benchmarkReplaySideEffect(b, 1000) }
func BenchmarkReplaySideEffect10000(b *testing.B) { benchmarkReplaySideEffect(b, 10000) }

func BenchmarkReplayExecute1000(b *testing.B)  { benchmarkReplayExecute(b, 1000) }
func BenchmarkReplayExecute10000(b *testing.B) { benchmarkReplayExecute(b, 10000) }

func BenchmarkReplaySignal10000(b *testing.B) { benchmarkReplaySignal(b, 10000) }
