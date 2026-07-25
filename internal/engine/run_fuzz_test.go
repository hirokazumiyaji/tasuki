package engine_test

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/internal/engine"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func FuzzRunSyntheticJournal(f *testing.F) {
	f.Add([]byte{0})
	f.Add([]byte{1, 2, 3, 4, 5})
	f.Add([]byte{0, 1, 0, 1, 0, 1, 0, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		events := []journal.Event{
			{Seq: 1, Type: journal.TypeWorkflowStarted, Name: "fuzz"},
		}
		seq := int64(2)
		n := len(data)
		if n > 16 {
			n = 16
		}
		for i := 0; i < n; i++ {
			switch data[i] % 4 {
			case 0:
				events = append(events, journal.Event{Seq: seq, Type: journal.TypeSideEffect, Payload: []byte(`0`)})
				seq++
			case 1:
				events = append(events, journal.Event{Seq: seq, Type: journal.TypeTimerCreated})
				seq++
				events = append(events, journal.Event{Seq: seq, Type: journal.TypeTimerFired, RefSeq: seq - 1})
				seq++
			case 2:
				events = append(events, journal.Event{Seq: seq, Type: journal.TypeNowRecorded, Payload: []byte(`"2026-01-01T00:00:00Z"`)})
				seq++
			default:
				// leave gap / noise type that may cause stuck — still must not panic
				events = append(events, journal.Event{Seq: seq, Type: journal.TypeSignalReceived, Name: "x", Payload: []byte(`{}`)})
				seq++
			}
		}

		_ = engine.Run(events, func(ctx *workflow.Context) (any, error) {
			for i := 0; i < 3; i++ {
				if _, err := workflow.SideEffect(ctx, func() int { return i }); err != nil {
					return nil, err
				}
			}
			if err := workflow.Sleep(ctx, 0); err != nil {
				return nil, err
			}
			return "ok", nil
		})
	})
}
