package tasuki_test

import (
	"context"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
)

func TestClient_ScheduleGetPause(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := tasuki.NewClient(b)

	if err := c.UpsertSchedule(ctx, backend.NewSchedule{
		ID: "hourly", Cron: "0 * * * *", Workflow: "job", Queue: "default",
	}); err != nil {
		t.Fatal(err)
	}
	s, err := c.GetSchedule(ctx, "hourly")
	if err != nil {
		t.Fatal(err)
	}
	if s.ID != "hourly" || s.Paused {
		t.Fatalf("%+v", s)
	}
	if err := c.PauseSchedule(ctx, "hourly", true); err != nil {
		t.Fatal(err)
	}
	s2, err := c.GetSchedule(ctx, "hourly")
	if err != nil {
		t.Fatal(err)
	}
	if !s2.Paused {
		t.Fatal("want paused")
	}
}

func TestStart_WithQueue(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	b.SetNow(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	c := tasuki.NewClient(b)
	h, err := tasuki.Start(ctx, c, "WF", struct{}{},
		tasuki.WithID("q-1"),
		tasuki.WithQueue("special"),
	)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Get(ctx, h.ID())
	if err != nil {
		t.Fatal(err)
	}
	if info.Queue != "special" {
		t.Fatalf("queue=%q", info.Queue)
	}
}
