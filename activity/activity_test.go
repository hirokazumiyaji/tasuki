package activity_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hirokazumiyaji/tasuki/activity"
	"github.com/hirokazumiyaji/tasuki/codec"
)

func TestInfoAndHeartbeatRoundTrip(t *testing.T) {
	var got []byte
	ctx := activity.WithEnv(context.Background(), &activity.Env{
		Info: activity.Info{
			InstanceID: "i1", ActivityName: "a", Attempt: 2, TaskID: 9,
			IdempotencyKey: "i1/3",
		},
		Codec:   codec.JSON(),
		Details: []byte(`{"n":7}`),
		Record: func(ctx context.Context, details []byte) error {
			got = append([]byte(nil), details...)
			return nil
		},
	})
	info := activity.GetInfo(ctx)
	if info.InstanceID != "i1" || info.Attempt != 2 || info.IdempotencyKey != "i1/3" {
		t.Fatalf("info=%+v", info)
	}
	var d struct {
		N int `json:"n"`
	}
	if err := activity.GetHeartbeatDetails(ctx, &d); err != nil || d.N != 7 {
		t.Fatalf("details=%+v err=%v", d, err)
	}
	if err := activity.RecordHeartbeat(ctx, map[string]int{"n": 8}); err != nil {
		t.Fatal(err)
	}
	if string(got) == "" {
		t.Fatal("expected recorded payload")
	}
}

func TestGetHeartbeatDetails_None(t *testing.T) {
	ctx := activity.WithEnv(context.Background(), &activity.Env{Codec: codec.JSON()})
	err := activity.GetHeartbeatDetails(ctx, new(int))
	if !errors.Is(err, activity.ErrNoDetails) {
		t.Fatalf("got %v", err)
	}
}
