package activity

import (
	"context"
	"errors"
	"fmt"

	"github.com/hirokazumiyaji/tasuki/codec"
)

// ErrNoDetails is returned by GetHeartbeatDetails when no heartbeat was recorded.
var ErrNoDetails = errors.New("activity: no heartbeat details")

// Info is execution metadata for the current activity attempt.
type Info struct {
	InstanceID     string
	ActivityName   string
	Attempt        int // 1-based
	TaskID         int64
	IdempotencyKey string
}

type envKey struct{}

// Env is injected by the worker; not for application construction.
type Env struct {
	Info    Info
	Codec   codec.Codec
	Details []byte // from prior attempt
	Record  func(ctx context.Context, details []byte) error
}

// WithEnv attaches Env to ctx.
func WithEnv(ctx context.Context, env *Env) context.Context {
	return context.WithValue(ctx, envKey{}, env)
}

func envFrom(ctx context.Context) (*Env, bool) {
	e, ok := ctx.Value(envKey{}).(*Env)
	return e, ok && e != nil
}

// GetInfo returns activity metadata. Zero value if not running under a worker.
func GetInfo(ctx context.Context) Info {
	if e, ok := envFrom(ctx); ok {
		return e.Info
	}
	return Info{}
}

// RecordHeartbeat extends the activity lease and stores details for retries.
func RecordHeartbeat(ctx context.Context, details any) error {
	e, ok := envFrom(ctx)
	if !ok || e.Record == nil {
		return fmt.Errorf("activity: RecordHeartbeat outside activity")
	}
	var payload []byte
	if details != nil {
		c := e.Codec
		if c == nil {
			c = codec.JSON()
		}
		b, err := c.Marshal(details)
		if err != nil {
			return err
		}
		payload = b
	}
	return e.Record(ctx, payload)
}

// GetHeartbeatDetails unmarshals the last heartbeat details into dest.
func GetHeartbeatDetails(ctx context.Context, dest any) error {
	e, ok := envFrom(ctx)
	if !ok {
		return ErrNoDetails
	}
	if len(e.Details) == 0 {
		return ErrNoDetails
	}
	c := e.Codec
	if c == nil {
		c = codec.JSON()
	}
	return c.Unmarshal(e.Details, dest)
}
