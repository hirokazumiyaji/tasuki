package workflow

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Now returns a durable timestamp recorded in the journal.
func Now(ctx *Context) time.Time {
	payload, _ := json.Marshal(ctx.now.UTC())
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeNowRecorded}, payload)
	var t time.Time
	_ = json.Unmarshal(ev.Payload, &t)
	return t
}

// SideEffect runs fn only when recording a new command; on replay returns the stored value.
func SideEffect[T any](ctx *Context, fn func() T) (T, error) {
	var zero T
	recorded := ctx.recordedCommands()
	if ctx.cmdIndex < len(recorded) {
		ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeSideEffect}, nil)
		var out T
		if err := ctx.codec.Unmarshal(ev.Payload, &out); err != nil {
			return zero, err
		}
		return out, nil
	}
	val := fn()
	payload, err := ctx.codec.Marshal(val)
	if err != nil {
		return zero, err
	}
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeSideEffect}, payload)
	_ = ev
	return val, nil
}

// NewUUID returns a durable random UUID string.
func NewUUID(ctx *Context) (string, error) {
	return SideEffect(ctx, func() string {
		var b [16]byte
		_, _ = rand.Read(b[:])
		return hex.EncodeToString(b[:])
	})
}
