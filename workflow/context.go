package workflow

import (
	"runtime"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

type Context struct {
	events      []journal.Event
	cmdIndex    int
	commands    []journal.Event
	nextSeq     int64
	now         time.Time
	completions map[int64]journal.Event
	canceled    bool
	suspended   bool
}

func NewContext(events []journal.Event, now time.Time) *Context {
	ctx := &Context{
		events:      events,
		now:         now,
		completions: map[int64]journal.Event{},
		nextSeq:     1,
	}
	for _, e := range events {
		if e.Seq >= ctx.nextSeq {
			ctx.nextSeq = e.Seq + 1
		}
		if e.Type.IsCompletion() && e.RefSeq != 0 {
			ctx.completions[e.RefSeq] = e
		}
		if e.Type == journal.TypeCancelRequested {
			ctx.canceled = true
		}
	}
	return ctx
}

func (c *Context) NewCommands() []journal.Event { return c.commands }

func WasSuspended(c *Context) bool { return c.suspended }

func (c *Context) recordOrReplay(cmd journal.Command, payload []byte) journal.Event {
	recordedCmds := c.recordedCommands()
	if c.cmdIndex < len(recordedCmds) {
		rec := recordedCmds[c.cmdIndex]
		c.cmdIndex++
		if err := journal.MatchCommand(rec, cmd); err != nil {
			raiseDeterminism(err)
		}
		return rec
	}
	ev := journal.Event{
		Seq:     c.nextSeq,
		Type:    cmd.Type,
		Name:    cmd.Name,
		Payload: payload,
	}
	c.nextSeq++
	c.commands = append(c.commands, ev)
	c.cmdIndex++
	return ev
}

func (c *Context) recordedCommands() []journal.Event {
	out := make([]journal.Event, 0)
	for _, e := range c.events {
		if e.Type.IsCommand() {
			out = append(out, e)
		}
	}
	return out
}

func (c *Context) awaitCompletion(seq int64) (journal.Event, bool) {
	ev, ok := c.completions[seq]
	return ev, ok
}

func (c *Context) suspend() {
	c.suspended = true
	runtime.Goexit()
}

type determinismPanic struct{ err error }

func (d determinismPanic) Error() string { return d.err.Error() }
func (d determinismPanic) Unwrap() error { return d.err }

func raiseDeterminism(err error) {
	panic(determinismPanic{err: err})
}

// AsDeterminismPanic extracts a determinism violation from a recovered panic value.
func AsDeterminismPanic(r any) (error, bool) {
	if d, ok := r.(determinismPanic); ok {
		return d.err, true
	}
	return nil, false
}

// Sleep schedules a durable timer. If the timer has not fired in the journal, the
// workflow goroutine suspends via runtime.Goexit.
func Sleep(ctx *Context, d time.Duration) error {
	if ctx.canceled {
		return ErrCanceled
	}
	_ = d // fire_at payload is added in Task 5
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, nil)
	if _, ok := ctx.awaitCompletion(ev.Seq); !ok {
		ctx.suspend()
		return nil // unreachable after Goexit
	}
	return nil
}
