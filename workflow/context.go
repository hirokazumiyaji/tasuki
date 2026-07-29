package workflow

import (
	"encoding/json"
	"runtime"
	"time"

	"github.com/hirokazumiyaji/tasuki/journal"
)

// Codec serializes user payloads (activity inputs and results, signals,
// side effects, child inputs). It mirrors codec.Codec so the worker can
// inject its configured codec without an import cycle.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

type Context struct {
	events      []journal.Event
	cmdIndex    int
	commands    []journal.Event
	nextSeq     int64
	now         time.Time
	completions map[int64]journal.Event
	canceled    bool
	suspended   bool
	info            WorkflowInfo
	consumedSignals map[int64]bool
	codec           Codec
	queryMode        bool
	queryInvoking    bool
	queryHandlers    map[string]queryHandler
	updateHandlers   map[string]updateHandler
	searchAttributes map[string]string
	memo             map[string]string
	localRunner      LocalActivityRunner
}

func NewContext(events []journal.Event, now time.Time) *Context {
	ctx := &Context{
		events:      events,
		now:         now,
		completions:     map[int64]journal.Event{},
		nextSeq:         1,
		consumedSignals: map[int64]bool{},
		codec:           jsonCodec{},
	}
	for _, e := range events {
		if e.Type == journal.TypeWorkflowStarted {
			ctx.info.Name = e.Name
			break
		}
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
		if e.Type == journal.TypeSearchAttributesUpdated {
			var m map[string]string
			_ = json.Unmarshal(e.Payload, &m)
			ctx.searchAttributes = cloneSearchAttrs(m)
		}
		if e.Type == journal.TypeMemoUpdated {
			var m map[string]string
			_ = json.Unmarshal(e.Payload, &m)
			ctx.memo = cloneSearchAttrs(m)
		}
	}
	return ctx
}

func (c *Context) NewCommands() []journal.Event { return c.commands }

func (c *Context) SetInfo(info WorkflowInfo) { c.info = info }

// SetCodec injects the worker's payload codec. Unset, the context uses plain JSON.
func (c *Context) SetCodec(m Codec) { c.codec = m }

// SetLocalActivityRunner injects the worker's local activity invoker.
func (c *Context) SetLocalActivityRunner(r LocalActivityRunner) { c.localRunner = r }

func WasSuspended(c *Context) bool { return c.suspended }

// ClearSuspended resets the suspend flag so a follow-up phase (e.g. Update
// dispatch) can run on the same Context after the main workflow Goexit'd.
func ClearSuspended(c *Context) { c.suspended = false }

func (c *Context) recordOrReplay(cmd journal.Command, payload []byte) journal.Event {
	recordedCmds := c.recordedCommands()
	for c.cmdIndex < len(recordedCmds) {
		rec := recordedCmds[c.cmdIndex]
		// Old code skips version markers it does not understand.
		if cmd.Type != journal.TypeVersionMarker && rec.Type == journal.TypeVersionMarker {
			c.cmdIndex++
			continue
		}
		c.cmdIndex++
		if err := journal.MatchCommand(rec, cmd); err != nil {
			raiseDeterminism(err)
		}
		return rec
	}
	if c.queryInvoking {
		raiseDeterminism(errQuerySideEffect)
	}
	if c.queryMode {
		// Read-only query: do not extend history; stop at the wait point.
		c.suspend()
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
	// Replay an already-recorded timer before applying cancel, so command matching stays aligned.
	if rec, ok := ctx.peekCommand(); ok && rec.Type == journal.TypeTimerCreated {
		ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, rec.Payload)
		if _, done := ctx.awaitCompletion(ev.Seq); done {
			return nil
		}
		if ctx.canceled {
			return ErrCanceled
		}
		ctx.suspend()
		return nil
	}
	if ctx.canceled {
		return ErrCanceled
	}
	payload, err := json.Marshal(timerPayload{FireAt: ctx.now.Add(d)})
	if err != nil {
		return err
	}
	ev := ctx.recordOrReplay(journal.Command{Type: journal.TypeTimerCreated}, payload)
	if _, ok := ctx.awaitCompletion(ev.Seq); !ok {
		ctx.suspend()
		return nil // unreachable after Goexit
	}
	return nil
}

type timerPayload struct {
	FireAt time.Time `json:"fire_at"`
}

func (c *Context) takeSignal(name string) (journal.Event, bool) {
	for _, e := range c.events {
		if e.Type != journal.TypeSignalReceived || e.Name != name {
			continue
		}
		if c.consumedSignals[e.Seq] {
			continue
		}
		c.consumedSignals[e.Seq] = true
		return e, true
	}
	return journal.Event{}, false
}

func (c *Context) peekSignal(name string) (journal.Event, bool) {
	for _, e := range c.events {
		if e.Type != journal.TypeSignalReceived || e.Name != name {
			continue
		}
		if c.consumedSignals[e.Seq] {
			continue
		}
		return e, true
	}
	return journal.Event{}, false
}

func (c *Context) peekCommand() (journal.Event, bool) {
	cmds := c.recordedCommands()
	// skip already-handled index; also surface markers
	if c.cmdIndex >= len(cmds) {
		return journal.Event{}, false
	}
	return cmds[c.cmdIndex], true
}

func (c *Context) skipCommand() {
	c.cmdIndex++
}
