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
	events   []journal.Event
	cmdIndex int
	commands []journal.Event
	// recorded is the precomputed command subsequence of events, built once
	// in NewContext so recordOrReplay/peekCommand index in O(1) instead of
	// rescanning all events per command (O(N^2) replay).
	recorded []journal.Event
	// signals holds per-name signal queues in journal order, built once in
	// NewContext; signalPos tracks the consumed cursor per name so
	// takeSignal/peekSignal are O(1) amortized instead of scanning events.
	signals          map[string][]journal.Event
	signalPos        map[string]int
	nextSeq          int64
	now              time.Time
	completions      map[int64]journal.Event
	canceled         bool
	suspended        bool
	info             WorkflowInfo
	consumedSignals  map[int64]bool
	codec            Codec
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
		events:          events,
		now:             now,
		completions:     map[int64]journal.Event{},
		nextSeq:         1,
		consumedSignals: map[int64]bool{},
		codec:           jsonCodec{},
		signals:         map[string][]journal.Event{},
		signalPos:       map[string]int{},
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
		if e.Type.IsCommand() {
			ctx.recorded = append(ctx.recorded, e)
		}
		if e.Type == journal.TypeSignalReceived {
			ctx.signals[e.Name] = append(ctx.signals[e.Name], e)
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
	for c.cmdIndex < len(c.recorded) {
		rec := c.recorded[c.cmdIndex]
		// Skip version markers this call site does not understand.
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
	return c.recorded
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
	return sleepAt(ctx, ctx.now.Add(d))
}

// SleepUntil schedules a durable timer that fires at the given absolute time.
// The deadline is recorded verbatim in the journal and never passes through
// time.Duration, so deadlines beyond the ~290-year duration range are preserved
// instead of saturating via time.Time.Sub. A Now checkpoint is still recorded
// first to anchor determinism and keep the journal shape
// (now_recorded + timer_created) compatible with existing histories.
// A deadline at or before Now is clamped to Now before persisting: zero or
// pre-year-1000 times recorded verbatim break MySQL DATETIME(6) inserts
// (minimum year 1000) under strict mode, turning a wake into a retry. The
// clamped timer stays already-due and fires on the next tick.
func SleepUntil(ctx *Context, t time.Time) error {
	now := Now(ctx)
	if !t.After(now) {
		t = now
	}
	return sleepAt(ctx, t)
}

func sleepAt(ctx *Context, fireAt time.Time) error {
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
	payload, err := json.Marshal(timerPayload{FireAt: fireAt})
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
	q := c.signals[name]
	pos := c.signalPos[name]
	if pos >= len(q) {
		return journal.Event{}, false
	}
	ev := q[pos]
	c.signalPos[name] = pos + 1
	c.consumedSignals[ev.Seq] = true
	return ev, true
}

func (c *Context) peekSignal(name string) (journal.Event, bool) {
	q := c.signals[name]
	pos := c.signalPos[name]
	if pos >= len(q) {
		return journal.Event{}, false
	}
	return q[pos], true
}

func (c *Context) peekCommand() (journal.Event, bool) {
	// skip already-handled index; also surface markers
	if c.cmdIndex >= len(c.recorded) {
		return journal.Event{}, false
	}
	return c.recorded[c.cmdIndex], true
}

func (c *Context) skipCommand() {
	c.cmdIndex++
}
