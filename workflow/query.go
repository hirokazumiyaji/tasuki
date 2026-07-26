package workflow

import "errors"

// ErrUnknownQuery is returned when no handler is registered for the query name.
var ErrUnknownQuery = errors.New("workflow: unknown query")

// errQuerySideEffect is raised when a query handler tries to record a new command.
var errQuerySideEffect = errors.New("workflow: query handler must not record commands")

type queryHandler func(arg []byte) (result []byte, err error)

// SetQueryHandler registers a read-only query handler on this workflow Context.
// It must be called at a deterministic point on every replay (typically near the start).
// Handlers are not recorded in the journal.
func SetQueryHandler[I, O any](ctx *Context, name string, fn func(I) (O, error)) {
	if ctx.queryHandlers == nil {
		ctx.queryHandlers = map[string]queryHandler{}
	}
	codec := ctx.codec
	ctx.queryHandlers[name] = func(arg []byte) ([]byte, error) {
		var in I
		if len(arg) > 0 {
			if err := codec.Unmarshal(arg, &in); err != nil {
				return nil, err
			}
		}
		out, err := fn(in)
		if err != nil {
			return nil, err
		}
		return codec.Marshal(out)
	}
}

// SetQueryMode enables read-only replay used by Query (internal to engine/worker).
func (c *Context) SetQueryMode(v bool) { c.queryMode = v }

// InvokeQuery runs a registered handler (internal to engine/worker).
func (c *Context) InvokeQuery(name string, arg []byte) ([]byte, error) {
	h, ok := c.queryHandlers[name]
	if !ok {
		return nil, ErrUnknownQuery
	}
	// Handler runs on the caller's goroutine; new commands must not Goexit.
	c.queryMode = false
	c.queryInvoking = true
	defer func() { c.queryInvoking = false }()

	var (
		payload []byte
		err     error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				if dErr, ok := AsDeterminismPanic(r); ok {
					err = dErr
					return
				}
				panic(r)
			}
		}()
		payload, err = h(arg)
	}()
	return payload, err
}
