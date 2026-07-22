package tasuki

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"strings"

	"github.com/hirokazumiyaji/tasuki/codec"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

type workflowEntry struct {
	name string
	fn   func(ctx *workflow.Context, input []byte) ([]byte, error)
}

type activityEntry struct {
	name string
	fn   func(ctx context.Context, input []byte) ([]byte, error)
}

type registry struct {
	workflows  map[string]workflowEntry
	activities map[string]activityEntry
	codec      codec.Codec
}

func newRegistry(c codec.Codec) *registry {
	return &registry{
		workflows:  map[string]workflowEntry{},
		activities: map[string]activityEntry{},
		codec:      c,
	}
}

func funcName(fn any) string {
	v := runtime.FuncForPC(reflect.ValueOf(fn).Pointer())
	if v == nil {
		return ""
	}
	name := v.Name()
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	name = strings.TrimSuffix(name, "-fm")
	return name
}

func RegisterWorkflow[I, O any](w *Worker, fn func(*workflow.Context, I) (O, error), opts ...RegisterOption) {
	o := registerOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	name := o.name
	if name == "" {
		name = funcName(fn)
	}
	w.reg.workflows[name] = workflowEntry{
		name: name,
		fn: func(ctx *workflow.Context, input []byte) ([]byte, error) {
			var in I
			if len(input) > 0 {
				if err := w.reg.codec.Unmarshal(input, &in); err != nil {
					return nil, err
				}
			}
			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}
			return w.reg.codec.Marshal(out)
		},
	}
}

func RegisterActivity[I, O any](w *Worker, fn func(context.Context, I) (O, error), opts ...RegisterOption) {
	o := registerOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	name := o.name
	if name == "" {
		name = funcName(fn)
	}
	w.reg.activities[name] = activityEntry{
		name: name,
		fn: func(ctx context.Context, input []byte) ([]byte, error) {
			var in I
			if len(input) > 0 {
				if err := w.reg.codec.Unmarshal(input, &in); err != nil {
					return nil, err
				}
			}
			out, err := fn(ctx, in)
			if err != nil {
				return nil, err
			}
			return w.reg.codec.Marshal(out)
		},
	}
}

func (r *registry) workflow(name string) (workflowEntry, error) {
	e, ok := r.workflows[name]
	if !ok {
		return workflowEntry{}, fmt.Errorf("workflow %q not registered", name)
	}
	return e, nil
}

func (r *registry) activity(name string) (activityEntry, error) {
	e, ok := r.activities[name]
	if !ok {
		return activityEntry{}, fmt.Errorf("activity %q not registered", name)
	}
	return e, nil
}
