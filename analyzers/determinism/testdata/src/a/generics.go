package a

import (
	"time"

	randv2 "math/rand/v2"

	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Explicit generic instantiations wrap the callee in IndexExpr/IndexListExpr:
// prohibited calls must still be detected, and workflow escape-hatch
// exemptions must still apply.

func BadRandV2Generic(ctx *workflow.Context, _ struct{}) error {
	_ = randv2.N[int](10) // want `math/rand/v2`
	return nil
}

func OkGenericSideEffect(ctx *workflow.Context, _ struct{}) error {
	_, _ = workflow.SideEffect[string](ctx, func() string {
		return time.Now().String()
	})
	return nil
}

func OkGenericQueryHandler(ctx *workflow.Context, _ struct{}) error {
	workflow.SetQueryHandler[int, int](ctx, "q", func(v int) (int, error) {
		_ = time.Now()
		return v, nil
	})
	return nil
}

// Range operands of type-parameter type carry the constraint interface as
// their underlying type; the core map/channel type must be resolved.

func BadGenericMapRange[M ~map[string]int](ctx *workflow.Context, m M) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func BadGenericChanRange[C ~chan int](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func OkGenericSliceRange[S ~[]int](ctx *workflow.Context, s S) error {
	for _, v := range s {
		_ = v
	}
	return nil
}

// make(C) with a channel type parameter hides channel creation behind an
// Ident (not a ChanType node); the argument must resolve through the
// type-param core type before classifying.

func BadGenericMakeChan[C ~chan int](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

func BadGenericMakeChanParen[C ~chan int](ctx *workflow.Context, _ struct{}) error {
	_ = make((C)) // want `make\(chan`
	return nil
}

// A channel union whose members differ only in direction has no single
// identical core type, yet every instantiation ranges over a receivable
// channel: it must still be flagged.

func BadUnionChanRange[C chan int | <-chan int](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}
