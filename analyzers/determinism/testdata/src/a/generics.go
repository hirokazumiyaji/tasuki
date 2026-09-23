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

// make accepts channels of any direction (make(chan T), make(<-chan T) and
// make(chan<- T) all compile), so a channel union with send-capable members
// still creates a native channel and must be flagged — even though the
// range-oriented core type ignores send-only members.

func BadUnionSendMakeChan[C chan int | chan<- int](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// A receive-compatible channel union must keep being flagged by the make
// check (it was already covered via the range-oriented core type).

func BadUnionRecvMakeChan[C chan int | <-chan int](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// Exact (non-union) constraints carry the concrete type directly as the
// embedded constraint term rather than a union; range and make over them
// must still resolve.

func BadExactChanRange[C chan int](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func BadExactMapRange[M map[string]int](ctx *workflow.Context, m M) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func BadExactMakeChan[C chan int](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// Composed (named) constraints embed another constraint interface instead of
// spelling the union inline; the embedded interface must be flattened
// recursively before classifying the range/make operand.

type MapBase interface{ ~map[string]int }

type DerivedMap interface{ MapBase }

type DerivedMap2 interface{ DerivedMap }

func BadComposedMapRange[M DerivedMap](ctx *workflow.Context, m M) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func BadComposedMapRange2[M DerivedMap2](ctx *workflow.Context, m M) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

type ChanBase interface{ ~chan int }

type DerivedChan interface{ ChanBase }

func BadComposedChanRange[C DerivedChan](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func BadComposedMakeChan[C DerivedChan](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// Intersected embedded constraints denote the INTERSECTION across embeds, not
// the flattened union: `chan int | chan<- int` intersected with
// `chan int | <-chan int` is {chan int}, so ranging must still flag.
func BadIntersectChanRange[C interface {
	chan int | chan<- int
	chan int | <-chan int
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

// Likewise `chan int | []int` intersected with `chan int` is {chan int}, so
// make must still flag even though the flattened union mixes channel and
// non-channel terms.
func BadIntersectMakeChan[C interface {
	chan int | []int
	chan int
}](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// Comparable and method-only embeds contribute no type terms, so they are
// neutral in the intersection: `~chan int` ∩ comparable is still {chan int}
// and must keep flagging range and make.
func BadComparableChanRange[C interface {
	~chan int
	comparable
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func BadComparableMakeChan[C interface {
	~chan int
	comparable
}](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

type MethodOnly interface{ M() }

func BadMethodOnlyMapRange[M interface {
	~map[string]int
	MethodOnly
}](ctx *workflow.Context, m M) error {
	for k := range m { // want `ranging over a map is not allowed`
		_ = k
	}
	return nil
}

func BadMethodOnlyMakeChan[C interface {
	~chan int
	MethodOnly
}](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// comparable filters non-comparable terms (maps, slices, funcs) out of the
// candidate set: `~map[string]int | ~chan int` ∩ comparable is {chan int},
// so range and make over it must still flag as channel operations (not map
// operations, and not silence).

func BadComparableMixedRange[C interface {
	~map[string]int | ~chan int
	comparable
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func BadComparableMixedMake[C interface {
	~map[string]int | ~chan int
	comparable
}](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// The filter is transitive through named constraints: Base carries
// comparable into the intersection even though the outer interface never
// names it.

type ComparableBase interface{ comparable }

func BadComparableNamedMixedRange[C interface {
	~map[string]int | ~chan int
	ComparableBase
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

// comparable must filter COMPOSITE non-comparable terms too: `~[1][]int`
// (array of slices) is not comparable, so `~[1][]int | ~chan int` ∩
// comparable is {chan int} and range/make must still flag as channel
// operations instead of falling silent on the mixed set.
func BadComparableCompositeMixedRange[C interface {
	~[1][]int | ~chan int
	comparable
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

func BadComparableCompositeMixedMake[C interface {
	~[1][]int | ~chan int
	comparable
}](ctx *workflow.Context, _ struct{}) error {
	_ = make(C) // want `make\(chan`
	return nil
}

// A struct holding a slice is likewise not comparable and must be filtered,
// leaving the channel term.
func BadComparableStructMixedRange[C interface {
	~struct{ Vs []int } | ~chan int
	comparable
}](ctx *workflow.Context, ch C) error {
	for v := range ch { // want `ranging over a channel is not allowed`
		_ = v
	}
	return nil
}

// A composite of comparables stays: covered at the unit level instead (see
// analyzer_internal_test.go). A range-based Ok control cannot spell it: any
// mixed set whose composite term survives the filter has no single core type
// and does not compile (`[2]int and chan int have different underlying
// types`), so analysistest cannot load it.
