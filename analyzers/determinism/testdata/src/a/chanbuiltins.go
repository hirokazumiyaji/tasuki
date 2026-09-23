package a

import (
	"github.com/hirokazumiyaji/tasuki/workflow"
)

// Predeclared close/len/cap builtins on native channels fall through the
// package-qualified call checks (empty package path); they must be resolved
// via TypesInfo.Uses to *types.Builtin and classified through the same
// core channel type as make/range.

func BadCloseChan(ctx *workflow.Context, ch chan int) error {
	close(ch) // want `close on a channel is not allowed`
	return nil
}

func BadLenChan(ctx *workflow.Context, ch chan int) error {
	_ = len(ch) // want `len on a channel is not allowed`
	return nil
}

func BadCapChan(ctx *workflow.Context, ch chan int) error {
	_ = cap(ch) // want `cap on a channel is not allowed`
	return nil
}

func BadCloseNamedChan(ctx *workflow.Context, ch namedChan) error {
	close(ch) // want `close on a channel is not allowed`
	return nil
}

func BadLenNamedChan(ctx *workflow.Context, ch namedChan) error {
	_ = len(ch) // want `len on a channel is not allowed`
	return nil
}

func BadCapNamedChan(ctx *workflow.Context, ch namedChan) error {
	_ = cap(ch) // want `cap on a channel is not allowed`
	return nil
}

func BadParenCloseChan(ctx *workflow.Context, ch chan int) error {
	(close)(ch) // want `close on a channel is not allowed`
	return nil
}

// len/cap on non-channels must stay clean.

func OkLenSlice(ctx *workflow.Context, _ struct{}) error {
	_ = len([]int{1, 2})
	return nil
}

func OkCapSlice(ctx *workflow.Context, _ struct{}) error {
	_ = cap([]int{1, 2})
	return nil
}

func OkLenMap(ctx *workflow.Context, _ struct{}) error {
	_ = len(map[string]int{"a": 1})
	return nil
}

func OkLenString(ctx *workflow.Context, _ struct{}) error {
	_ = len("abc")
	return nil
}

// A local shadowing a predeclared builtin is an ordinary call, not a
// channel operation.

func OkShadowedLen(ctx *workflow.Context, ch chan int) error {
	len := func(_ chan int) int { return 0 }
	_ = len(ch)
	return nil
}

// Channel-constraint type parameters resolve through the constraint set,
// like make/range.

func BadGenericCloseChan[C ~chan int](ctx *workflow.Context, ch C) error {
	close(ch) // want `close on a channel is not allowed`
	return nil
}

func BadGenericLenChan[C ~chan int](ctx *workflow.Context, ch C) error {
	_ = len(ch) // want `len on a channel is not allowed`
	return nil
}

func BadGenericCapChan[C ~chan int](ctx *workflow.Context, ch C) error {
	_ = cap(ch) // want `cap on a channel is not allowed`
	return nil
}

func BadUnionLenChan[C chan int | <-chan int](ctx *workflow.Context, ch C) error {
	_ = len(ch) // want `len on a channel is not allowed`
	return nil
}

func OkGenericLenSlice[S ~[]int](ctx *workflow.Context, s S) error {
	_ = len(s)
	return nil
}

// close/len/cap accept channel unions with mixed element types (unlike make,
// which must allocate a common element type): every instantiation is a
// send-capable channel, so these must still be flagged.

func BadMixedCloseChan[C chan int | chan string](ctx *workflow.Context, ch C) error {
	close(ch) // want `close on a channel is not allowed`
	return nil
}

func BadMixedLenChan[C chan int | chan string](ctx *workflow.Context, ch C) error {
	_ = len(ch) // want `len on a channel is not allowed`
	return nil
}

func BadMixedCapChan[C chan int | chan string](ctx *workflow.Context, ch C) error {
	_ = cap(ch) // want `cap on a channel is not allowed`
	return nil
}

// A mixed channel/non-channel union is not provably a channel operand: len
// may legally apply to the non-channel member, so it stays clean.

func OkMixedChanSliceLen[C chan int | []int](ctx *workflow.Context, x C) error {
	_ = len(x)
	return nil
}
