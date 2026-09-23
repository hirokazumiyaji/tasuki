package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"testing"
)

// TestTypeComparable covers the issue #301 round-11 P2 at the helper level:
// the comparable filter must drop COMPOSITE non-comparable terms (arrays and
// structs built transitively from slices, maps, or funcs) while keeping
// composites of comparables. A range-based Ok control cannot spell the keep
// direction in testdata: any mixed set whose composite term survives has no
// single core type and does not compile, so analysistest cannot load it.
//
// The probe types are defined (not aliases) so the check also exercises the
// Named branch of the recursion.
func TestTypeComparable(t *testing.T) {
	const src = `package p
type KeepBasic int
type KeepChan chan int
type KeepRecvChan <-chan int
type KeepPtr *int
type KeepArray [2]int
type KeepStruct struct {
	N int
	A [2]string
	P *int
}
type KeepIface interface{ M() }
type KeepAny any

type DropSlice []int
type DropMap map[string]int
type DropFunc func()
type DropArray [1][]int
type DropArrMap [2]map[string]int
type DropStruct struct{ Vs []int }
type DropNested struct {
	A [2][]int
	S struct{ Vs []int }
}
type DropDeep [2][3][]string
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "p.go", src, 0)
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := new(types.Config).Check("p", fset, []*ast.File{f}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		drop bool
	}{
		{"KeepBasic", false},
		{"KeepChan", false},
		{"KeepRecvChan", false},
		{"KeepPtr", false},
		{"KeepArray", false},
		{"KeepStruct", false},
		{"KeepIface", false},
		{"KeepAny", false},
		{"DropSlice", true},
		{"DropMap", true},
		{"DropFunc", true},
		{"DropArray", true},
		{"DropArrMap", true},
		{"DropStruct", true},
		{"DropNested", true},
		{"DropDeep", true},
	} {
		obj := pkg.Scope().Lookup(tc.name)
		if obj == nil {
			t.Fatalf("type %s not found", tc.name)
		}
		tn, ok := obj.(*types.TypeName)
		if !ok {
			t.Fatalf("%s is %T, want *types.TypeName", tc.name, obj)
		}
		if got := violatesComparable(tn.Type()); got != tc.drop {
			t.Errorf("violatesComparable(%s) = %v, want %v", tc.name, got, tc.drop)
		}
	}
}
