package dynamodb

import (
	"sort"
	"testing"
)

func TestLessInstance_Order(t *testing.T) {
	type c struct {
		created int64
		id      string
	}
	in := []c{{3, "b"}, {1, "z"}, {1, "a"}, {2, "m"}}
	sort.Slice(in, func(i, j int) bool {
		return lessInstance(in[i].created, in[i].id, in[j].created, in[j].id)
	})
	want := []c{{1, "a"}, {1, "z"}, {2, "m"}, {3, "b"}}
	for i := range want {
		if in[i] != want[i] {
			t.Fatalf("got %+v want %+v", in, want)
		}
	}
}

func TestLessInstance_OffsetLimitSemantics(t *testing.T) {
	// Offset/Limit apply after stable sort; verify with synthetic data.
	type c struct {
		created int64
		id      string
	}
	all := []c{{2, "b"}, {1, "a"}, {3, "c"}, {1, "0"}}
	sort.Slice(all, func(i, j int) bool {
		return lessInstance(all[i].created, all[i].id, all[j].created, all[j].id)
	})
	// Sorted: (1,0),(1,a),(2,b),(3,c). Offset 1, Limit 2 -> (1,a),(2,b).
	got := all[1:]
	if len(got) > 2 {
		got = got[:2]
	}
	if len(got) != 2 || got[0] != (c{1, "a"}) || got[1] != (c{2, "b"}) {
		t.Fatalf("got %+v", got)
	}
}
