package bench_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hirokazumiyaji/tasuki/bench"
)

func TestResult_FormatHuman(t *testing.T) {
	r := bench.Result{
		Backend: "memory", Workers: 4, Instances: 200, Steps: 3,
		Completed: 200, Failed: 0, WallSeconds: 0.015, Throughput: 13218.92,
	}
	s := r.FormatHuman()
	for _, want := range []string{"backend=memory", "completed=200", "throughput=13218.92/s"} {
		if !strings.Contains(s, want) {
			t.Fatalf("missing %q in %q", want, s)
		}
	}
}

func TestResult_FormatJSON(t *testing.T) {
	r := bench.Result{Backend: "memory", Workers: 2, Instances: 10, Steps: 1, Completed: 10}
	b, err := r.FormatJSON()
	if err != nil {
		t.Fatal(err)
	}
	var got bench.Result
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Backend != "memory" || got.Completed != 10 || got.Workers != 2 {
		t.Fatalf("%+v", got)
	}
}
