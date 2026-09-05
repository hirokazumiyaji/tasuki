package bench

import (
	"encoding/json"
	"fmt"
)

// Result is one benchmark run summary.
type Result struct {
	Backend     string         `json:"backend"`
	Workers     int            `json:"workers"`
	Instances   int            `json:"instances"`
	Steps       int            `json:"steps"`
	Scenario    string         `json:"scenario,omitempty"`
	RunID       string         `json:"run_id,omitempty"`
	Completed   int            `json:"completed"`
	Failed      int            `json:"failed"`
	WallSeconds float64        `json:"wall_seconds"`
	Throughput  float64        `json:"throughput"`
	LatencyP50  float64        `json:"latency_p50_ms,omitempty"`
	LatencyP95  float64        `json:"latency_p95_ms,omitempty"`
	LatencyP99  float64        `json:"latency_p99_ms,omitempty"`
	Settings    map[string]any `json:"settings,omitempty"`
}

func (r Result) FormatHuman() string {
	base := fmt.Sprintf(
		"backend=%s workers=%d instances=%d steps=%d scenario=%s run=%s\ncompleted=%d failed=%d wall=%.3fs throughput=%.2f/s\n",
		r.Backend, r.Workers, r.Instances, r.Steps, r.Scenario, r.RunID,
		r.Completed, r.Failed, r.WallSeconds, r.Throughput,
	)
	if r.LatencyP50 > 0 || r.LatencyP95 > 0 || r.LatencyP99 > 0 {
		base += fmt.Sprintf("latency_ms p50=%.2f p95=%.2f p99=%.2f\n", r.LatencyP50, r.LatencyP95, r.LatencyP99)
	}
	return base
}

func (r Result) FormatJSON() ([]byte, error) {
	return json.Marshal(r)
}
