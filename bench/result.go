package bench

import (
	"encoding/json"
	"fmt"
)

// Result is one benchmark run summary.
type Result struct {
	Backend     string  `json:"backend"`
	Workers     int     `json:"workers"`
	Instances   int     `json:"instances"`
	Steps       int     `json:"steps"`
	Completed   int     `json:"completed"`
	Failed      int     `json:"failed"`
	WallSeconds float64 `json:"wall_seconds"`
	Throughput  float64 `json:"throughput"`
}

func (r Result) FormatHuman() string {
	return fmt.Sprintf(
		"backend=%s workers=%d instances=%d steps=%d\ncompleted=%d failed=%d wall=%.3fs throughput=%.2f/s\n",
		r.Backend, r.Workers, r.Instances, r.Steps,
		r.Completed, r.Failed, r.WallSeconds, r.Throughput,
	)
}

func (r Result) FormatJSON() ([]byte, error) {
	return json.Marshal(r)
}
