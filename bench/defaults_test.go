package bench

import (
	"testing"
	"time"
)

func TestConfig_withDefaults(t *testing.T) {
	cfg := Config{}.withDefaults()
	if cfg.Workers != 4 || cfg.Instances != 200 || cfg.Steps != 3 {
		t.Fatalf("%+v", cfg)
	}
	if cfg.Poll != 20*time.Millisecond || cfg.Lease != 30*time.Second {
		t.Fatalf("%+v", cfg)
	}
	cfg2 := Config{Workers: 2, Instances: 5, Steps: 1, Poll: time.Millisecond, Lease: time.Second}.withDefaults()
	if cfg2.Workers != 2 || cfg2.Instances != 5 || cfg2.Steps != 1 {
		t.Fatalf("%+v", cfg2)
	}
}
