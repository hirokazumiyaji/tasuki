package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/bench"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres")
	workers := flag.Int("workers", 4, "in-process worker count")
	instances := flag.Int("instances", 200, "workflows to start")
	steps := flag.Int("steps", 3, "serial activity steps per workflow")
	duration := flag.Duration("duration", 0, "max wait (0 = wait for all)")
	poll := flag.Duration("poll", 20*time.Millisecond, "worker poll interval")
	jsonOut := flag.Bool("json", false, "emit JSON result")
	flag.Parse()

	ctx := context.Background()
	var (
		b      backend.Backend
		closer func()
		name   string
	)
	switch *backendFlag {
	case "memory":
		b = memory.New()
		closer = func() {}
		name = "memory"
	case "postgres":
		fmt.Fprintln(os.Stderr, "postgres backend not wired yet; use a later build or wait for Task 4")
		os.Exit(2)
	default:
		fmt.Fprintf(os.Stderr, "unknown -backend=%q (want memory|postgres)\n", *backendFlag)
		os.Exit(2)
	}
	defer closer()

	res, err := bench.Run(ctx, b, name, bench.Config{
		Workers:   *workers,
		Instances: *instances,
		Steps:     *steps,
		Duration:  *duration,
		Poll:      *poll,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if *jsonOut {
			if raw, jerr := res.FormatJSON(); jerr == nil {
				fmt.Println(string(raw))
			}
		} else {
			fmt.Print(res.FormatHuman())
		}
		os.Exit(1)
	}
	if *jsonOut {
		raw, jerr := res.FormatJSON()
		if jerr != nil {
			fmt.Fprintln(os.Stderr, jerr)
			os.Exit(1)
		}
		fmt.Println(string(raw))
	} else {
		fmt.Print(res.FormatHuman())
	}
}
