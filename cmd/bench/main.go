package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki/bench"
	"github.com/hirokazumiyaji/tasuki/internal/backendopen"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres|sqlite|mysql|spanner|dynamodb|firestore")
	workers := flag.Int("workers", 4, "in-process worker count")
	instances := flag.Int("instances", 200, "workflows to start")
	steps := flag.Int("steps", 3, "serial activity steps per workflow")
	duration := flag.Duration("duration", 0, "max wait (0 = wait for all)")
	poll := flag.Duration("poll", 20*time.Millisecond, "worker poll interval")
	claimLimit := flag.Int("claim-limit", 0, "tasks per claim (0 = worker default 10)")
	activityConc := flag.Int("activity-concurrency", 0, "parallel activities (0 = worker default 1)")
	workflowConc := flag.Int("workflow-concurrency", 0, "parallel workflows (0 = worker default 1)")
	scenario := flag.String("scenario", "chain", "chain|long-history|mixed")
	runID := flag.String("run-id", "", "run prefix for instance IDs (default auto-generated)")
	reset := flag.Bool("reset", false, "DANGER: wipe the target store before running (must confirm target)")
	jsonOut := flag.Bool("json", false, "emit JSON result")
	flag.Parse()

	ctx := context.Background()
	if *reset {
		fmt.Fprintf(os.Stderr, "bench: --reset wipes backend=%q target %s\n", *backendFlag, backendopen.DescribeTarget(*backendFlag))
		if os.Getenv("TASUKI_ALLOW_RESET") != "1" {
			fmt.Fprintln(os.Stderr, "bench: refusing --reset without TASUKI_ALLOW_RESET=1 (set it to confirm the wipe target)")
			os.Exit(2)
		}
	}
	b, closer, err := backendopen.Open(ctx, *backendFlag, backendopen.Options{Reset: *reset})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	res, err := bench.Run(ctx, b, *backendFlag, bench.Config{
		Workers:             *workers,
		Instances:           *instances,
		Steps:               *steps,
		Duration:            *duration,
		Poll:                *poll,
		ClaimLimit:          *claimLimit,
		ActivityConcurrency: *activityConc,
		WorkflowConcurrency: *workflowConc,
		Scenario:            *scenario,
		RunID:               *runID,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		printResult(res, *jsonOut)
		os.Exit(1)
	}
	printResult(res, *jsonOut)
}

func printResult(res bench.Result, jsonOut bool) {
	if jsonOut {
		raw, jerr := res.FormatJSON()
		if jerr != nil {
			fmt.Fprintln(os.Stderr, jerr)
			os.Exit(1)
		}
		fmt.Println(string(raw))
		return
	}
	fmt.Print(res.FormatHuman())
}
