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
	jsonOut := flag.Bool("json", false, "emit JSON result")
	flag.Parse()

	ctx := context.Background()
	b, closer, err := backendopen.Open(ctx, *backendFlag, backendopen.Options{Reset: true})
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
