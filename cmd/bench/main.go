package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/backend/postgres"
	"github.com/hirokazumiyaji/tasuki/bench"
)

func main() {
	backendFlag := flag.String("backend", "memory", "memory|postgres")
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
	b, closer, name, err := openBackend(ctx, *backendFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer closer()

	res, err := bench.Run(ctx, b, name, bench.Config{
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

func openBackend(ctx context.Context, name string) (backend.Backend, func(), string, error) {
	switch name {
	case "memory":
		return memory.New(), func() {}, "memory", nil
	case "postgres":
		dsn := os.Getenv("TASUKI_POSTGRES_DSN")
		if dsn == "" {
			return nil, nil, "", fmt.Errorf("TASUKI_POSTGRES_DSN required")
		}
		pb, err := postgres.New(ctx, dsn)
		if err != nil {
			return nil, nil, "", err
		}
		if err := pb.Migrate(ctx); err != nil {
			pb.Close()
			return nil, nil, "", err
		}
		if err := pb.Reset(ctx); err != nil {
			pb.Close()
			return nil, nil, "", err
		}
		return pb, pb.Close, "postgres", nil
	default:
		return nil, nil, "", fmt.Errorf("unknown -backend=%q (want memory|postgres)", name)
	}
}
