# Benchmark Baseline (Memory)

[English] | [日本語](ja/06-bench-baseline.md)

Reproduction steps (for relative comparison; no strict SLO gate):

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -scenario=mixed -run-id=base1
go run ./cmd/bench -backend=memory -instances=50 -workers=2 -scenario=long-history

# Comparative runs on persistent stores (preserves existing data, isolated by run-id)
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4 -run-id=pg1
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4 -run-id=pg2
```

Measurement window captures end-to-end time from initiation of workflow submissions to all instances completing (instances completing during the submission phase are accurately accounted for). The JSON output records latency percentiles (p50 / p95 / p99 in ms) alongside effective configuration options (poll interval, lease duration, claim limit, concurrency levels, scenario, run ID).

Measurement Variance and Constraints:

- On the local memory backend, total wall-clock time is on the order of 10–20ms, subject to ±30% variance from OS thread scheduling. Take the median of at least 3 repetitions.
- On persistent databases, database autovacuum, connection pool warming, and network round-trips dominate. Because initial cold runs differ from subsequent warm runs, record two consecutive warm runs within the same run-id sequence.
- The `mixed` scenario includes deliberate 5ms slow activities; lower throughput and higher p99 compared to `chain` are expected. The `long-history` scenario (default 20 steps) evaluates replay cost and should not be compared directly against short chains.
- Scan operations (e.g. DynamoDB `ListInstances` table scans, 5s periodic orphan recovery scans) are excluded from benchmark load. For production visibility considerations, refer to [09-limits.md](09-limits.md).

## Captured Result (Memory)

- **Date**: 2026-07-29
- **Host**: local (darwin)
- **Command**: `go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms`
- **Result**:

```
backend=memory workers=4 instances=200 steps=3
completed=200 failed=0 wall=0.015s throughput=13218.92/s
```
