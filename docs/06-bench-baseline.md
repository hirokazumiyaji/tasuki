# Bench baseline (memory)

再現手順（SLO ゲートなし。比較用メモ）:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms
```

Postgres（手動）:

```bash
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run ./cmd/bench -backend=postgres -instances=200 -workers=4
```

## Captured result (memory)

- Date: 2026-07-29
- Host: local (darwin)
- Command: `go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms`
- Result:

```
backend=memory workers=4 instances=200 steps=3
completed=200 failed=0 wall=0.015s throughput=13218.92/s
```
