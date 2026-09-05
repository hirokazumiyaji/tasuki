# Bench baseline (memory)

再現手順（SLO ゲートなし。比較用メモ）:

```bash
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms
go run ./cmd/bench -backend=memory -instances=200 -workers=4 -scenario=mixed -run-id=base1
go run ./cmd/bench -backend=memory -instances=50 -workers=2 -scenario=long-history
# 永続ストアでの反復比較（既存データを保持し run-id で分離）
export TASUKI_POSTGRES_DSN='postgres://tasuki:tasuki@localhost:5432/tasuki?sslmode=disable'
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4 -run-id=pg1
go run -tags tasuki_all ./cmd/bench -backend=postgres -instances=200 -workers=4 -run-id=pg2
```

計測区間は投入開始→全完了の E2E（投入中に完了した件数も正しく分母子に含む）。JSON には遅延分布（p50/p95/p99 ms）と実効設定（poll/lease/claim-limit/並列度/scenario/run-id）を記録する。

測定のばらつきと制約:

- ローカル memory では wall が 10ms 台になり、OS スケジューリングで ±30% 程度ばらつく。3 回以上反復し中央値で比較する。
- 永続ストアでは DB の autovacuum・接続プール・ネットワークが支配的。初回（cold）と 2 回目以降（warm）で差が出るため、同一 `run-id` 体系で warm 2 回を記録する。
- `mixed` は slow activity（5ms）を含むため chain より throughput が低く p99 が長いのが正常。`long-history`（既定 20 steps）は replay コストの比較用で、短 chain と直接比較しない。
- Scan 系操作（DynamoDB `ListInstances` 全走査、orphan 回復の 5s 間隔スキャン）は bench 負荷に含まれない。実運用の一覧表示コストは別途 `docs/09-limits.md` を参照する。

## Captured result (memory)

- Date: 2026-07-29
- Host: local (darwin)
- Command: `go run ./cmd/bench -backend=memory -instances=200 -workers=4 -steps=3 -poll=20ms`
- Result:

```
backend=memory workers=4 instances=200 steps=3
completed=200 failed=0 wall=0.015s throughput=13218.92/s
```
