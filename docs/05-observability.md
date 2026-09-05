# 観測性

tasuki ワーカーが出すログとメトリクスの一覧。

## ログ（slog）

`WorkerOptions.Logger`（既定 `slog.Default()`）に、タスク処理の要点を出す。

| イベント | レベル | 主な属性 |
|---|---|---|
| workflow task 開始 | Debug | `instance_id`, `task_id`, `workflow` |
| workflow 終端 | Info | `instance_id`, `status` |
| workflow stuck | Warn | `instance_id`, `error` |
| journal size warning | Warn | `instance_id`, `workflow`, `journal_events`, `threshold` |
| activity task 開始 | Debug | `instance_id`, `task_id`, `activity`, `attempt` |
| activity リトライ | Info | `instance_id`, `activity`, `attempt`, `visible_at` |
| activity 恒久失敗 | Warn | `instance_id`, `activity`, `error` |
| activity panic 回収 | Warn | `panic`（+ `activity` for local） |
| store 操作失敗 | Warn | `op`, `err`（+ `instance_id` 等） |
| store 競合/停止時キャンセル | Debug | `op`, `err` |
| orphan task 回復 | Info | `n` |
| truncated fanout | Info | `instance_id`, `kept_commands`, `total_commands`, `budget` |
| shutdown lease 解放失敗/タイムアウト | Warn | `task_id` / `remaining`, `error` |

## メトリクス（OpenTelemetry）

Meter 名: `github.com/hirokazumiyaji/tasuki`

| 名前 | 型 | 説明 |
|---|---|---|
| `tasuki.workflow.tasks` | Counter | 処理したワークフロータスク数 |
| `tasuki.activity.tasks` | Counter | 処理したアクティビティタスク数 |
| `tasuki.workflow.terminal` | Counter | ワークフロー終端回数 |
| `tasuki.activity.retries` | Counter | スケジュールしたアクティビティリトライ数 |
| `tasuki.workflow.journal_warnings` | Counter | ジャーナル件数警告の回数 |
| `tasuki.tasks.backlog` | Gauge | キューごとの claim 可能タスク数（属性 `kind`, `queue`）。Worker がポーリング tick ごとにサンプリング |
| `tasuki.worker.incompatible_nacks` | Counter | 非対応タスクの nack 回数（属性 `reason`） |
| `tasuki.worker.store_errors` | Counter | ストア操作失敗数（属性 `op`: `fire_timers`, `claim_schedules`, `claim_workflow`, `claim_activity`, `commit_workflow`, `complete_activity`, `retry_activity`, `release_lease`, `extend_lease`, `recover_tasks`） |

`WorkerOptions.Metrics` に `observability.NewMetrics()` の結果を渡す。未設定時は記録しない（noop）。
グローバル `MeterProvider` が未設定なら OTel 既定の noop 実装が使われる。
backlog Gauge は `Metrics` 設定時のみ、`CountClaimableTasks` でストア上の claim 可能件数を読む（失敗してもタスク処理は継続）。

## 検知条件と切り分け手順

- `tasuki.worker.store_errors{op="claim_workflow"|"claim_activity"}` の継続増加 + 進行停止 → ストア接続・認証・スロットリングを疑う。`store operation failed` ログの `err` と `op` を確認し、DB/エミュレータの死活と `TASUKI_*_DSN` を点検する。
- `op="fire_timers"` の増加 → timer テーブルの肥大・インデックス欠落。Postgres では `wf_timers(fire_at)` の VACUUM 状況を確認する。
- `op="commit_workflow"` の増加 + `ErrConflict` ではない → 永続障害。`debug` の `store contention`（`ErrConflict`/`ErrSuperseded`）は正常系（競合・重複 acquisiton）でアラート対象外。
- `op="complete_activity"` / `"retry_activity"` の増加 → activity 完了パスの障害。`activity failed` と区別する（後者は業務失敗、前者はストア失敗）。
- `op="release_lease"` / `"extend_lease"` の増加 → シャットダウン時や長時間 activity の lease 延長失敗。`ShutdownReleaseTimeout`（既定 5s）超過は Warn ログに残り、lease 失効後の再獲得で回復する。
- `op="recover_tasks"` の増加 → orphan 回復スキャンの失敗。DynamoDB/Firestore の権限・スロットリングを確認する。
- 通常の競合（`ErrConflict`、`ErrSuperseded`）や停止時キャンセル（`context.Canceled`）は Debug のみで `store_errors` に計上しない。継続障害のみ Warn + カウンタで検知し、ラベル数は `op` の固定集合に有界化している。

## PostgreSQL のデッドタプルと Bloat 監視

`wf_tasks` と `wf_inbox` は `ClaimTasks` や `CompleteActivity` によって高頻度で INSERT / UPDATE / DELETE される。
PostgreSQL の MVCC では UPDATE や DELETE の結果がデッドタプルとして残るため、autovacuum が追いつかないとテーブルとインデックスが肥大化する（Table Bloat）。
`ClaimTasks` の `FOR UPDATE SKIP LOCKED` はインデックスを順に走査するため、デッドタプルの蓄積はタスク取得レイテンシの劣化として表面化する。

tasuki は `Migrate` 実行時に高頻度テーブルへ次の reloptions を設定する。
小さいテーブルでは既定値で十分なので、大規模環境での調整が出発点になる。

```sql
-- backend/postgres/migrations/000002_vacuum_tuning.up.sql と同じ内容
ALTER TABLE wf_tasks SET (
    autovacuum_vacuum_scale_factor = 0.05,  -- 既定 0.2: テーブルの 5% がデッドタプルになったら VACUUM
    autovacuum_vacuum_cost_limit = 1000,    -- 既定 200: 1 ラウンドでより多くの掃除を進める
    fillfactor = 80                          -- 更新用の空きを各ページに残し、インデックスを更新しない HOT 更新を促す
);
ALTER TABLE wf_inbox SET (autovacuum_vacuum_scale_factor = 0.05, autovacuum_vacuum_cost_limit = 1000);
ALTER TABLE wf_instances SET (autovacuum_vacuum_scale_factor = 0.05, fillfactor = 80);
ALTER TABLE wf_signal_dedupe SET (autovacuum_vacuum_scale_factor = 0.05);
```

設定値は `pg_class` で確認できる。

```sql
SELECT relname, reloptions FROM pg_class WHERE relname LIKE 'wf_%';
```

### 監視項目

`postgres.Pool()` から監視クエリを流すか、DB 監視スタックから直接収集する。

デッドタプル数とその割合。`n_dead_tup / n_live_tup` が 0.1（10%）を継続的に超えるようなら autovacuum の設定を見直す。

```sql
SELECT relname, n_live_tup, n_dead_tup,
       n_dead_tup::float / GREATEST(n_live_tup, 1) AS dead_ratio,
       last_autovacuum
FROM pg_stat_user_tables
WHERE relname LIKE 'wf_%'
ORDER BY n_dead_tup DESC;
```

自動 VACUUM の実行状況。`last_autovacuum` が古いテーブルはコスト予算が足りていない可能性がある。

Bloat 率（実データに対する無駄な領域の割合）は `pgstattuple` 拡張で近似的に測る。重いクエリなので、切り分け時に限って実行する。

```sql
CREATE EXTENSION IF NOT EXISTS pgstattuple;
SELECT * FROM pgstattuple('wf_tasks');
```

テーブルサイズの推移も併せて記録する。デッドタプルは VACUUM で回収される一方、既に膨らんだテーブルが縮むことはないため、`pg_total_relation_size` が単調増加するようなら VACUUM FULL または `pg_repack` による再編成を検討する。

成長そのものの対策は、完了済みインスタンスの削除（[07-retention.md](07-retention.md)）と長寿命ワークフローの `ContinueAsNew` を組み合わせる。
