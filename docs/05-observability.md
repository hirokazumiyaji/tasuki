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

## メトリクス（OpenTelemetry）

Meter 名: `github.com/hirokazumiyaji/tasuki`

| 名前 | 型 | 説明 |
|---|---|---|
| `tasuki.workflow.tasks` | Counter | 処理したワークフロータスク数 |
| `tasuki.activity.tasks` | Counter | 処理したアクティビティタスク数 |
| `tasuki.workflow.terminal` | Counter | ワークフロー終端回数 |
| `tasuki.activity.retries` | Counter | スケジュールしたアクティビティリトライ数 |
| `tasuki.workflow.journal_warnings` | Counter | ジャーナル件数警告の回数 |

`WorkerOptions.Metrics` に `observability.NewMetrics()` の結果を渡す。未設定時は記録しない（noop）。
グローバル `MeterProvider` が未設定なら OTel 既定の noop 実装が使われる。
