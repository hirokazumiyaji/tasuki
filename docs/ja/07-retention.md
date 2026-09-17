# データ保持（Retention）

[English](../07-retention.md) | 日本語

長期運用では、完了済みワークフローインスタンスとそのジャーナルが蓄積し、ストレージを圧迫する。`Backend.PurgeInstances` は終端に達したインスタンスとその依存データを一括削除するための運用インターフェースである。

## API

```go
n, err := backend.PurgeInstances(ctx, olderThan time.Duration, statuses []string, limit int) (int, error)
```

引数の意味は次のとおり。

- **olderThan**：完了時刻が `now - olderThan` より前であるインスタンスだけを対象にする。
- **statuses**：対象とする終端ステータス。空（nil）のときは既定値として `{completed, failed, terminated, canceled}` を使う。`continued` も指定できる。
- **limit**：1 回の呼び出しで削除するインスタンス数の上限。0 以下なら `backend.DefaultPurgeLimit`（1000）を使う。

戻り値の `n` は実際に削除したインスタンス数である。

実行中（`running`）、および `stuck` のインスタンスは削除対象にならない。これらのステータスを `statuses` に渡すとエラーになる。誤ったステータス名で稼働中のワークフローを消す事故を防ぐための仕様である。

## 削除されるデータ

1 インスタンスあたり、次の行がすべて削除される。

| テーブル | 内容 |
|---|---|
| `wf_instances` | インスタンス本体 |
| `wf_journal` | 実行履歴イベント |
| `wf_inbox` | 未処理シグナルなどの inbox |
| `wf_tasks` | 残留タスク（遅れて到着した activity 完了など） |
| `wf_timers` | 未来に発火する予定だったタイマー |
| `wf_signal_dedupe` | シグナル重複排除キー |

SQL 系バックエンド（PostgreSQL / MySQL / SQLite / Spanner）は 1 トランザクションで削除する。トランザクションの途中で失敗した場合、そのバッチ全体がロールバックされる。

DynamoDB と Firestore はテーブル間トランザクションを持たないため、1 インスタンス単位のベストエフォート削除になる。削除の途中で失敗すると、そのインスタンスには関連行が一部残る可能性がある。残余は再実行では回収されない（対象から外れるため）。厳密さが必要な場合は、削除前にアーカイブへ退避する方式を推奨する。

## 運用パターン

Purge は専用ジョブから周期的に呼ぶことを想定している。ワーカーのループに入れず、cron や別プロセスで走らせる。

```go
func retentionLoop(ctx context.Context, b backend.Backend) error {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			for {
				n, err := b.PurgeInstances(ctx, 30*24*time.Hour, nil, backend.DefaultPurgeLimit)
				if err != nil {
					return err
				}
				if n < backend.DefaultPurgeLimit {
					break // 対象を拭い切った
				}
			}
		}
	}
}
```

戻り値が上限と等しい間は対象が残っている。同じ呼び出しを繰り返して段階的に削除する。

## 効果

完了済みインスタンスを定期的に落とすことで、`wf_instances`・`wf_journal` のサイズとインデックスサイズが伸び続けない。`ListInstances` や参照系クエリの性能はテーブルサイズに依存するため、保持期間の設定は監視とセットで行う。テーブルサイズの監視については [05-observability.md](05-observability.md) を参照。
