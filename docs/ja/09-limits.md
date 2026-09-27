# Backend limits and budgets

[English](../09-limits.md) | 日本語

各バックエンドの原子性予算と上限超過時の挙動。

## MaxAdvancementEffects

`backend.Capabilities.MaxAdvancementEffects` は 1 回の workflow advancement で原子的にコミットできる操作数の目安。0 は無制限（memory/postgres/mysql/sqlite/spanner）。

| Backend | 値 | 根拠 |
|---|---|---|
| DynamoDB | 80 | `TransactWriteItems` 上限 100 に対する安全マージン（instance CAS 1 + task delete 1 + 予備 18） |
| Firestore | 400 | トランザクション 500 に対するマージン |
| others | 0（無制限） | 単一 SQL トランザクションで原子確保 |

操作数の数え方（DynamoDB 換算）:

```
2 (instance CAS + task delete)
+ len(NewEvents) (journal puts)
+ len(ActivityTasks) (task puts)
+ len(Timers)
+ len(DrainedInbox) (inbox deletes; ingested は NewEvents に含まれるため inbox 1 件あたり計 2)
+ 3*len(Children) (instance/journal/task)
+ (ParentNotify ? 1 : 0)
```

シグナル重複排除行（`wf_signal_dedupe`）は意図的に上記に**含めない**。

## シグナル重複排除と terminal クリーンアップ（Firestore / Spanner）

`wf_signal_dedupe` はインスタンスの存続期間中 `DedupeID` ごとに 1 行蓄積する。コミット対象トランザクション内でキーごとに 1 行削除すると terminal コミットが履歴件数に比例する。dedupe 600 件のインスタンスは 600 以上の書き込みを要し、Firestore のトランザクション上限 500（予算 400）を超え、Spanner のコミット mutation 上限にも触れる。`TerminateInstance` が `wf_tasks` / `wf_timers` / `wf_signal_dedupe` を 1 コミットで掃く場合も同様である。

- terminal advancement はトランザクション内で上限内の effect のみ確定する（instance 状態、journal puts、activity/timer puts、inbox deletes、children、parent notify、task delete 1 件）。dedupe キーは**コミット後にページング掃き出し**する。Firestore は 400 件ずつ `Batch` + `Limit` ループ、Spanner は 500 キーずつページング read-write トランザクション、DynamoDB は従来通りコミット後ページングである。掃き出しはベストエフォートで、クラッシュ時の残りは retention purge が回収する。terminal インスタンスは不変のため、非トランザクション掃き出しが advancement と競合することはない。
- `TerminateInstance` は `status` を `terminated` にする小トランザクション（read 1 + update 1）のみ先に確定し、その後 `wf_tasks` / `wf_timers` / `wf_signal_dedupe` のページング掃き出し完了を待ってから復帰する。そのため既見 `DedupeID` の再送は正しく新規 inbox として取り込まれ、claim 側に残骸は見えない。inbox/journal 行（あれば）は purge に委ねる。
- Spanner の `PurgeInstances` はインスタンスごとにページング処理する。各子テーブル（`wf_tasks`、`wf_timers`、`wf_signal_dedupe`、`wf_inbox`、`wf_journal`）を 500 キー `LIMIT` ページで掃き、`wf_inbox_seq` + `wf_instances` 行を最後に消し、初回掃き〜インスタンス削除間に割り込んだ書き込みを第 2 掃きで回収する。
- 準拠テスト: `backendtest` の `TerminalLargeDedupe` が dedupe 600 件（上限 500 超）で `TerminateInstance` と terminal `CommitAdvancement` の双方が成功し dedupe が消えることを検証する。

## Fanout の前進方式

`suspended` advancement が予算を超える場合、Worker は新規 commands の prefix のみをコミットし、残りを次 tick の replay に委ねる（journal と task の整合性を保つため中間不整合を作らない）。`EnsureWorkflowTask` で即時フォローアップを確保する。フォローアップは可能な限り同一トランザクション内で原子的に確保する（DynamoDB は singleton タスク行の in-place 更新、Firestore/SQL は同一トランザクション内の upsert）。そのためコミット→ensure の隙間でクラッシュしても残 replay が失われることはない。activity 完了時にもフォローアップが発生するため、前進は止まらない。

- 50 件の `ExecuteAsync` fanout（DynamoDB 換算 102 ops）は 2 回程度に分割して完了する。
- 100 件も同様に分割して完了する（再起動しても scheduled event と task が重複しない。journal の `attribute_not_exists` 条件と replay の prefix 一致で冪等）。
- `inbox`/`timer`/`child` 混在時も同じ式で計算する。inbox drain 単体で予算を超える場合は drain を縮小できず診断エラーにする（`MaxPerInstance` や inbox バッチを見直す）。

## 永続的に収まらない単一操作

- terminal を含む advancement が予算を超える場合は切り詰めず診断エラーにする（例: `tasuki: terminal advancement needs 120 ops, budget 80`）。Worker は terminal turn（完了・失敗・ContinueAsNew・stuck）も事前に検証する。fanout を 1 tick あたりに収まる粒度に分割するか、子ワークフローに分割する。
- `SendToInboxBatch` が予算超の場合は `ErrBatchTooLarge` を返す（`InboxBatchLimit` = `MaxAdvancementEffects/4`、無制限時は 100）。

## Scan コストの限界（DynamoDB）

`ListInstances` は Scan ベースのため、一致件数 M に対するソートは `O(M log M)`（標準ソート）に改善済みだが、Scan 自体の読み取りコスト（全件読み・RCU）は残る。小さい `Limit` でも全件 Scan が発生するため、大量件数の一覧表示は運用で避ける（フィルタ + 別途索引設計が必要な場合は測定に基づき別途設計）。
