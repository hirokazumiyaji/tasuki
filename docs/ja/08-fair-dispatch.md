# 公平ディスパッチ（Fair Dispatch）

[English](../08-fair-dispatch.md) | 日本語

単一の巨大なワークフローが短時間に大量のアクティビティタスクを発行すると、同一キューの他のワークフローのタスクが後回しになる（Head-of-Line Blocking）。
`ClaimRequest.MaxPerInstance` は、1 回の claim バッチにおけるインスタンスごとのタスク数に上限を付け、この偏りを抑える。

## 動作

既定の claim は `visible_at` 順（FIFO）でタスクを返す。
`MaxPerInstance` を設定すると、FIFO 順を保ったままインスタンスごとに最大 N 件までを取得し、上限を超えたタスクは次回のポーリングに残る。

```text
キュー: [flood-1, flood-2, flood-3, victim-1]   (FIFO 順)

Limit=3, MaxPerInstance=0 → flood-1, flood-2, flood-3   # victim は待たされる
Limit=3, MaxPerInstance=1 → flood-1, victim-1           # victim は最初のバッチに割り込む
```

上限を超えて取得できなかった分でバッチが満たされない場合でも、バッチはそのまま返る。
残ったタスクはポーリング間隔後に再取得できるため、隔離対象のインスタンスはキュー全体の進行を止めず、自身のスループットだけが抑えられる。

候補は FIFO 順のままページングして収集するため、大量のタスクの後ろに隠れた victim もバッチに割り込める。
1 ページの幅は `FairOverfetch(limit)` で決まり、バッチが満たされるか候補を使い切るまで読み進める。

## 使い方

ワーカー単位で設定する。

```go
w := tasuki.NewWorker(b, tasuki.WorkerOptions{
    Queues:         []string{"default"},
    MaxPerInstance: 1,
})
```

バックエンドを直接使う場合（バッチ処理など）は `ClaimRequest.MaxPerInstance` に同じ意味の値を渡す。

対応バックエンド: PostgreSQL / MySQL / SQLite / memory。
Spanner / DynamoDB / Firestore では未対応で、値は無視される（従来どおり FIFO）。

## 高負荷ワークフローの隔離

公平ディスパッチは飢餓を防ぐが、隔離の第一選択はキュー分割である。重いワークフローを専用キューに振り向ければ、設定を一切変更せずに他のワークフローへの影響を zero にできる。

```go
// 重いバッチ処理だけ bulk キューへ
tasuki.Start(ctx, c, BulkWorkflow, in, tasuki.WithQueue("bulk"))

// 通常ワーカーと bulk ワーカーでワーカー自体を分離
tasuki.NewWorker(b, tasuki.WorkerOptions{Queues: []string{"default"}})
tasuki.NewWorker(b, tasuki.WorkerOptions{Queues: []string{"bulk"}, ClaimLimit: 50})
```

キューごとの滞留は `tasuki.tasks.backlog` メトリクス（[05-observability.md](05-observability.md)）と `CountClaimableTasks` で監視でき、bulk ワーカーのオートスケール判定にそのまま使える。

指針として、次のように使い分ける。

- **常時重いワークフロー**：専用キューへ分離（ワーカーも分ける）
- **たまに大きくなるワークフロー**：`MaxPerInstance` で頭打ちし、他のワークフローを守る
- **優先度を付けたい**：キュー分離 + ワーカー数で表現する

## タスク単位の優先度について

タスクごとの優先度カラム（`ORDER BY priority DESC, visible_at`）も可能な設計だが、次の理由で保留している。

- 優先度の付与・変更 API と、低優先度タスクの飢餓防止（エイジング）がセットで必要になる
- 分離キュー + `MaxPerInstance` で既知のユースケースはカバーできる

必要になった時点で、`NewTask` とスキーマに優先度を追加する形で導入する。