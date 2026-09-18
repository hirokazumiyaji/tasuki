# 公開 API 設計

[English](../03-api.md) | 日本語

本書はライブラリ利用者から見た API を定める。
モジュールパスは `github.com/hirokazumiyaji/tasuki`、ルートパッケージ名は `tasuki` とする。

## 設計方針

- ジェネリクスで型を通す。公開 API に `interface{}`（`any`）を出さず、入出力の型はコンパイル時に検査される。
- ワークフロー内で使えるコンテキストは `context.Context` ではなく専用の `*workflow.Context` とする。型を分けることで、ワークフロー内から IO を伴う関数（`context.Context` を要求する）をうっかり呼ぶ誤りを型エラーにできる。
- Go にはジェネリックなメソッドがないため、型パラメータを持つ操作（Execute、Start など）はパッケージ関数として提供する。
- バックエンドは `backend.Backend` インターフェースで差し替えられる。ワークフローとアクティビティのコードはストアに依存せず、バックエンドの変更で書き換えを要求しない。

## 最小構成の例

```go
package main

import (
    "context"
    "time"

    "github.com/hirokazumiyaji/tasuki"
    "github.com/hirokazumiyaji/tasuki/activity"
    "github.com/hirokazumiyaji/tasuki/backend/postgres"
    "github.com/hirokazumiyaji/tasuki/workflow"
    "github.com/jackc/pgx/v5/pgxpool"
)

type OrderInput struct{ OrderID string }
type OrderResult struct{ InvoiceID string }

// ワークフロー: 決定的なオーケストレーションだけを書く
func OrderWorkflow(ctx *workflow.Context, in OrderInput) (OrderResult, error) {
    charge, err := workflow.Execute(ctx, ChargePayment, ChargeInput{OrderID: in.OrderID},
        workflow.WithRetry(workflow.RetryPolicy{MaxAttempts: 5}))
    if err != nil {
        return OrderResult{}, err
    }

    // 配送手配とメール送信を並行実行する
    ship := workflow.ExecuteAsync(ctx, ShipOrder, ShipInput{OrderID: in.OrderID})
    mail := workflow.ExecuteAsync(ctx, SendReceiptMail, MailInput{To: charge.CustomerEmail})
    if _, err := ship.Get(ctx); err != nil {
        return OrderResult{}, err
    }
    if _, err := mail.Get(ctx); err != nil {
        return OrderResult{}, err
    }

    // 7 日後のフォローアップまで durable にスリープする
    if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil {
        return OrderResult{}, err
    }
    if _, err := workflow.Execute(ctx, SendFollowUpMail, MailInput{To: charge.CustomerEmail}); err != nil {
        return OrderResult{}, err
    }
    return OrderResult{InvoiceID: charge.InvoiceID}, nil
}

// アクティビティ: 副作用はここに置く。at-least-once 実行のため冪等に実装する
func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error) {
    key := activity.GetInfo(ctx).IdempotencyKey
    return paymentClient.Charge(ctx, in.OrderID, key)
}

func main() {
    ctx := context.Background()
    pool, _ := pgxpool.New(ctx, "postgres://...")

    w := tasuki.NewWorker(postgres.NewBackend(pool), tasuki.WorkerOptions{})
    tasuki.RegisterWorkflow(w, OrderWorkflow)
    tasuki.RegisterActivity(w, ChargePayment)
    tasuki.RegisterActivity(w, ShipOrder)
    tasuki.RegisterActivity(w, SendReceiptMail)
    tasuki.RegisterActivity(w, SendFollowUpMail)
    w.Start(ctx)
    defer w.Shutdown(ctx)

    // 開始はどのプロセスからでもよい（Client はワーカーなしでも作れる）
    c := tasuki.NewClient(postgres.NewBackend(pool))
    h, _ := tasuki.Start(ctx, c, OrderWorkflow, OrderInput{OrderID: "order-123"},
        tasuki.WithID("order-123"))
    res, _ := h.Result(ctx)
    _ = res
}
```

## ワークフロー内 API

`workflow` パッケージが提供する操作の一覧。

| 関数 | 概要 |
|---|---|
| `Execute[I, O](ctx, fn, in, opts...) (O, error)` | アクティビティを実行し完了を待つ（`WithRetry` / `WithStartToCloseTimeout`） |
| `ExecuteLocal[I, O](ctx, name, in) (O, error)` | 同一 Worker 上で同期実行し結果をジャーナルする（タスクキューなし・リトライなし） |
| `ExecuteAsync[I, O](ctx, fn, in, opts...) *Future[O]` | アクティビティを開始し Future を返す（同じオプション可） |
| `ExecuteChild[I, O](ctx, wf, in, opts...) (O, error)` | 子ワークフローを実行し完了を待つ（Async 版もある） |
| `Sleep(ctx, d) error` / `SleepUntil(ctx, t) error` | durable なタイマーで待つ |
| `SleepAsync(ctx, d) *Future[struct{}]` | タイマーを Future として開始する（タイムアウトの Select 用） |
| `Now(ctx) time.Time` | リプレイで変わらない現在時刻 |
| `ReceiveSignal[T](ctx, name) (T, error)` | シグナルの到着を待つ |
| `ReceiveSignalWithTimeout[T](ctx, name, d) (T, bool, error)` | タイムアウト付きで待つ（第 2 戻り値が受信可否） |
| `SideEffect[T](ctx, fn) (T, error)` | 非決定的な値の生成を一度だけ実行して記録する |
| `NewUUID(ctx) (string, error)` | 記録される UUID 生成（SideEffect の糖衣） |
| `Await(ctx, futures...) (int, error)` | 最初に完了した Future の添字を返す |
| `AwaitAll(ctx, futures...) error` | すべての完了を待ち、最初のエラーを返す |
| `GetVersion(ctx, changeID, min, max) int` | 実行中インスタンスと共存するコード変更の分岐 |
| `ContinueAsNew[I](ctx, in) error` | 履歴を打ち切り新しい実行へ引き継ぐ（return で使うエラー値） |
| `Info(ctx) WorkflowInfo` | インスタンス ID、ワークフロー名、開始時刻 |
| `SetQueryHandler[I, O](ctx, name, fn)` | 読み取り専用のクエリハンドラを登録する（ジャーナルには残らない） |
| `SetUpdateHandler[I, O](ctx, name, fn)` | 実行中インスタンスへの Update ハンドラを登録する（`Execute` / `Sleep` 可） |
| `UpsertSearchAttributes(ctx, attrs)` | 検索属性をマージ更新する（空文字の値はそのキーを削除） |
| `UpsertMemo(ctx, attrs)` | 表示用メモをマージ更新する（空文字の値はそのキーを削除） |

`SetQueryHandler` はリプレイのたびに同じ決定的な位置で呼び出す。
ハンドラは履歴を進めない（`Execute` や `Sleep` など新しいコマンドを記録してはならない）。

`SetUpdateHandler` も同様に毎回同じ位置で登録する。
ハンドラは `*workflow.Context` を受け取り、`Execute` / `Sleep` など通常のワークフロー API を使える。
呼び出しは `tasuki.Update`（Worker 同一プロセス）。任意の `WithUpdateID` で再送冪等。
進行中の Update があるあいだ、メインのワークフローは新しいコマンドを進めない（単一ゴルーチンの協調モデル）。

`UpsertSearchAttributes` は決定的コマンドとしてジャーナルに残り、ペイロードは適用後のマップ全体である。
クエリ実行中に呼ぶと、他の副作用と同様に拒否／サスペンドされる。

`UpsertMemo` も同様にジャーナルに残るが、`List` の絞り込みには使わない（Get で見える表示用メタデータ）。

`ExecuteLocal` は `RegisterActivity` した関数をワークフロータスク内で同期実行する。
通常の `Execute` と違いアクティビティタスクは作らず、リトライも行わない。短い・信頼できる処理向け。結果（またはエラー）は `local_activity` コマンドとしてジャーナルに残り、リプレイではランナーを呼ばない。

ワーカーはタスク実行中、リース半減期ごとに `ExtendLease` でワークフロータスクのリースを延長するため、リプレイやローカル Activity の合計が `LeaseDuration` を超えても他ワーカーに奪われて二重実行にならない。ローカル Activity にはワークフローターンの `context.Context` を渡すため、`Shutdown` でキャンセルできる（コンテキストを無視する処理はターンとシャットダウンを塞ぎ続ける）。1 回の `ExecuteLocal` を期限で区切りたい場合は `WorkerOptions.LocalActivityTimeout` を設定する。

長寿命・ループするワークフローは、イベント数が数千〜1万付近になったら `ContinueAsNew` で履歴を打ち切ることを推奨する（既定の警告しきい値と揃える）。警告自体は実行を止めない。

`Future[O]` は `Get(ctx) (O, error)` を持つ。
`Await` に異なる型の Future を混ぜられるよう、すべての Future は型を消した `Awaitable` インターフェースを満たす。

### 並行実行の考え方

ワークフロー関数自体は常に単一ゴルーチンで実行される（[02-architecture.md](02-architecture.md) の実行モデル）。
並行なのは「進行中のアクティビティと子ワークフロー」であり、ワークフローコードの並行性は `ExecuteAsync` と `Await` 系で表現する。
ワークフロー内で `go` 文やチャネルを使うことはできない。
それより大きな並行構造が必要な場合は子ワークフローへ分割する。

高頻度イベントを 1 インスタンスに集めると遷移が直列化して頭打ちになる（ホットインスタンス）。
キー単位のインスタンス分割や子ワークフローへの枝分けを使う。詳細は [02-architecture.md](02-architecture.md) の「ホットインスタンスと分割指針」。

タイムアウト付きの外部処理は、タイマーとの Select として書ける。

```go
f := workflow.ExecuteAsync(ctx, CallSlowAPI, in)
t := workflow.SleepAsync(ctx, 10*time.Minute)
idx, err := workflow.Await(ctx, f, t)
if err != nil {
    return out, err
}
if idx == 1 {
    return out, ErrAPITimeout // タイマーが先に完了した
}
```

### キャンセルの現れ方

キャンセル要求が取り込まれた後、待ち受け API（Sleep、Get、ReceiveSignal、Await）は `workflow.ErrCanceled` を返す。
`Execute` はキャンセル後も使える。
補償処理（返金、予約の取り消しなど）をアクティビティとして実行してから return するためである。
return したとき、エラーが `ErrCanceled`（を包むもの）ならインスタンスは canceled、それ以外は通常の終端になる。

### GetVersion とコード変更

実行中インスタンスが残っている間、そのインスタンスが通過済みの区間の呼び出し列を変えると決定性違反になる。
呼び出し列が変わる変更は `GetVersion` で分岐する。

```go
v := workflow.GetVersion(ctx, "add-fraud-check", 1, 2)
if v >= 2 {
    if _, err := workflow.Execute(ctx, FraudCheck, in); err != nil {
        return out, err
    }
}
```

`GetVersion` は次の規則で動く。

- 初めて実行に到達した時点で `max` を version_marker として記録し、以後のリプレイでは記録値を返す。
- 記録がない位置のリプレイ（変更前に通過済みの履歴）では `min` を返す。
- version_marker は照合で特別扱いされ、marker を知らない旧コードは読み飛ばす。分岐の実体が異なれば、次のコマンド照合が違反として検出する。

ローリングデプロイ中は新旧ワーカーが混在するため、新コードが記録した履歴を旧ワーカーがリプレイすると決定性違反になり得る。
Worker は決定性違反、および未登録のワークフロー／アクティビティを terminal `stuck` や activity fail にせず、タスクを Nack して再可視にする（`IncompatibleRetryDelay`、既定 5 秒。負数で即時）。
混在が解消すれば新ワーカーが拾って前進する。大きな変更では新しいワークフロー名（`OrderWorkflowV2`）を切ることも有効。

## 決定性の制約

ワークフロー関数は、同じ履歴に対して同じ呼び出し列を生成しなければならない。
禁止事項と代替を対で示す。

| 禁止 | 代替 |
|---|---|
| `time.Now()`、`time.Since` | `workflow.Now(ctx)` |
| `time.Sleep`、`time.After` | `workflow.Sleep` / `SleepAsync` |
| `rand`、UUID 生成 | `workflow.SideEffect` / `workflow.NewUUID` |
| `go` 文、チャネル、`sync` パッケージ | `ExecuteAsync` + `Await`、子ワークフロー |
| map の反復順に依存する分岐、呼び出し順 | キーをソートしてから反復する |
| ネットワーク、ファイル、DB などの IO | アクティビティに置く |
| グローバル可変状態、環境変数の参照 | ワークフロー入力か SideEffect で渡す |
| defer 内の副作用 | defer はサスペンドのたびに実行されるため、純粋な処理のみ許す |

このうち IO とゴルーチンは `*workflow.Context` の型で構造的に防げるが、`time.Now` や map の反復順は型では防げない。
`go vet` 互換の静的解析器（`analyzers/determinism`）を提供して検出する（詳細は [04-testing.md](04-testing.md)）。
すり抜けた違反も、リプレイ時の照合が実行時に検出してインスタンスを stuck に隔離する（違反したままの前進はしない）。

## リトライとエラー

アクティビティのリトライは呼び出し側がポリシーとして与える。

```go
type RetryPolicy struct {
    InitialInterval    time.Duration // 既定 1s
    BackoffCoefficient float64       // 既定 2.0
    MaxInterval        time.Duration // 既定 1m
    MaxAttempts        int           // 既定 0（無制限）
}
```

既定は Temporal と同じく無制限リトライとする。
一時障害で止まらないことを既定とし、打ち切りたい呼び出しには `MaxAttempts` や `WithStartToCloseTimeout` を与える。

`WithStartToCloseTimeout(d)` は **1 試行**の開始から完了までの上限である（`d <= 0` は未指定＝上限なし）。
超過するとその試行は `"activity start-to-close timeout"` で失敗し、通常の失敗と同じく `RetryPolicy` / `MaxAttempts` / `NonRetryable` の対象になる。
ワーカーはアクティビティに渡す `context.Context` を打ち切る（コンテキストを無視する処理は止められない）。

リトライしても意味のないエラー（バリデーション失敗など）は、アクティビティが `tasuki.NonRetryable(err)` で包んで返す。
このエラーは即座に恒久的失敗となり、ワークフロー側へそのまま返る。
`MaxAttempts` 到達時も同様にワークフロー側へエラーが返り、以後の対処（補償、別経路、失敗として終端）はワークフローコードが決める。

## アクティビティの定義と冪等性

アクティビティは `context.Context` を取る通常の関数である。
渡されるコンテキストは、`WithStartToCloseTimeout` を付けた場合はその期限で打ち切られる。
未指定時はリース延長（ハートビート／自動延長）により長く動き続けられる。

```go
func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error)
```

実行情報は `activity.GetInfo(ctx)` から得る。

```go
type Info struct {
    InstanceID     string
    ActivityName   string
    Attempt        int    // 1 始まり
    TaskID         int64
    IdempotencyKey string // InstanceID とスケジュール seq から成る。リトライ間で不変
}
```

長時間アクティビティは `activity.RecordHeartbeat(ctx, details)` でリースを延ばし、進捗を記録できる。
リトライ時は `activity.GetHeartbeatDetails(ctx, &dest)` で直前の details を取り出せる（未記録なら `activity.ErrNoDetails`）。
ワーカーはフォールバックとしてリース半減期ごとの自動延長も行う。

アクティビティは at-least-once 実行である（[02-architecture.md](02-architecture.md)）。
外部システムへの副作用を一度きりにしたい場合は、`IdempotencyKey` を外部 API の冪等キーや一意制約に使う。
このキーはリトライ間で変わらず、同じ論理呼び出しを識別する。

## クライアント API

```go
c := tasuki.NewClient(backend)

h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"))
h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"),
    tasuki.WithSearchAttributes(map[string]string{"tenant": "acme", "order_id": "42"}))
h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"),
    tasuki.WithMemo(map[string]string{"note": "vip"}))
res, err := h.Result(ctx)                    // 終端までポーリングで待つ
err = c.Signal(ctx, "order-123", "approve", payload)
err = c.Signal(ctx, "order-123", "approve", payload, tasuki.WithDedupeID("pay-42"))
err = c.SignalBatch(ctx, "order-123", []tasuki.SignalItem{
    {Name: "approve", Payload: payload, DedupeID: "pay-42"},
    {Name: "note", Payload: note},
})
err = c.Cancel(ctx, "order-123")             // 協調的キャンセル
err = c.Terminate(ctx, "order-123")          // 即時終了
info, err := c.Get(ctx, "order-123")         // 状態、結果、失敗理由、検索属性、メモ
events, err := c.GetJournal(ctx, "order-123") // 実行履歴
list, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusStuck})
list, err := c.List(ctx, tasuki.InstanceFilter{
    Status: tasuki.StatusRunning,
    SearchAttributes: map[string]string{"tenant": "acme"},
})
```

`WithSearchAttributes` は Start 時に文字列キー／値の可視メタデータを付ける。
`List` の `SearchAttributes` は各キーの完全一致を AND で絞り込む（未設定キーは不一致）。
実行中の更新は `workflow.UpsertSearchAttributes`（マージ。空文字は削除）。

`WithMemo` は表示用の文字列注釈を付ける（Get で見える。List フィルタには使わない）。
実行中の更新は `workflow.UpsertMemo`（マージ。空文字は削除）。

`Signal` に `WithDedupeID` を付けると、同じインスタンス内でその ID の再送は inbox に増えない（戻り値は `nil`）。
未指定または空文字のときは従来どおり、送信ごとの到着になる。
dedupe キーはインスタンスが終端になると消える。

`SignalBatch` は同一インスタンスへ複数シグナルを原子的に投入する。
各 item の `DedupeID` は任意で、ヒットした件だけスキップ（全体は成功）。空スライスは no-op。
ストアの書き込み上限を超えると `backend.ErrBatchTooLarge`（部分適用なし）。

`Start` は ID で冪等である。
同じ ID がすでに存在する場合は `tasuki.ErrAlreadyStarted` を返し、そのとき返るハンドルは既存インスタンスを指す。
API ハンドラのリトライで二重開始しない、という組み込み用途で重要な性質のため、エラーではなく正常系の一部として文書化する。

```go
h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID(orderID))
if err != nil && !errors.Is(err, tasuki.ErrAlreadyStarted) {
    return err
}
res, err := h.Result(ctx) // 新規でも既存でも同じに扱える
```

`Handle[O].Result` はポーリング（既定 200ms 間隔）で待つ。
通知による即時化は各バックエンドの起床機構（PostgreSQL の `NOTIFY`、DynamoDB/Firestore のプロセス間起床、他ストアのプロセス内ハブ。詳細は [02-architecture.md](02-architecture.md)）と連携して行われる。

### クエリ

実行中（または終端）のインスタンスから、シグナルなしで派生状態を読むには `tasuki.Query` を使う。
ワークフローを登録した同一プロセスの Worker が必要である（レジストリでハンドラ定義を解決するため）。

```go
out, err := tasuki.Query[struct{}, int](ctx, w, "order-123", "count", struct{}{})
```

内部では journal と可視な inbox を仮 seq で連結してリプレイし、名前付きハンドラを呼ぶ。
Claim や Commit は行わないため、`next_seq` とタスクは変わらない。
未登録の名前は `workflow.ErrUnknownQuery`、未知のインスタンスは `backend.ErrNotFound` を返す。

### Update

実行中インスタンスへリクエスト／レスポンス型の更新を送るには `tasuki.Update` を使う（Worker 同一プロセス）。

```go
out, err := tasuki.Update[ReviseIn, ReviseOut](ctx, w, "order-123", "revise", in,
    tasuki.WithUpdateID("rev-42"))
```

inbox に `update_requested` を入れ、ワークフロータスクで `SetUpdateHandler` を実行する。
ハンドラは `Execute` などでサスペンドでき、完了は `update_completed` としてジャーナルに残る。
同じ `WithUpdateID` の再送は、完了済みなら同じ結果を返す。

## 登録と命名

ワークフローとアクティビティの名前は DB に永続化され、リプレイの照合キーになる。
既定ではリフレクションで関数名（`OrderWorkflow`）を導出するが、関数のリネームは互換性の破壊（実行中インスタンスの stuck 化）になる。
実運用では明示的な名前を推奨する。

```go
tasuki.RegisterWorkflow(w, OrderWorkflow, tasuki.WithName("order"))
```

## シリアライゼーション

入出力とイベントペイロードの直列化は `Codec` で差し替えられる。

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(data []byte, v any) error
}
```

既定は `encoding/json`。
運用上の指針を二つ定める。

- 構造体の進化はフィールド追加までを互換とする。実行中インスタンスが残る間のフィールド削除、リネーム、型変更は避ける。
- ペイロードは小さく保つ。大きなデータは本体を渡さず、オブジェクトストレージのキーなど参照を渡す。

### 暗号化

保存されるユーザーペイロード（入力、結果、アクティビティ入出力、シグナル、SideEffect、子ワークフロー入力）を AES-256-GCM で暗号化する `Encrypted` コーデックを同梱する。
出力は鍵 ID とノンスを含む JSON 封筒であり、jsonb カラムにもそのまま保存できる。

```go
keys, err := codec.StaticKeys("2026-07", map[string][]byte{
    "2026-07": currentKey, // 32 バイト
    "2026-01": oldKey,     // ローテーション済みの鍵も復号用に残す
})
enc := codec.Encrypted(codec.JSON(), keys)

w := tasuki.NewWorker(b, tasuki.WorkerOptions{Codec: enc})
c := tasuki.NewClient(b, tasuki.WithCodec(enc))
```

運用規則を四つ定める。

- Worker と Client に同じコーデックを設定する。
- ローテーションは primary の切り替えで行い、旧鍵は該当ペイロードが残る間 `Lookup` に残す（再暗号化は不要）。
- 封筒マーカーのないペイロードは平文として読むため、既存インスタンスが残るストアでも有効化できる。
- インスタンス ID、ワークフロー名、キュー名、時刻は暗号化されない（メタデータは平文）。

## テスト支援

`wftest` パッケージで、DB なし、仮想時計のユニットテストを書ける。

```go
func TestOrderWorkflow(t *testing.T) {
    env := wftest.New(t)
    wftest.RegisterActivity(env, ChargePayment)
    wftest.MockActivity(env, ShipOrder, func(ctx context.Context, in ShipInput) (ShipResult, error) {
        return ShipResult{TrackingID: "t-1"}, nil
    })

    res, err := wftest.Run(env, OrderWorkflow, OrderInput{OrderID: "o-1"})
    if err != nil {
        t.Fatal(err)
    }
    if res.InvoiceID == "" {
        t.Error("invoice id is empty")
    }
}
```

仮想時計は、進行できるタスクがなくなった時点で最も近いタイマーまで自動で進む。
7 日のスリープを含むワークフローも実時間なしでテストできる。
シグナルは `env.Signal(name, payload)` で任意のタイミングに注入する。

## ワーカー設定

```go
type WorkerOptions struct {
    Queues               []string      // 既定 ["default"]
    WorkflowSlots        int           // 既定 100。同時に処理するワークフロータスク数
    ActivitySlots        int           // 既定 100。同時に実行するアクティビティ数
    PollInterval         time.Duration // 既定 1s
    LeaseDuration        time.Duration // 既定 30s
    WorkerID             string        // 既定 ホスト名 + ランダムサフィックス
    Codec                Codec         // 既定 JSON
    Logger               *slog.Logger  // 既定 slog.Default()
    JournalWarnThreshold int           // 0 → 既定 10000。負数で無効。超過時は Warn + メトリクスのみ
    IncompatibleRetryDelay time.Duration // 0 → 既定 5s。負数で即時再可視。非互換 Nack 後の hidden 時間
    LocalActivityTimeout time.Duration // 既定 0（無制限）。1 回の ExecuteLocal を期限で区切る
}
```

決定性違反や未登録のワークフロー／アクティビティは terminal にせず Nack する（上記 Delay）。
メトリクス `tasuki.worker.incompatible_nacks`。

## スキーマの検証とマイグレーション

ストアのスキーマ管理はバックエンドごとに行う。PostgreSQL バックエンドはバージョニングされたマイグレーションファイル（`backend/postgres/migrations/`）を持ち、詳細は [migrations の README](../../backend/postgres/migrations/README.ja.md) を参照。

ストアが未マイグレーション（必要なテーブルが無い）とき、Worker は起動しない。
`Worker.Start` は、バックエンドが `backend.SchemaValidator` を実装していれば起動前に検証し、失敗したら Error ログを出してポーリングループを起動しない。
`WorkerOptions.DisableSchemaValidation` を `true` にすると、この検証を無効化できる（自己管理でスキーマを用意する運用向け）。

アプリケーション側で明示的に検証したいときは `tasuki.ValidateSchema(ctx, backend)` を使う。
未対応バックエンドに対しては何もしない。

`w.Start(ctx)` は非同期にポーラーを起動して即座に返る。
`w.Shutdown(ctx)` は新規獲得を止め、実行中タスクの完了を ctx の期限まで待ち、未完了タスクのリースを解放（`visible_at` を現在時刻へ戻す）してから返る。
リース解放により、他のプロセスがリース期限を待たずに引き継げる。
