# 公開 API 設計

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
    key := activity.Info(ctx).IdempotencyKey
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
| `Execute[I, O](ctx, fn, in, opts...) (O, error)` | アクティビティを実行し完了を待つ |
| `ExecuteAsync[I, O](ctx, fn, in, opts...) *Future[O]` | アクティビティを開始し Future を返す |
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

`Future[O]` は `Get(ctx) (O, error)` を持つ。
`Await` に異なる型の Future を混ぜられるよう、すべての Future は型を消した `Awaitable` インターフェースを満たす。

### 並行実行の考え方

ワークフロー関数自体は常に単一ゴルーチンで実行される（[02-architecture.md](02-architecture.md) の実行モデル）。
並行なのは「進行中のアクティビティと子ワークフロー」であり、ワークフローコードの並行性は `ExecuteAsync` と `Await` 系で表現する。
ワークフロー内で `go` 文やチャネルを使うことはできない。
それより大きな並行構造が必要な場合は子ワークフローへ分割する。

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

ローリングデプロイ中は新旧ワーカーが混在するため、新コードが記録した marker 以降を旧ワーカーが処理すると stuck になり得る。
stuck はタスクの再投入で回復し、最終的に新ワーカーが拾って前進する。
混在期間を短くするか、大きな変更では新しいワークフロー名（`OrderWorkflowV2`）を切ることを推奨する。
ワーカーのバージョン管理による構造的な解決は将来候補とする（[04-plan.md](04-plan.md)）。

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
`go vet` 互換の静的解析器を提供して検出する（[04-plan.md](04-plan.md) M3）。
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
一時障害で止まらないことを既定とし、打ち切りたい呼び出しに `MaxAttempts` やタイムアウトを与える設計である。

リトライしても意味のないエラー（バリデーション失敗など）は、アクティビティが `tasuki.NonRetryable(err)` で包んで返す。
このエラーは即座に恒久的失敗となり、ワークフロー側へそのまま返る。
`MaxAttempts` 到達時も同様にワークフロー側へエラーが返り、以後の対処（補償、別経路、失敗として終端）はワークフローコードが決める。

## アクティビティの定義と冪等性

アクティビティは `context.Context` を取る通常の関数である。
渡されるコンテキストはリースの残り時間で打ち切られる。

```go
func ChargePayment(ctx context.Context, in ChargeInput) (ChargeResult, error)
```

実行情報は `activity.Info(ctx)` から得る。

```go
type Info struct {
    InstanceID     string
    ActivityName   string
    Attempt        int    // 1 始まり
    IdempotencyKey string // InstanceID とスケジュール seq から成る。リトライ間で不変
}
```

アクティビティは at-least-once 実行である（[02-architecture.md](02-architecture.md)）。
外部システムへの副作用を一度きりにしたい場合は、`IdempotencyKey` を外部 API の冪等キーや一意制約に使う。
このキーはリトライ間で変わらず、同じ論理呼び出しを識別する。

## クライアント API

```go
c := tasuki.NewClient(backend)

h, err := tasuki.Start(ctx, c, OrderWorkflow, in, tasuki.WithID("order-123"))
res, err := h.Result(ctx)                    // 終端までポーリングで待つ
err = c.Signal(ctx, "order-123", "approve", payload)
err = c.Cancel(ctx, "order-123")             // 協調的キャンセル
err = c.Terminate(ctx, "order-123")          // 即時終了
info, err := c.Get(ctx, "order-123")         // 状態、結果、失敗理由
events, err := c.GetJournal(ctx, "order-123") // 実行履歴
list, err := c.List(ctx, tasuki.InstanceFilter{Status: tasuki.StatusStuck})
```

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
通知による即時化は最適化として計画する（[04-plan.md](04-plan.md) M5）。

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
    JournalWarnThreshold int           // 既定 10000 イベント
}
```

`w.Start(ctx)` は非同期にポーラーを起動して即座に返る。
`w.Shutdown(ctx)` は新規獲得を止め、実行中タスクの完了を ctx の期限まで待ち、未完了タスクのリースを解放（`visible_at` を現在時刻へ戻す）してから返る。
リース解放により、他のプロセスがリース期限を待たずに引き継げる。
