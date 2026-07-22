# tasuki

Go アプリケーションに組み込んで使う durable workflow engine。
名前は駅伝の襷（tasuki）から。走者から走者へ襷を引き継いで長い距離を走り切るように、ワーカーからワーカーへ実行を引き継いでワークフローを完走させる。

Temporal のような「リトライ、タイマー、状態永続化を自分で書かず、ワークフローをコードとして書く」体験を、専用サーバーなしで提供する。
エンジンはライブラリとしてアプリケーションプロセス内で動き、永続化はアプリケーションが持つデータストアに相乗りする。
バックエンドはインターフェースで差し替え可能で、参照実装は PostgreSQL、対応対象に MySQL / MariaDB、SQLite、Spanner、TiDB、DynamoDB、Firestore を含む。

```go
func OrderWorkflow(ctx *workflow.Context, in OrderInput) (OrderResult, error) {
    charge, err := workflow.Execute(ctx, ChargePayment, ChargeInput{OrderID: in.OrderID})
    if err != nil {
        return OrderResult{}, err
    }
    if err := workflow.Sleep(ctx, 7*24*time.Hour); err != nil { // プロセスが再起動しても継続する
        return OrderResult{}, err
    }
    _, err = workflow.Execute(ctx, SendFollowUpMail, MailInput{To: charge.CustomerEmail})
    return OrderResult{InvoiceID: charge.InvoiceID}, err
}
```

## ステータス

M0（実行モデルの検証）完了。ジャーナル再実行、`runtime.Goexit` によるサスペンド、インメモリバックエンド、仮想時計（`wftest`）が動く。
次は M1（PostgreSQL バックエンドと実用最小）へ進む。

```bash
go test ./... -race
go run ./examples/m0-hello/
```

## 設計ドキュメント

| 文書 | 内容 |
|---|---|
| [docs/01-overview.md](docs/01-overview.md) | 目的、要求、非目標、既存プロダクト比較、用語 |
| [docs/02-architecture.md](docs/02-architecture.md) | 実行モデル、exactly-once 状態遷移プロトコル、データモデル |
| [docs/03-api.md](docs/03-api.md) | 公開 API、コード例、決定性の制約、テスト支援 |
| [docs/04-plan.md](docs/04-plan.md) | マイルストーン、テスト戦略、リスク |
| [docs/superpowers/plans/2026-07-23-m0-execution-model.md](docs/superpowers/plans/2026-07-23-m0-execution-model.md) | M0 実装プラン |
