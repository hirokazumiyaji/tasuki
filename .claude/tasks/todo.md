# tasuki M2 表現力

## ゴール

03-api.md の表現力 API（Async/Await、シグナル、子ワークフロー、SideEffect、GetVersion、Cancel、ContinueAsNew）を完成させ、適合テストとドキュメント例でグリーンにする。

## タスク

- [x] M2 実装プラン
- [x] Task 1: Future + child event types
- [x] Task 2: ExecuteAsync / Await
- [x] Task 3: SideEffect / Now / Info
- [x] Task 4: SendToInbox + Signal / Cancel
- [x] Task 5: ReceiveSignal
- [x] Task 6: GetVersion
- [x] Task 7: Cancel compensation
- [x] Task 8: Child workflows
- [x] Task 9: ContinueAsNew
- [x] Task 10: Conform + doctest

## 受入条件

- [x] ExecuteAsync / SleepAsync / Await / AwaitAll
- [x] Signals / Child workflows / SideEffect / GetVersion / Cancel / ContinueAsNew
- [x] 適合テスト（signal race、child notify、cancel compensation）が memory / postgres で通過
- [x] 03-api OrderWorkflow スタイルのドキュメントテストがコンパイル・実行される
