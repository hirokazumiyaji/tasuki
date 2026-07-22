# tasuki M1 PostgreSQL バックエンドと実用最小

## ゴール

単一 PostgreSQL を共有する複数プロセスでクラッシュに耐えて動く最小構成を作り、適合テストをインメモリと PostgreSQL でグリーンにする。

## タスク

- [x] M1 実装プラン
- [x] Task 1: Backend interface extension
- [x] Task 2: Conformance suite
- [x] Task 3: Conformance concurrency
- [x] Task 4: PostgreSQL migrate
- [x] Task 5: PostgreSQL backend
- [x] Task 6: Client Result / Terminate
- [x] Task 7: Retry + lease extension
- [x] Task 8: Graceful shutdown
- [x] Task 9: Chaos kill -9
- [x] Task 10: Quickstart + CI

## 受入条件

- [x] 適合テストを memory / postgres が通過
- [x] カオステストで全インスタンスが正しい結果で終端、ジャーナル連続
- [x] README クイックスタートが動く
