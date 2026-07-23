# tasuki M4 MySQL / MariaDB

## ゴール

MySQL 8（および MariaDB）バックエンドを適合テストとカオスでグリーンにする。

## タスク

- [x] M4 MySQL 実装プラン
- [x] Task 2: Scaffold + migrate
- [x] Task 3–5: Backend + conformance
- [x] Task 6: Chaos + example + README

## 注記

- コミットとマージコミットの両方に `[skip ci]` を付ける
- TiDB / Spanner / DynamoDB / Firestore は後続フェーズ

## 受入条件

- [x] MySQL が `backendtest.Run` を通過
- [x] カオステストが MySQL 上でグリーン
- [x] クイックスタート例が動く
