# tasuki M4 MySQL / MariaDB

## ゴール

MySQL 8（および MariaDB）バックエンドを適合テストとカオスでグリーンにする。

## タスク

- [ ] M4 MySQL 実装プラン
- [ ] Task 2: Scaffold + migrate
- [ ] Task 3: Backend core
- [ ] Task 4: Inbox / schedules / List
- [ ] Task 5: Conformance
- [ ] Task 6: Chaos + example + README

## 注記

- コミットとマージコミットの両方に `[skip ci]` を付ける
- TiDB / Spanner / DynamoDB / Firestore は後続フェーズ

## 受入条件

- [ ] MySQL が `backendtest.Run` を通過
- [ ] カオステストが MySQL 上でグリーン
- [ ] クイックスタート例が動く
