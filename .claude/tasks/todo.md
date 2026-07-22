# tasuki M0 実行モデル検証

## ゴール

ジャーナル再実行方式の中核を実装し、M0 受入条件を自動テストでグリーンにする。

## タスク

- [x] M0 実装プラン作成（docs/superpowers/plans/2026-07-23-m0-execution-model.md）
- [x] Task 1: journal event types
- [x] Task 2: command matching
- [x] Task 3: Context + Goexit suspend
- [x] Task 4: determinism stuck
- [x] Task 5: codec + Sleep fire_at
- [x] Task 6: memory backend
- [x] Task 7: worker replay
- [x] Task 8: wftest virtual clock
- [x] Task 9: example + README

## 受入条件

- [x] 複数ステップのリプレイ再開
- [x] 決定性違反 → stuck
- [x] defer / recover がサスペンドを壊さない
- [x] 7 日スリープが仮想時計で完走
