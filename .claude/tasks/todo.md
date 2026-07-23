# tasuki M3 運用性

## ゴール

SQLite、cron スケジュール、観測性、決定性解析器、状態閲覧を揃え、M3 受入条件を自動テストでグリーンにする。

## タスク

- [x] M3 実装プラン
- [x] Task 2: Client GetJournal + List
- [x] Task 3: Schedule types + memory
- [x] Task 4: Postgres schedules + worker poller
- [x] Task 5: SQLite scaffold
- [x] Task 6: SQLite backend + conform
- [x] Task 7: Observability
- [x] Task 8: Determinism analyzer
- [x] Task 9: Example + README

## 注記

- 全コミットに `[skip ci]` を付けた（Actions 枠不足のため）

## 受入条件

- [x] SQLite が適合テストを通過
- [x] スケジュール二重発火が ID 重複排除で無効化される
- [x] 解析器が time.Now / go / rand を検出
- [x] List / GetJournal / OTel 文書 / example
