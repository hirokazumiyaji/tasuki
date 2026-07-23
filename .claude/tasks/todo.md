# tasuki M3 運用性

## ゴール

SQLite、cron スケジュール、観測性、決定性解析器、状態閲覧を揃え、M3 受入条件を自動テストでグリーンにする。

## タスク

- [ ] M3 実装プラン
- [ ] Task 2: Client GetJournal + List
- [ ] Task 3: Schedule types + memory
- [ ] Task 4: Postgres schedules + worker poller
- [ ] Task 5: SQLite scaffold
- [ ] Task 6: SQLite backend + conform
- [ ] Task 7: Observability
- [ ] Task 8: Determinism analyzer
- [ ] Task 9: Example + README

## 注記

- 全コミットに `[skip ci]` を付ける（Actions 枠不足のため）

## 受入条件

- [ ] SQLite が適合テストを通過
- [ ] スケジュール二重発火が ID 重複排除で無効化される
- [ ] 解析器が time.Now / go / rand を検出
- [ ] List / GetJournal / OTel 文書 / example
