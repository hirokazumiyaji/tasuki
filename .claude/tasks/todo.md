# tasuki M5 PostgreSQL Task Notify

## ゴール

PostgreSQL LISTEN/NOTIFY で Worker を起床し、PollInterval 待ちを減らす（ヒントのみ、ticker フォールバック）。

## タスク

- [x] Task 1: Spec + Plan
- [x] Task 2: TaskNotifier + Subscribe + CreateInstance notify
- [x] Task 3: 残りの notify 発火点
- [ ] Task 4: Worker loop 統合
- [ ] Task 5: Conform / chaos / README

## 注記

- Spec: docs/superpowers/specs/2026-07-24-m5-postgres-notify-design.md
- Plan: docs/superpowers/plans/2026-07-24-m5-postgres-notify.md
- Channel: `tasuki_tasks`
