# tasuki M5 Workflow Parallelism

## ゴール

`WorkflowConcurrency` で Claim 済み workflow を tick 内並列実行（同一 instance は mutex、デフォルト 1）。

## タスク

- [x] Task 1: Spec + Plan
- [x] Task 2: WorkerOptions + tick + instance mutex
- [x] Task 3: Bench flag + README

## 注記

- Spec: docs/superpowers/specs/2026-07-24-m5-workflow-parallel-design.md
- Plan: docs/superpowers/plans/2026-07-24-m5-workflow-parallel.md
