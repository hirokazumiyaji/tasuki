# tasuki M6 Worker Incompatible Nack

## タスク

- [x] Task 1: Spec + Plan
- [x] Task 2: Backend.NackTask
- [x] Task 3: Worker paths + tests
- [x] Task 4: Docs

# Durable Actor Boundary

## タスク

- [x] 設計文書と実装計画を作成する
- [x] `workflowActor` の逐次処理テストを追加する
- [x] Worker の workflow task を actor 境界経由にする
- [x] durable actor の対応関係を設計書と README に記載する
- [x] focused test と race test を実行する
- [x] レビュー結果を記録する

## レビュー

- `workflowActor` が instance ごとの workflow turn を逐次化し、Worker は `actorFor` 経由で dispatch する。
- durable mailbox、journal replay、`next_seq` CAS、lease、Backend API、公開 API は変更していない。
- `go test ./... -race -count=1` を実行し、root module の全 package が通過した。
- `backend/postgres`、`backend/mysql`、`backend/spanner`、`backend/dynamodb`、`backend/firestore`、`backend/sqlite` で `go test ./... -count=1` を実行し、全て通過した。
- race 検証を妨げていたログ出力 buffer と incompatible Worker test の競合条件を test-only で修正した。
