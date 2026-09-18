# Modules, versions, and publishing

[English](../10-modules.md) | 日本語

公開単位・依存バージョン・タグ方針。

## モジュール構成

| Module | Path | 公開対象 |
|---|---|---|
| root | `github.com/hirokazumiyaji/tasuki` | ライブラリ本体（workflow/activity/client/worker/memory/bench/contrib/ui）。`GOWORK=off` で独立ビルド可能 |
| dynamodb | `.../backend/dynamodb` | DynamoDB ストア |
| firestore | `.../backend/firestore` | Firestore ストア |
| mysql | `.../backend/mysql` | MySQL ストア |
| postgres | `.../backend/postgres` | Postgres ストア |
| spanner | `.../backend/spanner` | Spanner ストア |
| sqlite | `.../backend/sqlite` | SQLite ストア |

root は既定ビルドで backend サブモジュールに依存しない。`internal/backendopen` は既定で memory のみを内蔵し、他 backend は `-tags tasuki_all` ビルド（workspace 内）で登録される。examples の非 memory 系（m1/m3/m4）と `chaos/cmd/worker` も `tasuki_all` タグ付きのため、既定の `GOWORK=off go list ./...` は root のみで成功する。

## 最小 Go バージョン

- 宣言: root `go 1.24`、backend 各モジュール `go 1.24`、`go.work` も `go 1.24`（CI `1.24.x` と一致）。
- 検証: `GOTOOLCHAIN=local go build ./...`（workspace 外の Go 1.24 環境で確認）。

## 依存バージョン

- root の間接依存（`x/sync`、`x/mod` 等）は `go.mod`/`go.sum` に明示し、workspace の置換で隠さない。CI の `gowork-check` が `GOWORK=off GOPROXY=off go list ./...` で検証する。
- backend ごとの AWS/GCP/DB ドライバは各 backend の `go.mod` に閉じる。root に持ち込まない。

## バージョン付与と公開手順

1. root と各 backend は独立タグで公開する（例: `v0.2.0` は root、`backend/dynamodb/v0.2.0` は DynamoDB モジュール）。
2. backend の `go.mod` の `replace github.com/hirokazumiyaji/tasuki => ../..` はローカル開発用。タグ付け前に置換先の root バージョンが公開済みであることを確認し、必要なら `require github.com/hirokazumiyaji/tasuki vX.Y.Z` に更新する。
3. 公開前チェック:
   ```bash
   # root 独立ビルド
   GOWORK=off GOPROXY=off go list ./...
   GOWORK=off go build ./...
   GOTOOLCHAIN=local go build ./...
   # workspace 全体（全 backend 込み）
   go build -tags tasuki_all ./...
   # 利用者側 smoke（要公開タグ）
   mkdir /tmp/smoke && cd /tmp/smoke && go mod init smoke
   go get github.com/hirokazumiyaji/tasuki@vX.Y.Z
   go get github.com/hirokazumiyaji/tasuki/backend/sqlite@vX.Y.Z # 任意
   ```
4. CLI（`cmd/bench`、`contrib/ui/cmd/tasuki-ui`）は root モジュールに含まれる。非 memory backend を使う CLI ビルドは `-tags tasuki_all` が必要（workspace 内）。

## CI

- `root` ジョブ: `go test ./... -race`（workspace。fuzz の seed は単体テストとしてここで実行）。
- `gowork-check` ジョブ: `GOWORK=off GOPROXY=off go list ./...` + `GOTOOLCHAIN=local go build ./...` で独立性を検証し、`go test -tags tasuki_all ./contrib/... ./examples/...` も実行。
- 各 backend ジョブ: 対応する `backend/<name>` ディレクトリで `go test ./... -race`。
- 各 chaos ジョブ: `kill -9` カオスを `-race` 付きで実行（インフラ不要の `chaos-sqlite` を含む）。
- `lint`/`vuln` ジョブ: root と backend に `staticcheck`（必須）と参考表示の `govulncheck`。
- `fuzz-nightly` ジョブ: nightly schedule（または手動 dispatch）のみ実行。各 codec/engine ターゲットに `-fuzz -fuzztime` 60 秒。
