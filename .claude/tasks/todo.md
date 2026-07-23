# tasuki M4 TiDB

## ゴール

`backend/mysql` を TiDB 上で適合・カオスし、流用可否を確定する。

## タスク

- [x] M4 TiDB プラン
- [x] Task 2: docker-compose TiDB + migrate
- [x] Task 3: Conformance on TiDB (+ I1 post-commit ensure)
- [x] Task 4: Conditional-update fallback（不要・docs）
- [x] Task 5: Chaos + README

## 注記

- コミットとマージコミットに `[skip ci]`
- 新規モジュールは原則作らない（mysql 実装を共有）
- ClaimExclusive は TiDB でも PASS（SKIP LOCKED 利用可）
- I1 は CommitAdvancement 後の ensure で硬化
- 次マイルストーン候補: Spanner
