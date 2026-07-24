# tasuki M5 Encrypted Codec

## ゴール

保存ペイロードの at-rest 暗号化。AES-256-GCM の JSON 封筒コーデック、ローテーション対応 Keyring、Client の codec 差し替えオプション。

## タスク

- [ ] Task 1: Spec + Plan
- [ ] Task 2: codec.Encrypted + Keyring（単体テスト）
- [ ] Task 3: Client WithCodec + E2E（memory / postgres jsonb）
- [ ] Task 4: ドキュメント（03-api、README）

## 注記

- Spec: docs/superpowers/specs/2026-07-24-m5-encrypted-codec-design.md
- Plan: docs/superpowers/plans/2026-07-24-m5-encrypted-codec.md
