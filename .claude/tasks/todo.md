# tasuki M5 Encrypted Codec

## ゴール

保存ペイロードの at-rest 暗号化。AES-256-GCM の JSON 封筒コーデック、ローテーション対応 Keyring、Client の codec 差し替えオプション。

## タスク

- [x] Task 1: Spec + Plan
- [x] Task 2: codec.Encrypted + Keyring（単体テスト）
- [x] Task 3: Client WithCodec + E2E（memory / postgres jsonb）。workflow パッケージが encoding/json 直書きで Codec を素通ししていた設計ギャップを検出し、Context への codec 注入で解消
- [x] Task 4: ドキュメント（03-api、README）

## 注記

- Spec: docs/superpowers/specs/2026-07-24-m5-encrypted-codec-design.md
- Plan: docs/superpowers/plans/2026-07-24-m5-encrypted-codec.md
