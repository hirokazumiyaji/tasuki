# Durable Actor Boundary Design

## Goal

明示的な Actor Runtime を追加せず、既存の workflow instance を durable actor として説明・整理できる内部境界を導入する。

## Decision

各 workflow instance を一つの durable actor とみなす。
Actor の識別子は `instance_id`、mailbox は `wf_inbox`、状態は journal と instance metadata、dispatch は Worker の task claim とする。
待機中の actor を goroutine として常駐させず、各メッセージ処理は journal replay と一回の atomic commit で完了させる。

既存の `next_seq` CAS、workflow task singleton、lease、activity の at-least-once 実行を変更しない。

## Mapping

| Actor concept | tasuki |
|---|---|
| Identity | `workflow instance_id` |
| Mailbox | `wf_inbox` の未取り込みイベント |
| Private state | journal、instance metadata、仮想的に再構成された `workflow.Context` |
| Message dispatch | Worker が claim した workflow task |
| Single-threaded turn | instance ごとの `workflowActor` による逐次処理 |
| Durable commit | `CommitAdvancement` と `next_seq` の optimistic concurrency |
| Supervision / recovery | lease expiry、task retry、incompatible task の Nack |

## Data flow

```text
external signal / activity completion / timer
                    |
                    v
              durable inbox
                    |
                    v
          workflow task is ensured
                    |
                    v
       load -> replay -> actor turn
                    |
                    v
        one atomic advancement commit
```

Actor turn の中では inbox を journal に仮取り込みして workflow を replay し、新しい command と派生 task を蓄積する。
commit が成功した場合だけ journal、inbox の削除、派生 task、instance の終端状態を可視化する。
競合した turn の結果は `next_seq` CAS によって破棄する。

## Invariants

1. 同一 instance の workflow turn は同時に一つだけ実行する。
2. プロセス内の actor 境界は効率のためのものであり、プロセス間の正しさは Backend の lease と CAS が担う。
3. 待機中の workflow はメモリ上の actor や goroutine を必要としない。
4. Actor turn はユーザーコードの任意の goroutine や channel を許可せず、既存の deterministic workflow API を使う。
5. Activity は actor turn の一部として実行せず、actor が schedule し、完了イベントを次の turn で取り込む。

## Non-goals

- 公開 API に Actor 型、`Send`、`Receive` を追加すること
- 常駐 goroutine による actor の実装
- プロセス間 actor ownership や in-memory mailbox
- 同一 instance の workflow turn の並列化
- hot instance の性能問題を actor 化だけで解決すること

## Error and recovery

Workflow replay の determinism violation は既存どおり incompatible task として Nack する。
Commit conflict は stale turn として扱い、sticky journal を破棄して次の claim で再読する。
actor のプロセスが停止しても、lease の期限後に durable task が再取得される。

## Testing

- `workflowActor` の同一 instance turn が逐次実行されることを unit test で検証する。
- 既存の replay、commit conflict、並行 claim のテストを通す。
- Worker の非同期ログを検証する test-only の出力 sink は、ログ書き込みと読み取りを同期する。
- 全体の race test で actor 境界と sticky cache の組み合わせを検証する。
