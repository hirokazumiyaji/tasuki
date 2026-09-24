package spanner

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nowUTC() time.Time { return time.Now().UTC() }

// commitTimestamp stamps swept-table ordering columns (sweep_commit_ts on
// wf_instances; created_at on wf_signal_dedupe, wf_tasks and wf_inbox) with
// the committing transaction's Spanner commit timestamp instead of the
// writer's wall clock (Codex round-22 P2 on #291). Client entry times cannot
// order the terminal cutoff: completed_at and row created_at stamped with
// nowUTC() on different workers skew, so a post-transition signal from a
// slow-clocked sender carries created_at at or below completed_at and the
// sweep deletes it, while a fast-clocked pre-transition row survives. Commit
// timestamps order the two commits exactly, with no cross-process clock
// skew. Columns accept the placeholder only with
// OPTIONS (allow_commit_timestamp=true) — see schema.sql and
// ensureCommitTimestampOptions. Explicit timestamps (tests, backfills,
// legacy rows) remain writable; they compare by wall time as before.
//
// completed_at deliberately stays a client-wall-clock timestamp (NOT a
// commit tick): retention purges compare it against the purger's own clock,
// and a commit tick can postdate that clock (TrueTime uncertainty; the
// emulator's clock runs ~150ms ahead of clients), which would make
// zero-window purges miss fresh victims. Sweep ordering and retention
// ordering use separate ticks so each compares within one time domain.
func commitTimestamp() time.Time { return spanner.CommitTimestamp }

func newID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	id := int64(binary.BigEndian.Uint64(b[:]) & 0x7fffffffffffffff)
	if id == 0 {
		return 1
	}
	return id
}

func isAlreadyExists(err error) bool {
	return status.Code(err) == codes.AlreadyExists
}

func isNotFound(err error) bool {
	return status.Code(err) == codes.NotFound
}

func jsonVal(b []byte) spanner.NullJSON {
	if len(b) == 0 {
		return spanner.NullJSON{}
	}
	if json.Valid(b) {
		return spanner.NullJSON{Value: json.RawMessage(append([]byte(nil), b...)), Valid: true}
	}
	wrapped, _ := json.Marshal(string(b))
	return spanner.NullJSON{Value: json.RawMessage(wrapped), Valid: true}
}

func jsonBytes(n spanner.NullJSON) []byte {
	if !n.Valid || n.Value == nil {
		return nil
	}
	switch t := n.Value.(type) {
	case json.RawMessage:
		return append([]byte(nil), t...)
	case []byte:
		return append([]byte(nil), t...)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return nil
		}
		return b
	}
}

func nullInt(v int64) spanner.NullInt64 {
	if v == 0 {
		return spanner.NullInt64{}
	}
	return spanner.NullInt64{Int64: v, Valid: true}
}

func nullStr(s string) spanner.NullString {
	if s == "" {
		return spanner.NullString{}
	}
	return spanner.NullString{StringVal: s, Valid: true}
}

type activityPayload struct {
	Name                    string          `json:"name"`
	Input                   json.RawMessage `json:"input"`
	Retry                   retryJSON       `json:"retry"`
	StartToCloseTimeoutMs   int64           `json:"start_to_close_timeout_ms,omitempty"`
}

type retryJSON struct {
	InitialIntervalMs  int64   `json:"initial_interval_ms"`
	BackoffCoefficient float64 `json:"backoff_coefficient"`
	MaxIntervalMs      int64   `json:"max_interval_ms"`
	MaxAttempts        int     `json:"max_attempts"`
}

type inboxEnv struct {
	Name string          `json:"_name,omitempty"`
	Body json.RawMessage `json:"_body,omitempty"`
}

func inboxPayload(ev journal.Event) []byte {
	if ev.Name == "" {
		return ev.Payload
	}
	b, _ := json.Marshal(inboxEnv{Name: ev.Name, Body: ev.Payload})
	return b
}

func unwrapInboxPayload(payload []byte) (string, []byte) {
	if len(payload) == 0 {
		return "", nil
	}
	var env inboxEnv
	if err := json.Unmarshal(payload, &env); err == nil && env.Name != "" {
		return env.Name, []byte(env.Body)
	}
	return "", payload
}
