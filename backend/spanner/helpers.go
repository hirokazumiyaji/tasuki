package spanner

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nowUTC() time.Time { return time.Now().UTC() }

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

// postTerminalDedupeMarker derives the post-terminal send marker for a
// DedupeID. Terminal sends always insert their event (round-4 lost-send
// fix), but retries must still dedupe: the first post-terminal send creates
// this marker alongside the event, and later retries see the marker and
// skip. Only the marker suppresses a terminal insert; the pre-terminal base
// key never does. User keys pass through escapeDedupeID on storage, so the
// "__post_terminal__:" marker namespace can never collide with a user
// DedupeID, however adversarial.
func postTerminalDedupeMarker(dedupeID string) string {
	return "__post_terminal__:" + dedupeID
}

// escapeDedupeID encodes a user-supplied DedupeID for storage so it can never
// collide with an internal post-terminal marker (Codex round 6 on #327). A
// user DedupeID of "__post_terminal__:x" used to share its row with the retry
// marker for user ID "x": a pre-terminal send of the former made the marker
// check for the latter see a row and swallow the first post-terminal send
// (lost signal). IDs starting with "__" gain one extra "__" prefix, so every
// stored user key is either free of a "__" prefix (unescaped) or starts with
// "____" (escaped), while every marker starts with "__post_terminal__:"
// ("__" followed by 'p'): the two sets are disjoint, and the encoding is
// injective, so distinct user IDs still map to distinct keys.
func escapeDedupeID(dedupeID string) string {
	if strings.HasPrefix(dedupeID, "__") {
		return "__" + dedupeID
	}
	return dedupeID
}

// dedupeKey is the wf_signal_dedupe row key for a user DedupeID (escaped).
// dedupeMarkerKey is the row key for its post-terminal send marker, derived
// from the RAW DedupeID and never escaped, so the marker namespace stays
// disjoint from every user key (see escapeDedupeID).
func dedupeKey(dedupeID string) string       { return escapeDedupeID(dedupeID) }
func dedupeMarkerKey(dedupeID string) string { return postTerminalDedupeMarker(dedupeID) }

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
