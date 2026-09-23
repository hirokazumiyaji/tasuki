package spanner

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
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

// dedupeKeyLimit is the wf_signal_dedupe.dedupe_id STRING(255) budget.
// Both backends share one encoding so keys behave identically and stay
// portable (Codex round 8 on #327).
const dedupeKeyLimit = 255

const (
	dedupeMarkerPrefix       = "__post_terminal__:"
	dedupeHashedUserPrefix   = "__hash__:"
	dedupeHashedMarkerPrefix = "__post_terminal__#h:"
	// dedupeMarkerPrefixV1 / dedupeHashedMarkerPrefixV1 namespace the
	// versioned (Codex round 9 on #327) post-terminal retry markers. The
	// legacy prefixes above are ambiguous with pre-upgrade user rows:
	// before the round-6 escape, a user DedupeID of "__post_terminal__:x"
	// was stored verbatim as "__post_terminal__:x" — the very key the
	// retry-marker probe for user ID "x" reads. The round-8 dual-read
	// (probing legacy raw forms) therefore mistakes that legacy user row
	// for a marker and swallows the first post-terminal send of "x" (lost
	// signal). Versioned markers live under a disjoint prefix no legacy
	// verbatim row uses, and marker probes check versioned forms only, so
	// unversioned legacy rows never match a marker probe (safe direction:
	// a duplicate event rather than a dropped send; pre-upgrade markers
	// are likewise ignored and may duplicate one retry).
	dedupeMarkerPrefixV1       = "__post_terminal__v1:"
	dedupeHashedMarkerPrefixV1 = "__post_terminal__v1#h:"
)

func hashDedupeID(dedupeID string) string {
	sum := sha256.Sum256([]byte(dedupeID))
	return hex.EncodeToString(sum[:])
}

// postTerminalDedupeMarker derives the post-terminal send marker for a
// DedupeID. Terminal sends always insert their event (round-4 lost-send
// fix), but retries must still dedupe: the first post-terminal send creates
// this marker alongside the event, and later retries see the marker and
// skip. Only the marker suppresses a terminal insert; the pre-terminal base
// key never does. User keys pass through escapeDedupeID on storage, so the
// versioned "__post_terminal__v1:" marker namespace can never collide with a
// user DedupeID, however adversarial.
//
// The marker carries the round-9 namespace version (not the legacy
// "__post_terminal__:" prefix): pre-upgrade user rows stored verbatim under
// the legacy prefix are indistinguishable from legacy markers, so probing
// them would drop genuine post-terminal sends. Marker probes
// (dedupeMarkerCandidates) check versioned forms only; rows predating the
// versioning (legacy user keys and pre-upgrade markers alike) never match.
//
// Long IDs hash into a bounded marker form under the same transparency rule
// as user keys (Codex round 8 on #327).
func postTerminalDedupeMarker(dedupeID string) string {
	if m := dedupeMarkerPrefixV1 + dedupeID; len(m) <= dedupeKeyLimit {
		return m
	}
	return dedupeHashedMarkerPrefixV1 + hashDedupeID(dedupeID)
}

// escapeDedupeID encodes a user-supplied DedupeID for storage so it can never
// collide with an internal post-terminal marker (Codex round 6 on #327, with
// the round-9 versioned marker namespace). A user DedupeID of
// "__post_terminal__v1:x" would otherwise share its row with the retry marker
// for user ID "x". IDs starting with "__" gain one extra "__" prefix, so every
// stored user key is either free of a "__" prefix (unescaped) or starts with
// "____" (escaped), while every marker starts with "__post_terminal__v1:"
// ("__" followed by 'p'): the two sets are disjoint, and the encoding is
// injective, so distinct user IDs still map to distinct keys. (Pre-escape
// verbatim rows such as "__post_terminal__:x" predate this namespacing;
// marker probes exclude them by version — see dedupeMarkerCandidates.)
//
// Long IDs that would exceed the STRING(255) budget hash into a bounded
// "__hash__:" form instead (Codex round 8 on #327); the mapping applies on
// write and lookup alike, so it stays transparent.
func escapeDedupeID(dedupeID string) string {
	var esc string
	if strings.HasPrefix(dedupeID, "__") {
		esc = "__" + dedupeID
	} else {
		esc = dedupeID
	}
	if len(esc) <= dedupeKeyLimit {
		return esc
	}
	return dedupeHashedUserPrefix + hashDedupeID(dedupeID)
}

// isPostTerminalMarkerKey reports whether a stored wf_signal_dedupe key is a
// post-terminal retry marker (versioned or legacy form). The terminate sweep
// uses this to preserve markers (Codex round 8 on #327). Legacy-prefixed
// rows are still classified as markers here (conservative preserve): a
// pre-upgrade verbatim user row such as "__post_terminal__:x" is ambiguous
// with a pre-upgrade marker, and preserving it merely delays its cleanup to
// purge, while sweeping a genuine pre-upgrade marker would duplicate its
// retry. SendToInbox marker probes do NOT use this function — they check
// versioned forms only (dedupeMarkerCandidates), so the ambiguity never
// drops a send.
func isPostTerminalMarkerKey(stored string) bool {
	return strings.HasPrefix(stored, dedupeMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeMarkerPrefixV1) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefixV1)
}

// dedupeKeyCandidates lists the stored user-key forms to probe on
// dedupe-check reads, legacy raw first (Codex round 8 on #327).
func dedupeKeyCandidates(dedupeID string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(k string) {
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	add(dedupeID)
	if strings.HasPrefix(dedupeID, "__") {
		add("__" + dedupeID)
	}
	add(escapeDedupeID(dedupeID))
	return out
}

// dedupeMarkerCandidates lists the stored marker forms to probe on terminal
// retry checks: the current versioned form only (Codex round 9 on #327).
// Writes always use postTerminalDedupeMarker. Legacy unversioned rows —
// pre-upgrade verbatim user keys like "__post_terminal__:x" as well as
// pre-upgrade markers — never match, so a legacy user row cannot swallow a
// genuine post-terminal send (safe direction: a pre-upgrade marker that is
// now ignored may duplicate one retry instead of dropping a send).
func dedupeMarkerCandidates(dedupeID string) []string {
	return []string{postTerminalDedupeMarker(dedupeID)}
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
