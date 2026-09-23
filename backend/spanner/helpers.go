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

// postTerminalMarkersTable holds post-terminal retry markers OUTSIDE the
// dedupe keyspace (Codex round 11 on #327). Markers used to live as rows of
// wf_signal_dedupe under a "__post_terminal__..:" prefix, but prefixes are
// insufficient: a legacy verbatim user row (old code stored DedupeIDs
// verbatim, so a user ID of "__post_terminal__v1:x" occupies the very row
// the v1 marker probe for "x" reads) collides with the marker in both probe
// directions — the user probe drops a genuine send on a marker row, the
// marker probe drops one on a legacy user row. A disjoint table ends the
// ambiguity structurally: user-key probes never consult it, marker probes
// never consult wf_signal_dedupe. Rows predating the move stay inert in
// wf_signal_dedupe (never probed as markers; user probes skip marker-shaped
// candidates) and drain via purge; the terminate sweep keeps preserving them
// exactly as before.
const postTerminalMarkersTable = "wf_post_terminal_markers"

// postTerminalDedupeMarker derives the post-terminal send marker key for a
// DedupeID. Since the round-11 move, markers are stored as rows of
// postTerminalMarkersTable, not wf_signal_dedupe: the derivation below only
// shapes the marker_key and the stored shape for operators, so no user key
// — however crafted — can share a row with a marker. (History: terminal
// sends always insert their event, but retries must still dedupe: the first
// post-terminal send creates the marker alongside the event, and later
// retries with the same DedupeID see the marker and skip. Only the marker
// suppresses a terminal insert; the pre-terminal base key never does.)
//
// The marker carries the round-9 namespace version (not the legacy
// "__post_terminal__:" prefix).
//
// Long IDs hash into a bounded marker form under the same transparency rule
// as user keys (Codex round 8 on #327).
func postTerminalDedupeMarker(dedupeID string) string {
	if m := dedupeMarkerPrefixV1 + dedupeID; len(m) <= dedupeKeyLimit {
		return m
	}
	return dedupeHashedMarkerPrefixV1 + hashDedupeID(dedupeID)
}

// escapeDedupeID encodes a user-supplied DedupeID for storage. IDs starting
// with "__" gain one extra "__" prefix, so every stored user key is either
// free of a "__" prefix (unescaped) or starts with "____" (escaped), and the
// encoding is injective, so distinct user IDs still map to distinct keys.
// (Pre-escape verbatim rows such as "__post_terminal__:x" predate this
// namespacing; since the round-11 marker move, live markers never share the
// dedupe keyspace — see isPostTerminalMarkerKey — and since round 12 running
// probes honor the raw legacy candidate as the live guard for a
// marker-shaped DedupeID.)
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

// isPostTerminalMarkerKey reports whether a stored wf_signal_dedupe key is
// shaped like a post-terminal retry marker (versioned or legacy form). Since
// the round-11 move, live markers never live in wf_signal_dedupe; this
// classifies only inert pre-upgrade rows. It serves two conservative
// purposes: the terminate sweep preserves such rows (deleting a pre-upgrade
// marker while its inbox event remains would duplicate its retry), and
// terminal base-key probes skip marker-shaped candidates (such a row is an
// inert marker or a legacy verbatim row — never the live guard for the
// probed DedupeID; skipping duplicates at worst, never drops). Running
// probes honor every candidate instead (Codex round 12 on #296): a running
// instance cannot own a post-terminal marker, so a marker-shaped row there
// is unambiguously a legacy user key. User keys written by current code —
// verbatim, "__"-escaped ("____.."), or hashed ("__hash__:..") — never carry
// these prefixes.
func isPostTerminalMarkerKey(stored string) bool {
	return strings.HasPrefix(stored, dedupeMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeMarkerPrefixV1) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefixV1)
}

// dedupeKeyCandidates lists the stored user-key forms to probe on
// dedupe-check reads, legacy raw first (Codex round 8 on #327). Terminal
// base-key probes skip marker-shaped candidates (see
// isPostTerminalMarkerKey); running probes honor every candidate, since a
// marker-shaped row on a running instance is unambiguously a legacy user
// key (Codex round 12 on #296).
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
