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
	Name                  string          `json:"name"`
	Input                 json.RawMessage `json:"input"`
	Retry                 retryJSON       `json:"retry"`
	StartToCloseTimeoutMs int64           `json:"start_to_close_timeout_ms,omitempty"`
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
//
// NOTE on framing (Codex round-16 on #296): unlike Firestore, which
// concatenates instance and key into one document ID (length-prefixed since
// the round-16 fix — see frameDedupeDocID there), Spanner keys are composite
// (instance_id, dedupe_id), so (instance, key) pairs are structurally
// unambiguous here; there is no concatenation to reframe. The key-level
// encodings below are byte-identical to Firestore's, and lookups validate
// the stored instance_id exactly like Firestore's docInstanceMatches (a
// no-op by construction here — the key's instance component always equals
// the stored column — kept as defense in depth so both backends enforce the
// same ownership invariant).
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

// rawFallbackDedupeKey derives the fallback guard key for a DedupeID whose
// canonical key is already occupied by a foreign legacy row (Codex round-16
// on #296). Short IDs fall back to the raw key itself, exactly as before.
// Over-budget IDs cannot: writing the raw long ID would exceed the
// STRING(255) budget, the commit fails, and the event is never delivered —
// so they hash into the __hash__: namespace with a domain-separated
// second-level hash: deterministic, bounded (77 chars), byte-identical to
// Firestore's, and structurally distinct from the occupied canonical hash
// (no "raw:" infix). Residual caveats, both duplicate-never-drop except
// where noted: a true double collision (canonical AND fallback keys both
// foreign-occupied) inserts unguarded, and deliberately reusing another
// send's 77-char fallback hash as your own DedupeID on the same instance
// would match its guard (sender-constructible only — DedupeIDs are
// sender-chosen — so a self-DoS shape, not a cross-user hole; Firestore
// additionally cross-checks the stored canonical form, which Spanner cannot
// record without a schema change since dedupe_id IS the key).
func rawFallbackDedupeKey(dedupeID string) string {
	if len(dedupeID) <= dedupeKeyLimit {
		return dedupeID
	}
	sum := sha256.Sum256([]byte("tasuki/dedupe-raw-fallback/v1\x00" + dedupeID))
	return dedupeHashedUserPrefix + "raw:" + hex.EncodeToString(sum[:])
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

// dedupeFormatVersion stamps new wf_signal_dedupe rows so reads can tell
// canonical (escaped) rows from legacy verbatim rows (Codex round 13 on
// #296). Pre-versioning code stored "__"-prefixed IDs verbatim, so a stored
// key alone is ambiguous: the row at "____x" may be the verbatim guard for
// user ID "____x" or the escaped guard for user ID "__x". Dual-read probes
// over legacy candidates therefore mistake one ID's row for another's and
// skip genuine sends (wrong owner). New writes carry format_version=1 with
// the canonical escapeDedupeID form in both the key and the dedupe_id
// column; rows with a NULL/absent version are legacy (v0) and match only by
// exact raw-ID equality, never as an escaped form of another ID. Databases
// created before the column existed gain it via Migrate (see
// ensureDedupeFormatVersionColumn), with existing rows defaulting to
// NULL = legacy.
const dedupeFormatVersion = 1

const dedupeFormatVersionColumn = "format_version"

// dedupeFormatRawKeyVersion stamps fallback guard rows written at the raw
// (unescaped) key instead of the canonical escaped key. The fallback is used
// when the canonical key is already occupied by a foreign legacy row (which
// cannot be overwritten and must not be mistaken for this ID's guard): the
// raw key with an explicit version still identifies its owner positively
// (legacy rows read a NULL version), so retries keep deduping instead of
// duplicating. Fallback rows keep the raw key in dedupe_id (it is the primary
// key); the v2 match rule consults the key, never the column, so the shapes
// stay unambiguous.
const dedupeFormatRawKeyVersion = 2

// matchDedupeRow reports whether a stored dedupe row guards the requested
// raw DedupeID. candidateKey is the probed stored-key form that located the
// row; storedDedupeID is the row's dedupe_id column; version is its
// format_version column (NULL for legacy rows).
//
//   - Versioned rows always store the canonical escapeDedupeID form, so they
//     match iff the stored column equals the requested ID's canonical form —
//     regardless of which candidate located them. A foreign-owner row (e.g.
//     "__x"'s "____x" found via "____x"'s raw candidate) never matches.
//   - Fallback rows (format_version >= 2) live at the rawFallbackDedupeKey
//     instead of the canonical key, so they match iff the candidate IS that
//     fallback key for the requested ID. (Firestore additionally
//     cross-checks the stored canonical form; Spanner cannot — dedupe_id IS
//     the key — so the documented hash-reuse caveat on rawFallbackDedupeKey
//     applies here.)
//   - Legacy rows were stored verbatim, so they belong to the requested ID
//     iff the stored key IS the requested raw ID exactly. An escaped
//     candidate hitting a legacy row is another ID's row and never matches
//     (safe direction: the send inserts, possibly duplicating, but is never
//     dropped).
//
// Callers additionally gate every hit on the stored instance_id equaling the
// probing instance (see readDedupeRow): a no-op by construction under
// composite keys, kept identical to Firestore's docInstanceMatches as
// defense in depth.
func matchDedupeRow(requestedRaw, candidateKey, storedDedupeID string, version spanner.NullInt64) bool {
	if version.Valid && version.Int64 >= dedupeFormatRawKeyVersion {
		// Fallback guard: the key IS the owner — no other ID's probe can
		// claim it (modulo the documented hash-reuse caveat).
		return candidateKey == rawFallbackDedupeKey(requestedRaw)
	}
	if version.Valid && version.Int64 >= dedupeFormatVersion {
		return storedDedupeID == escapeDedupeID(requestedRaw)
	}
	return candidateKey == requestedRaw
}

// dedupeKeyCandidates lists the stored user-key forms to probe on
// dedupe-check reads, legacy raw first (Codex round 8 on #327). The fallback
// guard key comes last: it is only consulted when the canonical key is
// occupied. Terminal base-key probes skip marker-shaped candidates (see
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
	add(rawFallbackDedupeKey(dedupeID))
	return out
}

func dedupeKey(dedupeID string) string       { return escapeDedupeID(dedupeID) }
func dedupeMarkerKey(dedupeID string) string { return postTerminalDedupeMarker(dedupeID) }

// Exported key-encoding accessors for the cross-backend parity test (see
// backend/firestore/dedupe_parity_test.go): both backends must encode
// DedupeIDs byte-identically even though they store them differently
// (framed Firestore document IDs vs. Spanner composite keys).
func EscapeDedupeID(dedupeID string) string { return escapeDedupeID(dedupeID) }
func PostTerminalDedupeMarker(dedupeID string) string {
	return postTerminalDedupeMarker(dedupeID)
}
func RawFallbackDedupeKey(dedupeID string) string  { return rawFallbackDedupeKey(dedupeID) }
func DedupeKeyCandidates(dedupeID string) []string { return dedupeKeyCandidates(dedupeID) }

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
