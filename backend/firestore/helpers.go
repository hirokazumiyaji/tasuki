package firestore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/hirokazumiyaji/tasuki/backend"
	"github.com/hirokazumiyaji/tasuki/journal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func nowUTC() time.Time         { return time.Now().UTC() }
func isNotFound(err error) bool { return status.Code(err) == codes.NotFound }

func newID() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	id := int64(binary.BigEndian.Uint64(b[:]) & 0x7fffffffffffffff)
	if id == 0 {
		return 1
	}
	return id
}

func wfTaskID(instanceID string) string { return "WF#" + instanceID }
func actTaskID(id int64) string         { return "ACT#" + strconv.FormatInt(id, 10) }
func journalID(instanceID string, seq int64) string {
	return instanceID + ":" + strconv.FormatInt(seq, 10)
}
func inboxID(instanceID string, id int64) string {
	return instanceID + ":" + strconv.FormatInt(id, 10)
}
func signalDedupeID(instanceID, dedupeID string) string {
	return instanceID + ":" + escapeDedupeID(dedupeID)
}

// signalDedupeMarkerID derives the document ID of the post-terminal send
// marker for a DedupeID. Markers live in the same wf_signal_dedupe
// collection as user keys, so the two namespaces must be disjoint for every
// user-supplied DedupeID (see escapeDedupeID): the marker is derived from the
// RAW DedupeID and never passed through the user-key escape.
func signalDedupeMarkerID(instanceID, dedupeID string) string {
	return instanceID + ":" + postTerminalDedupeMarker(dedupeID)
}

// dedupeKeyLimit is the Spanner wf_signal_dedupe.dedupe_id STRING(255)
// budget. Firestore has no hard limit here, but both backends share one
// encoding so keys behave identically and stay portable (Codex round 8 on
// #327: a 238-255-char DedupeID plus the marker/escape prefix would otherwise
// exceed the column on Spanner, and the "__" escape adds 2 chars on top).
const dedupeKeyLimit = 255

// dedupeMarkerPrefix prefixes legacy unhashed post-terminal retry markers
// (pre-versioning).
// dedupeHashedUserPrefix prefixes hashed user keys (long IDs that would
// exceed the column budget); dedupeHashedMarkerPrefix prefixes hashed
// markers. All hashed forms stay well under the budget (prefix + 64 hex
// chars) and stay disjoint from every other namespace: hashed users start
// with "__h" (vs "____" escaped, "__p" markers, and no-"__" verbatim), and
// hashed markers start with "__post_terminal__#h:" (vs "__post_terminal__:"
// unhashed markers).
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

// escapeDedupeID encodes a user-supplied DedupeID for storage so it can never
// collide with an internal post-terminal marker (Codex round 6 on #327, with
// the round-9 versioned marker namespace). A user DedupeID of
// "__post_terminal__v1:x" would otherwise share its document with the retry
// marker for user ID "x". IDs starting with "__" gain one extra "__" prefix,
// so every stored user key is either free of a "__" prefix (unescaped) or
// starts with "____" (escaped, since the raw ID already started with "__"),
// while every marker starts with "__post_terminal__v1:" ("__" followed by
// 'p'): the two sets are disjoint, and the encoding is injective, so
// distinct user IDs still map to distinct keys and normal dedupe is
// unaffected. (Pre-escape verbatim rows such as "__post_terminal__:x" predate
// this namespacing; marker probes exclude them by version — see
// dedupeMarkerCandidates.)
//
// Long IDs that would exceed the Spanner column budget (Codex round 8 on
// #327) hash into a bounded form instead: the hashed encoding applies the
// same mapping on write and lookup, so it stays transparent, and its "__h"
// prefix keeps it disjoint from verbatim ("x"), escaped ("____x"), and
// marker ("__post_terminal__v1:..") namespaces — including adversarial raws
// that literally equal a hashed value (those start with "__", so they are
// escaped away and never collide).
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

// postTerminalDedupeMarker derives the post-terminal send marker for a
// DedupeID. Terminal sends always insert their event (round-4 lost-send
// fix: a send racing the terminal transition must not be swallowed by a
// pre-terminal dedupe key snapshotted for the post-commit sweep), but
// retries must still dedupe: the first post-terminal send creates this
// marker alongside the event, and later retries with the same DedupeID see
// the marker and skip. Only the marker suppresses a terminal insert; the
// pre-terminal base key never does. User keys pass through escapeDedupeID on
// storage, so the versioned "__post_terminal__v1:" marker namespace can
// never collide with a user DedupeID, however adversarial.
//
// The marker carries the round-9 namespace version (not the legacy
// "__post_terminal__:" prefix): pre-upgrade user rows stored verbatim under
// the legacy prefix are indistinguishable from legacy markers, so probing
// them would drop genuine post-terminal sends. Marker probes
// (dedupeMarkerCandidates) check versioned forms only; rows predating the
// versioning (legacy user keys and pre-upgrade markers alike) never match.
//
// Long IDs hash into a bounded marker form under the same transparency rule
// as user keys (Codex round 8 on #327). The hashed marker prefix differs
// from the unhashed one so a hashed marker can never equal an unhashed
// marker for a 64-hex-char raw ID.
func postTerminalDedupeMarker(dedupeID string) string {
	if m := dedupeMarkerPrefixV1 + dedupeID; len(m) <= dedupeKeyLimit {
		return m
	}
	return dedupeHashedMarkerPrefixV1 + hashDedupeID(dedupeID)
}

// isPostTerminalMarkerKey reports whether a stored wf_signal_dedupe key is a
// post-terminal retry marker (versioned or legacy-hashed/unhashed form).
// User keys — verbatim, "__"-escaped ("____.."), or hashed ("__hash__:..")
// — never carry these prefixes. The terminate sweep uses this to preserve
// markers (Codex round 8 on #327). Legacy-prefixed rows are still classified
// as markers here (conservative preserve): a pre-upgrade verbatim user row
// such as "__post_terminal__:x" is ambiguous with a pre-upgrade marker, and
// preserving it merely delays its cleanup to purge, while sweeping a genuine
// pre-upgrade marker would duplicate its retry. SendToInbox marker probes do
// NOT use this function — they check versioned forms only
// (dedupeMarkerCandidates), so the ambiguity never drops a send.
func isPostTerminalMarkerKey(stored string) bool {
	return strings.HasPrefix(stored, dedupeMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefix) ||
		strings.HasPrefix(stored, dedupeMarkerPrefixV1) ||
		strings.HasPrefix(stored, dedupeHashedMarkerPrefixV1)
}

// dedupeKeyCandidates lists the stored user-key forms to probe on
// dedupe-check reads, legacy raw first (Codex round 8 on #327): rows written
// before the round-6 escape stored "__"-prefixed IDs verbatim, so a lookup
// for "__x" must probe "__x" before the current "____x", or it misses and
// duplicates the event. Long-ID rows written between round 6 and the round-8
// hash could also hold the over-budget escaped form on Firestore (Spanner
// would have rejected it), so all three forms are probed. Writes always use
// the current escapeDedupeID form.
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

// isPostTerminalMarkerDocID reports whether a wf_signal_dedupe document ID
// holds a post-terminal retry marker. Doc IDs are instanceID + ":" +
// storedKey, so the stored suffix is tested (Codex round 8 on #327).
func isPostTerminalMarkerDocID(docID, instanceID string) bool {
	suffix := docID
	if rest, ok := cutPrefix(docID, instanceID+":"); ok {
		suffix = rest
	}
	return isPostTerminalMarkerKey(suffix)
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return s, false
	}
	return s[len(prefix):], true
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

func jsonString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if !json.Valid(b) {
		wrapped, _ := json.Marshal(string(b))
		return string(wrapped)
	}
	return string(b)
}
func bytes(m map[string]any, key string) []byte {
	s, _ := m[key].(string)
	if s == "" {
		return nil
	}
	return []byte(s)
}
func str(m map[string]any, key string) string { v, _ := m[key].(string); return v }
func i64(m map[string]any, key string) int64 {
	switch v := m[key].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	return 0
}
func boolean(m map[string]any, key string) bool { v, _ := m[key].(bool); return v }
func timestamp(m map[string]any, key string) time.Time {
	v, _ := m[key].(time.Time)
	return v.UTC()
}

func inboxPayload(ev journal.Event) string {
	if ev.Name == "" {
		return jsonString(ev.Payload)
	}
	b, _ := json.Marshal(inboxEnv{Name: ev.Name, Body: ev.Payload})
	return string(b)
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

func decodeInstance(m map[string]any) *backend.Instance {
	return &backend.Instance{ID: str(m, "id"), Name: str(m, "name"), Queue: str(m, "queue"), Status: str(m, "status"),
		Input: bytes(m, "input"), Result: bytes(m, "result"), Failure: bytes(m, "failure"), NextSeq: i64(m, "next_seq"),
		ParentID: str(m, "parent_id"), ParentSeq: i64(m, "parent_seq"),
		SearchAttributes: stringMap(m, "search_attributes"), Memo: stringMap(m, "memo")}
}

func searchAttrsDoc(m map[string]string) map[string]string {
	if len(m) == 0 {
		return map[string]string{}
	}
	return backend.CloneSearchAttributes(m)
}

func stringMap(m map[string]any, key string) map[string]string {
	v, ok := m[key]
	if !ok || v == nil {
		return nil
	}
	switch t := v.(type) {
	case map[string]string:
		return backend.CloneSearchAttributes(t)
	case map[string]any:
		out := make(map[string]string, len(t))
		for k, raw := range t {
			if s, ok := raw.(string); ok {
				out[k] = s
			}
		}
		return backend.CloneSearchAttributes(out)
	default:
		return nil
	}
}
func decodeTask(m map[string]any) backend.Task {
	t := backend.Task{ID: i64(m, "id"), Kind: str(m, "kind"), Queue: str(m, "queue"), InstanceID: str(m, "instance_id"),
		Seq: i64(m, "ref_seq"), Attempt: int(i64(m, "attempt")), MaxAttempts: int(i64(m, "max_attempts")),
		VisibleAt: timestamp(m, "visible_at"), WorkerID: str(m, "worker_id"), HeartbeatDetails: bytes(m, "heartbeat")}
	if t.Kind == "activity" {
		var p activityPayload
		_ = json.Unmarshal(bytes(m, "payload"), &p)
		t.Name, t.Input, t.MaxAttempts = p.Name, p.Input, p.Retry.MaxAttempts
		t.Retry = backend.RetryPolicy{InitialInterval: time.Duration(p.Retry.InitialIntervalMs) * time.Millisecond, BackoffCoefficient: p.Retry.BackoffCoefficient, MaxInterval: time.Duration(p.Retry.MaxIntervalMs) * time.Millisecond, MaxAttempts: p.Retry.MaxAttempts}
		t.StartToCloseTimeout = time.Duration(p.StartToCloseTimeoutMs) * time.Millisecond
	}
	return t
}
