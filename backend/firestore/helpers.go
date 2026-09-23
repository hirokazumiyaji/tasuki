package firestore

import (
	"crypto/rand"
	"encoding/binary"
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

// escapeDedupeID encodes a user-supplied DedupeID for storage so it can never
// collide with an internal post-terminal marker (Codex round 6 on #327). A
// user DedupeID of "__post_terminal__:x" used to share its document with the
// retry marker for user ID "x": a pre-terminal send of the former made the
// marker check for the latter see a row and swallow the first post-terminal
// send (lost signal). IDs starting with "__" gain one extra "__" prefix, so
// every stored user key is either free of a "__" prefix (unescaped) or starts
// with "____" (escaped, since the raw ID already started with "__"), while
// every marker starts with "__post_terminal__:" ("__" followed by 'p'):
// the two sets are disjoint, and the encoding is injective, so distinct user
// IDs still map to distinct keys and normal dedupe is unaffected.
func escapeDedupeID(dedupeID string) string {
	if strings.HasPrefix(dedupeID, "__") {
		return "__" + dedupeID
	}
	return dedupeID
}

// postTerminalDedupeMarker derives the post-terminal send marker for a
// DedupeID. Terminal sends always insert their event (round-4 lost-send
// fix: a send racing the terminal transition must not be swallowed by a
// pre-terminal dedupe key snapshotted for the post-commit sweep), but
// retries must still dedupe: the first post-terminal send creates this
// marker alongside the event, and later retries with the same DedupeID see
// the marker and skip. Only the marker suppresses a terminal insert; the
// pre-terminal base key never does. User keys pass through escapeDedupeID on
// storage, so the "__post_terminal__:" marker namespace can never collide
// with a user DedupeID, however adversarial.
func postTerminalDedupeMarker(dedupeID string) string {
	return "__post_terminal__:" + dedupeID
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
