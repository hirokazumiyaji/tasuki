package dynamodb

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/hirokazumiyaji/tasuki/journal"
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

func avS(v string) types.AttributeValue {
	return &types.AttributeValueMemberS{Value: v}
}

func avN(v int64) types.AttributeValue {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(v, 10)}
}

func avBOOL(v bool) types.AttributeValue {
	return &types.AttributeValueMemberBOOL{Value: v}
}

func avJSON(b []byte) types.AttributeValue {
	if len(b) == 0 {
		return &types.AttributeValueMemberNULL{Value: true}
	}
	if !json.Valid(b) {
		wrapped, _ := json.Marshal(string(b))
		b = wrapped
	}
	return &types.AttributeValueMemberS{Value: string(b)}
}

func fromS(av types.AttributeValue) string {
	if m, ok := av.(*types.AttributeValueMemberS); ok {
		return m.Value
	}
	return ""
}

func fromN(av types.AttributeValue) int64 {
	if m, ok := av.(*types.AttributeValueMemberN); ok {
		n, _ := strconv.ParseInt(m.Value, 10, 64)
		return n
	}
	return 0
}

func fromBOOL(av types.AttributeValue) bool {
	if m, ok := av.(*types.AttributeValueMemberBOOL); ok {
		return m.Value
	}
	return false
}

func fromJSON(av types.AttributeValue) []byte {
	switch m := av.(type) {
	case *types.AttributeValueMemberS:
		return []byte(m.Value)
	case *types.AttributeValueMemberNULL:
		return nil
	default:
		return nil
	}
}

func timeToN(t time.Time) int64 {
	return t.UTC().UnixMicro()
}

func nToTime(n int64) time.Time {
	return time.UnixMicro(n).UTC()
}

func claimGSI(kind, queue string) string {
	return kind + "#" + queue
}

func wfTaskPK(instanceID string) string {
	return "WF#" + instanceID
}

func actTaskPK(id int64) string {
	return "ACT#" + strconv.FormatInt(id, 10)
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
