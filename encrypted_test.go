package tasuki_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend/memory"
	"github.com/hirokazumiyaji/tasuki/codec"
	"github.com/hirokazumiyaji/tasuki/journal"
	"github.com/hirokazumiyaji/tasuki/workflow"
)

func TestEncryptedCodec_EndToEnd(t *testing.T) {
	ctx := context.Background()
	b := memory.New()
	kr, err := codec.StaticKeys("k1", map[string][]byte{"k1": bytes.Repeat([]byte{'a'}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	enc := codec.Encrypted(codec.JSON(), kr)

	upper := func(ctx context.Context, s string) (string, error) { return s + "!", nil }

	w := tasuki.NewWorker(b, tasuki.WorkerOptions{PollInterval: time.Millisecond, Codec: enc})
	tasuki.RegisterActivity(w, upper, tasuki.WithName("upper"))
	tasuki.RegisterWorkflow(w, func(wctx *workflow.Context, in string) (string, error) {
		return workflow.Execute[string, string](wctx, "upper", in)
	}, tasuki.WithName("EncWF"))

	c := tasuki.NewClient(b, tasuki.WithCodec(enc))
	h, err := tasuki.Start(ctx, c, "EncWF", "secret-input", tasuki.WithID("enc-1"))
	if err != nil {
		t.Fatal(err)
	}
	w.Start(ctx)
	defer w.Shutdown(ctx)

	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := tasuki.Result[string](rctx, h)
	if err != nil {
		t.Fatal(err)
	}
	if out != "secret-input!" {
		t.Fatalf("got %q", out)
	}

	events, err := c.GetJournal(ctx, "enc-1")
	if err != nil {
		t.Fatal(err)
	}
	mustBeEnvelope := func(evType journal.Type, data []byte) {
		t.Helper()
		var env map[string]any
		if err := json.Unmarshal(data, &env); err != nil {
			t.Fatalf("payload of %s is not JSON: %v", evType, err)
		}
		if env["tasuki_enc"] != float64(1) {
			t.Fatalf("payload of %s is not encrypted: %s", evType, data)
		}
	}
	checked := 0
	for _, ev := range events {
		if len(ev.Payload) == 0 {
			continue
		}
		switch ev.Type {
		case journal.TypeWorkflowStarted, journal.TypeActivityCompleted, journal.TypeWorkflowCompleted:
			mustBeEnvelope(ev.Type, ev.Payload)
			checked++
		case journal.TypeActivityScheduled:
			var sched struct {
				Input json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(ev.Payload, &sched); err != nil {
				t.Fatalf("scheduled payload is not JSON: %v", err)
			}
			mustBeEnvelope(ev.Type, sched.Input)
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no payloads checked")
	}
}
