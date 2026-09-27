package firestore

import (
	"testing"

	"github.com/hirokazumiyaji/tasuki/backend"
)

// TestFirestoreTerminalInboxBatchLimitArithmetic pins the round-25 P2 write
// budget: a terminal first-send costs up to 5 writes (2 markers + 2 guards +
// 1 inbox) plus one flushInboxSeqs write, against Firestore's 500-write
// transaction limit. The cap must satisfy cap*5+1 <= 500 while the generic
// InboxBatchLimit (100) does not.
func TestFirestoreTerminalInboxBatchLimitArithmetic(t *testing.T) {
	const firestoreWriteLimit = 500
	if got := firestoreTerminalInboxBatchLimit*5 + 1; got > firestoreWriteLimit {
		t.Fatalf("terminal cap %d needs %d writes, exceeds %d", firestoreTerminalInboxBatchLimit, got, firestoreWriteLimit)
	}
	if bad := backend.DefaultInboxBatchLimit*5 + 1; bad <= firestoreWriteLimit {
		t.Fatalf("generic batch %d needs %d writes, would fit: cap unnecessary?", backend.DefaultInboxBatchLimit, bad)
	}
	if firestoreTerminalInboxBatchLimit >= backend.InboxBatchLimit((&Backend{}).Capabilities()) {
		t.Fatalf("terminal cap %d must be below the generic inbox batch limit", firestoreTerminalInboxBatchLimit)
	}
	// Running batches stay safe at the generic limit (2 guards + 1 inbox).
	if got := backend.DefaultInboxBatchLimit*3 + 1; got > firestoreWriteLimit {
		t.Fatalf("running batch %d needs %d writes, exceeds %d", backend.DefaultInboxBatchLimit, got, firestoreWriteLimit)
	}
}

// TestLegacyLegBlocks pins the round-25 P2 veto rule: a legacy leg occupied
// by a foreign row (different instance or different stored key) must not
// block the framed slot; an owned matching row still blocks (fork avoidance).
func TestLegacyLegBlocks(t *testing.T) {
	const inst = "A"
	canon := escapeDedupeID("__x") // "____x"
	if legacyLegBlocks("__x", canon, nil, inst) {
		t.Fatal("absent legacy leg must not block")
	}
	foreignInstance := map[string]any{"instance_id": "other", "dedupe_id": canon, dedupeFormatVersionField: int64(1)}
	if legacyLegBlocks("__x", canon, foreignInstance, inst) {
		t.Fatal("foreign-instance legacy row must not block")
	}
	foreignKey := map[string]any{"instance_id": inst, "dedupe_id": "other", dedupeFormatVersionField: int64(1)}
	if legacyLegBlocks("__x", canon, foreignKey, inst) {
		t.Fatal("foreign-key legacy row must not block")
	}
	own := map[string]any{"instance_id": inst, "dedupe_id": canon, dedupeFormatVersionField: int64(1)}
	if !legacyLegBlocks("__x", canon, own, inst) {
		t.Fatal("owned matching legacy row must still block")
	}
}
