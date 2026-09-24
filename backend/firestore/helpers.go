package firestore

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

// incarnationField is the wf_instances field carrying the per-incarnation
// identity token (see newIncarnation).
const incarnationField = "incarnation"

// newIncarnation mints the identity token for one instance incarnation
// (Codex round-21 P1 on #296): 128 crypto-random bits, hex-encoded. Terminal
// sweep and purge fences compare this token — not created_at — so a
// recreated ID can never alias a prior incarnation through clock rollback,
// VM restore, or timestamp precision truncation (all of which can reproduce
// the same created_at). A 128-bit random collision is practically
// impossible, unlike clock-derived equality. Legacy instance docs predate
// the field and carry no token; fences fall back to created_at comparison
// for them (see victimMatches) with that documented caveat.
func newIncarnation() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	// crypto/rand essentially never fails; the fallback keeps
	// CreateInstance total (unique per call via nanotime plus process
	// randomness) rather than failing instance creation.
	return fmt.Sprintf("fallback-%d-%d", time.Now().UnixNano(), newID())
}

func wfTaskID(instanceID string) string { return "WF#" + instanceID }
func actTaskID(id int64) string         { return "ACT#" + strconv.FormatInt(id, 10) }
func journalID(instanceID string, seq int64) string {
	return instanceID + ":" + strconv.FormatInt(seq, 10)
}
func inboxID(instanceID string, id int64) string {
	return instanceID + ":" + strconv.FormatInt(id, 10)
}

// frameDedupeDocID derives the document ID holding a stored dedupe key for
// one instance (Codex round-16 on #296). The framing length-prefixes the
// instance ID, so the (instance, key) pair maps injectively: the old
// instanceID + ":" + key concatenation was ambiguous when both parts are
// free-form strings (user doc ("a", "b:c") and ("a:b", "c") shared "a:b:c"),
// and lookups never validated the stored instance_id, so one instance's
// legacy row could suppress another instance's send (cross-instance skip).
// Probes construct both framings and validate ownership (see
// docInstanceMatches); they never parse doc IDs back, so only the
// terminate-sweep marker classifier parses (via dedupeDocKeySuffix), where
// both misclassification directions stay safe (delayed cleanup or a
// duplicate, never a drop).
func frameDedupeDocID(instanceID, key string) string {
	return strconv.Itoa(len(instanceID)) + ":" + instanceID + ":" + key
}

// legacyDedupeDocID derives the pre-framing document ID for a stored key.
// Rows written before framing keep this form; probes consult it after the
// framed form (upgrade dual-read) until purge reaps them.
func legacyDedupeDocID(instanceID, key string) string {
	return instanceID + ":" + key
}

func signalDedupeID(instanceID, dedupeID string) string {
	return frameDedupeDocID(instanceID, escapeDedupeID(dedupeID))
}

// splitDedupeDocID parses frameDedupeDocID back into its components.
// ok=false for legacy-framed or malformed IDs. Lengths are bytes, matching
// len() at framing time, so multibyte instance IDs round-trip exactly.
func splitDedupeDocID(docID string) (instanceID, key string, ok bool) {
	i := strings.IndexByte(docID, ':')
	if i <= 0 {
		return "", "", false
	}
	n, err := strconv.Atoi(docID[:i])
	if err != nil || n < 0 {
		return "", "", false
	}
	rest := docID[i+1:]
	if len(rest) < n+1 || rest[n] != ':' {
		return "", "", false
	}
	return rest[:n], rest[n+1:], true
}

// dedupeDocKeySuffix extracts the stored-key suffix of a dedupe document ID
// for one instance under either framing (framed first, legacy strip as
// fallback). Only the terminate-sweep marker classifier parses IDs; send
// probes construct both framings instead (see dedupeDocIDs).
func dedupeDocKeySuffix(docID, instanceID string) (string, bool) {
	if inst, key, ok := splitDedupeDocID(docID); ok && inst == instanceID {
		return key, true
	}
	return cutPrefix(docID, instanceID+":")
}

// dedupeDocIDs lists the document IDs to probe for stored-key forms: the
// framed form first, then the legacy form for pre-framing rows (upgrade
// dual-read; writes use the framed form only).
func dedupeDocIDs(instanceID string, keys []string) []string {
	var out []string
	seen := map[string]bool{}
	add := func(id string) {
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	for _, k := range keys {
		add(frameDedupeDocID(instanceID, k))
	}
	for _, k := range keys {
		add(legacyDedupeDocID(instanceID, k))
	}
	return out
}

// markerDocIDs lists the marker documents to probe for a DedupeID: the
// framed form first, then the legacy form for pre-framing markers.
func markerDocIDs(instanceID, dedupeID string) []string {
	return dedupeDocIDs(instanceID, []string{postTerminalDedupeMarker(dedupeID)})
}

// docInstanceMatches validates that a stored dedupe/marker row belongs to
// the probing instance (Codex round-16 on #296, defense in depth with the
// framed IDs above): under legacy framing two (instance, key) pairs could
// share one document, so a hit counts only when the row's instance_id field
// agrees. Rows predate the field only in theory (it has ridden every dedupe
// write since introduction), so a missing field falls back to the
// key/version match instead of forcing a duplicate.
func docInstanceMatches(doc map[string]any, instanceID string) bool {
	owner, _ := doc["instance_id"].(string)
	return owner == "" || owner == instanceID
}

// postTerminalMarkersCollection holds post-terminal retry markers OUTSIDE
// the dedupe keyspace (Codex round 11 on #327). Markers used to live as rows
// of wf_signal_dedupe under a "__post_terminal__..:" prefix, but prefixes
// are insufficient: a legacy verbatim user row (old code stored DedupeIDs
// verbatim, so a user ID of "__post_terminal__v1:x" occupies the very
// document the v1 marker probe for "x" reads) collides with the marker in
// both probe directions — the user probe drops a genuine send on a marker
// row, the marker probe drops one on a legacy user row. A disjoint
// collection ends the ambiguity structurally: user-key probes never consult
// it, marker probes never consult wf_signal_dedupe. Rows predating the move
// stay inert in wf_signal_dedupe (never probed as markers; user probes skip
// marker-shaped candidates) and drain via purge; the terminate sweep keeps
// preserving them exactly as before.
//
// The document ID reuses the versioned marker derivation
// (postTerminalDedupeMarker) so long IDs keep their bounded hashed form and
// the stored dedupe_id field keeps its shape for operators.
const postTerminalMarkersCollection = "wf_post_terminal_markers"

// postTerminalMarkerDocID derives the document ID of the post-terminal send
// marker for a DedupeID in postTerminalMarkersCollection. The framed form
// keeps (instance, marker) pairs injective (see frameDedupeDocID); probes
// consult the legacy concatenation for pre-framing markers (see
// markerDocIDs).
func postTerminalMarkerDocID(instanceID, dedupeID string) string {
	return frameDedupeDocID(instanceID, postTerminalDedupeMarker(dedupeID))
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

// escapeDedupeID encodes a user-supplied DedupeID for storage. IDs starting
// with "__" gain one extra "__" prefix, so every stored user key is either
// free of a "__" prefix (unescaped) or starts with "____" (escaped, since
// the raw ID already started with "__"), and the encoding is injective, so
// distinct user IDs still map to distinct keys and normal dedupe is
// unaffected. (Pre-escape verbatim rows such as "__post_terminal__:x"
// predate this namespacing; since the round-11 marker move, live markers
// never share the dedupe keyspace — see isPostTerminalMarkerKey — and since
// round 12 running probes honor the raw legacy candidate as the live guard
// for a marker-shaped DedupeID.)
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
// DedupeID. Since the round-11 move, markers are stored as documents of
// postTerminalMarkersCollection (see postTerminalMarkerDocID), not as
// wf_signal_dedupe rows: the derivation below only shapes the document ID
// suffix and the stored dedupe_id field, so no user key — however crafted —
// can share a document with a marker. (History: terminal sends always insert
// their event, but retries must still dedupe: the first post-terminal send
// creates the marker alongside the event, and later retries with the same
// DedupeID see the marker and skip. Only the marker suppresses a terminal
// insert; the pre-terminal base key never does.)
//
// The marker carries the round-9 namespace version (not the legacy
// "__post_terminal__:" prefix).
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

// isPostTerminalMarkerKey reports whether a stored wf_signal_dedupe key is
// shaped like a post-terminal retry marker (versioned or legacy-hashed/
// unhashed form). Since the round-11 move, live markers never live in
// wf_signal_dedupe; this classifies only inert pre-upgrade rows. It serves
// two conservative purposes: the terminate sweep preserves such rows
// (deleting a pre-upgrade marker while its inbox event remains would
// duplicate its retry), and terminal base-key probes skip marker-shaped
// candidates (such a row is an inert marker or a legacy verbatim row —
// never the live guard for the probed DedupeID; skipping duplicates at
// worst, never drops). Running probes honor every candidate instead (Codex
// round 12 on #296): a running instance cannot own a post-terminal marker,
// so a marker-shaped row there is unambiguously a legacy user key. User
// keys written by current code — verbatim, "__"-escaped ("____.."), or
// hashed ("__hash__:..") — never carry these prefixes.
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
// the canonical escapeDedupeID form in both the document ID and the
// dedupe_id field; rows without the field are legacy (v0) and match only by
// exact raw-ID equality, never as an escaped form of another ID.
const dedupeFormatVersion = 1

const dedupeFormatVersionField = "format_version"

// dedupeFormatRawKeyVersion stamps fallback guard rows written at the raw
// (unescaped) key instead of the canonical escaped key. The fallback is used
// when the canonical key is already occupied by a foreign legacy row (which
// cannot be overwritten and must not be mistaken for this ID's guard): the
// raw key with an explicit version still identifies its owner positively
// (legacy rows never carry a version), so retries keep deduping instead of
// duplicating. The dedupe_id field still carries the canonical escaped form.
const dedupeFormatRawKeyVersion = 2

// rawFallbackDedupeKey derives the fallback guard key for a DedupeID whose
// canonical key is already occupied by a foreign legacy row (Codex round-16
// on #296). Short IDs fall back to the raw key itself, exactly as before.
// Over-budget IDs cannot: writing the raw long ID would exceed the shared
// STRING(255) budget (Spanner rejects the commit and the event is never
// delivered), so they hash into the __hash__: namespace with a
// domain-separated second-level hash — deterministic, bounded (77 chars),
// and structurally distinct from the occupied canonical hash (which carries
// no "raw:" infix). Residual caveats, both duplicate-never-drop: a true
// double collision (canonical AND fallback keys both foreign-occupied)
// inserts unguarded, and deliberately reusing another ID's 77-char fallback
// hash as your own DedupeID on the same instance would match its guard (the
// v2 rule below still positively identifies the owner for every
// non-adversarial shape).
func rawFallbackDedupeKey(dedupeID string) string {
	if len(dedupeID) <= dedupeKeyLimit {
		return dedupeID
	}
	sum := sha256.Sum256([]byte("tasuki/dedupe-raw-fallback/v1\x00" + dedupeID))
	return dedupeHashedUserPrefix + "raw:" + hex.EncodeToString(sum[:])
}

// matchDedupeRow reports whether a stored dedupe row guards the requested
// raw DedupeID. candidateKey is the probed stored-key form that located the
// row; doc is the row's field map.
//
//   - Versioned rows (format_version >= 1) always store the canonical
//     escapeDedupeID form, so they match iff the stored dedupe_id field
//     equals the requested ID's canonical form — regardless of which
//     candidate located them. A foreign-owner row (e.g. "__x"'s "____x"
//     found via "____x"'s raw candidate) never matches.
//   - Fallback rows (format_version >= 2) live at the rawFallbackDedupeKey
//     instead of the canonical key, so they match iff the candidate IS that
//     fallback key for the requested ID — and the stored dedupe_id still
//     names the canonical form (or, for pre-refinement Spanner rows, the raw
//     key itself), so a deliberately reused fallback hash cannot claim
//     another ID's guard.
//   - Legacy rows (no format_version) were stored verbatim, so they belong
//     to the requested ID iff the row's STORED key IS the requested raw ID
//     exactly — not merely the probe candidate (Codex round-19 on #296). A
//     framing collision lands the probe on a foreign legacy row whose doc ID
//     aliases this key (frameDedupeDocID("3:3","x") is the legacy doc of
//     ("3","3:3:x")): the candidate equals the requested ID by construction,
//     so comparing the candidate mistakes that row for this ID's guard and
//     drops a genuine first send. Comparing the stored dedupe_id instead
//     keeps the send (safe direction: at worst a duplicate) while own-ID
//     verbatim guards still match exactly. Ownership still gates first (see
//     docInstanceMatches): a missing instance_id field (pre-field row) falls
//     back to this key check.
//
// Callers additionally gate every hit on docInstanceMatches: under legacy
// framing a document may hold another instance's row.
func matchDedupeRow(requestedRaw, candidateKey string, doc map[string]any) bool {
	version := i64(doc, dedupeFormatVersionField)
	if version >= dedupeFormatRawKeyVersion {
		// Raw-keyed versioned row (fallback guard): the key IS the owner —
		// no other ID's probe can claim it — with the stored canonical
		// form as a second opinion against hash-reuse confusion.
		stored := str(doc, "dedupe_id")
		return candidateKey == rawFallbackDedupeKey(requestedRaw) &&
			(stored == escapeDedupeID(requestedRaw) || stored == requestedRaw)
	}
	if version >= dedupeFormatVersion {
		return str(doc, "dedupe_id") == escapeDedupeID(requestedRaw)
	}
	return str(doc, "dedupe_id") == requestedRaw
}

// dedupeKeyCandidates lists the stored user-key forms to probe on
// dedupe-check reads, legacy raw first (Codex round 8 on #327): rows written
// before the round-6 escape stored "__"-prefixed IDs verbatim, so a lookup
// for "__x" must probe "__x" before the current "____x", or it misses and
// duplicates the event. Long-ID rows written between round 6 and the round-8
// hash could also hold the over-budget escaped form on Firestore (Spanner
// would have rejected it), so all three forms are probed. The fallback guard
// key comes last: it is only consulted when the canonical key is occupied.
// Writes always use the current escapeDedupeID form. Terminal base-key probes
// skip marker-shaped candidates (see isPostTerminalMarkerKey); running probes
// honor every candidate, since a marker-shaped row on a running instance is
// unambiguously a legacy user key (Codex round 12 on #296).
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

// isPostTerminalMarkerDocID reports whether a wf_signal_dedupe document ID
// holds a post-terminal retry marker. The stored suffix is extracted under
// either doc-ID framing (Codex round-16 on #296) and tested for a marker
// shape (Codex round 8 on #327).
func isPostTerminalMarkerDocID(docID, instanceID string) bool {
	suffix, ok := dedupeDocKeySuffix(docID, instanceID)
	if !ok {
		return false
	}
	return isPostTerminalMarkerKey(suffix)
}

// dedupeKeyProbe is the ownership-aware probe of one stored dedupe/marker
// key under both doc-ID framings (Codex round-18 on #296). framedDoc and
// legacyDoc are the existing rows at each framing (nil when absent); callers
// match owned rows framed-first via matchOwnedDedupeRow and treat either
// framing as blocking creation there (a foreign row still collides).
type dedupeKeyProbe struct {
	framedDoc  map[string]any
	legacyDoc  map[string]any
	framedFree bool
	legacyFree bool
}

// selectOwnedDedupeDoc picks the owned row from the framed/legacy documents
// holding one stored key: the framed row when owned, else the legacy row
// when owned, else none — while reporting occupancy of either framing even
// for foreign rows. An upgraded DB can hold another instance's legacy row
// exactly where this instance's framed probe lands
// (frameDedupeDocID("A","x") == legacyDedupeDocID("1:A","x") == "1:A:x"), so
// returning the first existing document — as getDocEitherFraming did —
// handed the caller a foreign row and skipped the owned legacy candidate:
// the guard was missed and every retry appended unguarded. Ownership-aware
// selection consults both framings and lets the first OWNED match win.
// Marker probes use this directly (any owned marker suppresses); user-key
// probes use matchOwnedDedupeRow below so an owned-but-not-matching framed
// row cannot hide an owned matching legacy row either.
func selectOwnedDedupeDoc(framed, legacy map[string]any, instanceID string) (owned map[string]any, occupied bool) {
	if framed != nil {
		occupied = true
		if docInstanceMatches(framed, instanceID) {
			return framed, true
		}
	}
	if legacy != nil {
		occupied = true
		if docInstanceMatches(legacy, instanceID) {
			return legacy, true
		}
	}
	return nil, occupied
}

// matchOwnedDedupeRow reports whether either framing's row guards the
// requested raw DedupeID: the framed row when owned AND matching, else the
// legacy row when owned AND matching. Ownership alone is not enough — a
// framed row owned by this instance need not guard THIS ID (e.g. another
// ID's canonical guard, or a same-instance foreign-key row), and stopping
// at it would hide the owned legacy guard and duplicate the send.
func matchOwnedDedupeRow(requestedRaw, candidateKey string, pr dedupeKeyProbe, instanceID string) bool {
	if pr.framedDoc != nil && docInstanceMatches(pr.framedDoc, instanceID) &&
		matchDedupeRow(requestedRaw, candidateKey, pr.framedDoc) {
		return true
	}
	return pr.legacyDoc != nil && docInstanceMatches(pr.legacyDoc, instanceID) &&
		matchDedupeRow(requestedRaw, candidateKey, pr.legacyDoc)
}

// dedupeGuardTarget is where a new dedupe guard row is created: the full
// document ID plus the version stamp (dedupeFormatVersion for a canonical
// key, dedupeFormatRawKeyVersion for a fallback key — the stamp travels with
// the KEY, not the framing, so a canonical guard at a legacy-framed doc
// still matches by stored canonical form).
type dedupeGuardTarget struct {
	docID string
	ver   int64
}

// pickDedupeGuardTarget chooses where to create the guard for a DedupeID
// with no owned guard found (Codex round-18 on #296). Each availability flag
// reports whether that candidate doc is free (unoccupied). Preference is
// existing behavior first — framed canonical when the whole canonical key is
// free, then framed fallback when the whole fallback key is free (a row at
// either framing occupies the key: creating over the other framing would
// fork the guard) — with the legacy framings as overflow for framed docs
// occupied by other rows: the legacy leg is consulted by probes (see
// matchOwnedDedupeRow), so the guard still dedupes retries. Docs already
// reserved by earlier items of the same batch count as unavailable:
// transaction reads don't see buffered Creates, so two items choosing one
// doc fail the whole batch deterministically on every retry (round-18 P2).
// ok=false when nothing is free: the caller inserts unguarded
// (duplicate-never-drop) instead of failing the batch.
func pickDedupeGuardTarget(instanceID, canonicalKey, fallbackKey string, canonFramedFree, canonLegacyFree, fbFramedFree, fbLegacyFree bool, reserved map[string]bool) (dedupeGuardTarget, bool) {
	canonFree := canonFramedFree && canonLegacyFree
	fbFree := fbFramedFree && fbLegacyFree
	cands := []dedupeGuardTarget{
		{frameDedupeDocID(instanceID, canonicalKey), dedupeFormatVersion},
		{frameDedupeDocID(instanceID, fallbackKey), dedupeFormatRawKeyVersion},
		{legacyDedupeDocID(instanceID, canonicalKey), dedupeFormatVersion},
		{legacyDedupeDocID(instanceID, fallbackKey), dedupeFormatRawKeyVersion},
	}
	free := []bool{
		canonFree,
		fbFree,
		!canonFramedFree && canonLegacyFree,
		!fbFramedFree && fbLegacyFree,
	}
	for i, c := range cands {
		if !free[i] || reserved[c.docID] {
			continue
		}
		return c, true
	}
	return dedupeGuardTarget{}, false
}

// dualDedupeGuardDoc returns the legacy-format counterpart of a framed guard
// target for rolling-upgrade dual-write (Codex round-24 on #296). New guards
// are created at the framed doc by pickDedupeGuardTarget, but nodes predating
// the length framing probe only instanceID + ":" + key: a retry routed to an
// old node misses the framed-only guard and duplicates the inbox event,
// breaking at-most-once during a mixed rollout — even for ordinary IDs with
// no colons. Writing the same guard under both framings keeps old-format
// readers correct: they see the legacy duplicate, new readers probe framed
// first and match either. The counterpart carries the same stored key and
// version stamp (the stamp travels with the KEY, not the framing), so both
// rows match identically via matchDedupeRow; purge and the terminate sweep
// list by the instance_id field and reap both, and the legacy rows drain
// once the fleet is upgraded. ok=false when there is no counterpart to
// write: the target is already legacy-framed (old readers see it directly),
// its legacy leg is occupied, it aliases the primary, or it is reserved by
// an earlier batch item (transaction reads don't see buffered Creates).
func dualDedupeGuardDoc(instanceID string, target dedupeGuardTarget, canonicalKey, fallbackKey string, canonLegacyFree, fbLegacyFree bool, reserved map[string]bool) (dedupeGuardTarget, bool) {
	var key string
	var legacyFree bool
	switch {
	case target.docID == frameDedupeDocID(instanceID, canonicalKey) && target.ver == int64(dedupeFormatVersion):
		key, legacyFree = canonicalKey, canonLegacyFree
	case target.docID == frameDedupeDocID(instanceID, fallbackKey) && target.ver == int64(dedupeFormatRawKeyVersion):
		key, legacyFree = fallbackKey, fbLegacyFree
	default:
		return dedupeGuardTarget{}, false
	}
	if !legacyFree {
		return dedupeGuardTarget{}, false
	}
	leg := legacyDedupeDocID(instanceID, key)
	if leg == target.docID || reserved[leg] {
		return dedupeGuardTarget{}, false
	}
	return dedupeGuardTarget{docID: leg, ver: target.ver}, true
}

// dualMarkerDoc returns the legacy-format counterpart of a framed
// post-terminal marker for rolling-upgrade dual-write (same round-24 P1 as
// above, in postTerminalMarkersCollection): pre-framing nodes probe only the
// legacy concatenation there and would otherwise miss a framed-only marker
// and duplicate one retry. ok=false when the legacy leg is occupied,
// aliases the primary, or is reserved by an earlier batch item.
func dualMarkerDoc(instanceID, dedupeID string, legacyFree bool, reserved map[string]bool) (string, bool) {
	framed := postTerminalMarkerDocID(instanceID, dedupeID)
	leg := legacyDedupeDocID(instanceID, postTerminalDedupeMarker(dedupeID))
	if !legacyFree || leg == framed || reserved[leg] {
		return "", false
	}
	return leg, true
}

// markerGuardTarget chooses where to stamp a post-terminal marker when no
// owned marker exists yet (Codex round-24 on #296): the framed doc when free
// (existing behavior), else the legacy doc when free (a foreign-occupied
// framed doc must not fail the send deterministically on every retry — the
// legacy leg is probed by new readers, so the marker still dedupes; old
// readers see it directly). ok=false when both legs are occupied by foreign
// rows: the caller stamps no marker and still inserts the event
// (duplicate-never-drop; the retry may duplicate once rather than the send
// failing). Docs reserved by earlier batch items count as unavailable.
func markerGuardTarget(instanceID, dedupeID string, framedFree, legacyFree bool, reserved map[string]bool) (string, bool) {
	framed := postTerminalMarkerDocID(instanceID, dedupeID)
	if framedFree && !reserved[framed] {
		return framed, true
	}
	leg := legacyDedupeDocID(instanceID, postTerminalDedupeMarker(dedupeID))
	if leg != framed && legacyFree && !reserved[leg] {
		return leg, true
	}
	return "", false
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return s, false
	}
	return s[len(prefix):], true
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
