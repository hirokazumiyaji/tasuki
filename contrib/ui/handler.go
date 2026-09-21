package ui

import (
	"bytes"
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hirokazumiyaji/tasuki"
	"github.com/hirokazumiyaji/tasuki/backend"
)

//go:embed templates/*
var templateFS embed.FS

type server struct {
	client *tasuki.Client
	tmpl   *template.Template
	secret []byte
}

// NewHandler returns an HTTP handler for instance list, journal detail, terminate, signal, and cancel.
func NewHandler(c *tasuki.Client, opts ...Option) http.Handler {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	tmpl := template.Must(template.New("").ParseFS(templateFS, "templates/*.html"))
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("contrib/ui: crypto/rand: " + err.Error())
	}
	s := &server{client: c, tmpl: tmpl, secret: secret}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleList)
	mux.HandleFunc("GET /instances/{id}", s.handleDetail)
	mux.HandleFunc("POST /instances/{id}/terminate", s.handleTerminate)
	mux.HandleFunc("POST /instances/{id}/signal", s.handleSignal)
	mux.HandleFunc("POST /instances/{id}/cancel", s.handleCancel)
	return withAuth(mux, o.token)
}

type listPage struct {
	Title   string
	Status  string
	Name    string
	Rows    []instanceRow
	Error   string
	Limit   int
	Offset  int
	HasMore bool
	HasPrev bool
	NextURL string
	PrevURL string
}

// Pagination bounds for the instance list. limit is clamped to maxListLimit
// so a single request cannot force an unbounded scan.
const (
	defaultListLimit = 100
	maxListLimit     = 500
)

type instanceRow struct {
	ID     string
	Name   string
	Queue  string
	Status string
}

type detailPage struct {
	Title        string
	ID           string
	Name         string
	Queue        string
	Status       string
	NextSeq      int64
	Events       []eventRow
	Error        string
	NotFound     bool
	CanTerminate bool
	CanSignal    bool
	CanCancel    bool
	CSRFToken    string
}

type eventRow struct {
	Seq     int64
	Type    string
	Name    string
	RefSeq  int64
	Payload string
}

func (s *server) handleList(w http.ResponseWriter, r *http.Request) {
	rid := ensureRequestID(w, r)
	status := r.URL.Query().Get("status")
	name := r.URL.Query().Get("name")
	limit, offset := parseListPagination(r)
	page := listPage{Title: "tasuki", Status: status, Name: name, Limit: limit, Offset: offset}
	list, err := s.client.List(r.Context(), tasuki.InstanceFilter{
		Status: status,
		Name:   name,
		Limit:  limit + 1,
		Offset: offset,
	})
	if err != nil {
		log.Printf("ui list request_id=%s status=%q name=%q limit=%d offset=%d: %v", rid, status, name, limit, offset, err)
		page.Error = "failed to list instances (request id " + rid + ")"
		s.render(w, r, "list.html", page, http.StatusInternalServerError)
		return
	}
	hasMore := len(list) > limit
	if hasMore {
		list = list[:limit]
	}
	for _, inst := range list {
		page.Rows = append(page.Rows, instanceRow{
			ID: inst.ID, Name: inst.Name, Queue: inst.Queue, Status: inst.Status,
		})
	}
	if hasMore {
		page.HasMore = true
		page.NextURL = listPageURL(status, name, limit, offset+limit)
	}
	if offset > 0 {
		page.HasPrev = true
		prev := offset - limit
		if prev < 0 {
			prev = 0
		}
		page.PrevURL = listPageURL(status, name, limit, prev)
	}
	s.render(w, r, "list.html", page, http.StatusOK)
}

// parseListPagination reads ?limit= and ?offset=. Invalid or missing values
// fall back to defaults; limit is clamped to maxListLimit.
func parseListPagination(r *http.Request) (limit, offset int) {
	limit = defaultListLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > maxListLimit {
		limit = maxListLimit
	}
	if s := r.URL.Query().Get("offset"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 {
			offset = v
		}
	}
	return limit, offset
}

// listPageURL rebuilds the list URL preserving filters with new paging.
func listPageURL(status, name string, limit, offset int) string {
	q := url.Values{}
	if status != "" {
		q.Set("status", status)
	}
	if name != "" {
		q.Set("name", name)
	}
	q.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		q.Set("offset", strconv.Itoa(offset))
	}
	u := "/"
	if enc := q.Encode(); enc != "" {
		u += "?" + enc
	}
	return u
}

// ensureRequestID attaches a request ID to the response (and request, so
// downstream helpers like buildDetail can read it). A client-supplied
// X-Request-ID is echoed back; otherwise a random one is generated.
func ensureRequestID(w http.ResponseWriter, r *http.Request) string {
	if id := w.Header().Get("X-Request-ID"); id != "" {
		return id
	}
	if id := r.Header.Get("X-Request-ID"); id != "" {
		w.Header().Set("X-Request-ID", id)
		return id
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		id := strconv.FormatInt(time.Now().UnixNano(), 16)
		w.Header().Set("X-Request-ID", id)
		r.Header.Set("X-Request-ID", id)
		return id
	}
	id := hex.EncodeToString(b[:])
	w.Header().Set("X-Request-ID", id)
	r.Header.Set("X-Request-ID", id)
	return id
}

func currentRequestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-ID"); id != "" {
		return id
	}
	return "unknown"
}

func (s *server) handleDetail(w http.ResponseWriter, r *http.Request) {
	ensureRequestID(w, r)
	id := r.PathValue("id")
	page, code := s.buildDetail(r, id, "")
	s.render(w, r, "detail.html", page, code)
}

func (s *server) handleTerminate(w http.ResponseWriter, r *http.Request) {
	ensureRequestID(w, r)
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	if !verifyCSRF(s.secret, id, r.Form.Get("csrf"), time.Now()) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if r.Form.Get("confirm") == "" {
		http.Error(w, "confirm required", http.StatusBadRequest)
		return
	}
	if err := s.client.Terminate(r.Context(), id); err != nil {
		rid := currentRequestID(r)
		msg := userFacingOpError("terminate", err, rid)
		log.Printf("ui terminate id=%s request_id=%s: %v", id, rid, err)
		page, code := s.buildDetail(r, id, msg)
		if page.NotFound {
			s.render(w, r, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, r, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

func (s *server) handleSignal(w http.ResponseWriter, r *http.Request) {
	ensureRequestID(w, r)
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	if !verifyCSRF(s.secret, id, r.Form.Get("csrf"), time.Now()) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	name := strings.TrimSpace(r.Form.Get("name"))
	if name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}
	raw := strings.TrimSpace(r.Form.Get("payload"))
	var payload any
	if raw != "" {
		if !json.Valid([]byte(raw)) {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if err := json.Unmarshal([]byte(raw), &payload); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
	}
	if err := s.client.Signal(r.Context(), id, name, payload); err != nil {
		rid := currentRequestID(r)
		msg := userFacingOpError("signal", err, rid)
		log.Printf("ui signal id=%s request_id=%s: %v", id, rid, err)
		page, code := s.buildDetail(r, id, msg)
		if page.NotFound {
			s.render(w, r, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, r, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
	ensureRequestID(w, r)
	id := r.PathValue("id")
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	if !verifyCSRF(s.secret, id, r.Form.Get("csrf"), time.Now()) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if r.Form.Get("confirm") == "" {
		http.Error(w, "confirm required", http.StatusBadRequest)
		return
	}
	if err := s.client.Cancel(r.Context(), id); err != nil {
		rid := currentRequestID(r)
		msg := userFacingOpError("cancel", err, rid)
		log.Printf("ui cancel id=%s request_id=%s: %v", id, rid, err)
		page, code := s.buildDetail(r, id, msg)
		if page.NotFound {
			s.render(w, r, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, r, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

// userFacingOpError maps backend errors to messages safe to show clients.
// NotFound stays specific; everything else becomes a generic message carrying
// the request ID, with details only in server logs.
func userFacingOpError(op string, err error, rid string) string {
	if errors.Is(err, backend.ErrNotFound) {
		return "instance not found"
	}
	return "failed to " + op + " instance (request id " + rid + ")"
}

func (s *server) buildDetail(r *http.Request, id, errMsg string) (detailPage, int) {
	rid := currentRequestID(r)
	page := detailPage{Title: "tasuki", ID: id, Error: errMsg}
	inst, err := s.client.Get(r.Context(), id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			page.NotFound = true
			if page.Error == "" {
				page.Error = "instance not found"
			}
			return page, http.StatusNotFound
		}
		log.Printf("ui detail get id=%s request_id=%s: %v", id, rid, err)
		page.NotFound = false
		if page.Error == "" {
			page.Error = "failed to load instance (request id " + rid + ")"
		}
		return page, http.StatusInternalServerError
	}
	page.Name = inst.Name
	page.Queue = inst.Queue
	page.Status = inst.Status
	page.NextSeq = inst.NextSeq
	if inst.Status == tasuki.StatusRunning {
		page.CanTerminate = true
		page.CanSignal = true
		page.CanCancel = true
		page.CSRFToken = issueCSRF(s.secret, id, time.Now())
	}
	events, err := s.client.GetJournal(r.Context(), id)
	if err != nil {
		if errors.Is(err, backend.ErrNotFound) {
			page.NotFound = true
			if page.Error == "" {
				page.Error = "instance not found"
			}
			return page, http.StatusNotFound
		}
		log.Printf("ui detail journal id=%s request_id=%s: %v", id, rid, err)
		if page.Error == "" {
			page.Error = "failed to load instance (request id " + rid + ")"
		}
		return page, http.StatusInternalServerError
	}
	for _, e := range events {
		page.Events = append(page.Events, eventRow{
			Seq: e.Seq, Type: string(e.Type), Name: e.Name, RefSeq: e.RefSeq,
			Payload: truncStr(string(e.Payload), 256),
		})
	}
	return page, http.StatusOK
}

func (s *server) render(w http.ResponseWriter, r *http.Request, name string, data any, code int) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		rid := ensureRequestID(w, r)
		log.Printf("ui render %s request_id=%s: %v", name, rid, err)
		http.Error(w, "internal error (request id "+rid+")", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write(buf.Bytes())
}

func truncStr(s string, n int) string {
	if n <= 0 {
		n = 256
	}
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
