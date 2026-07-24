package ui

import (
	"bytes"
	"crypto/rand"
	"embed"
	"encoding/json"
	"html/template"
	"net/http"
	"strings"
	"time"

	"github.com/hirokazumiyaji/tasuki"
)

//go:embed templates/*
var templateFS embed.FS

type server struct {
	client *tasuki.Client
	tmpl   *template.Template
	secret []byte
}

// NewHandler returns an HTTP handler for instance list, journal detail, terminate, signal, and cancel.
func NewHandler(c *tasuki.Client) http.Handler {
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
	return mux
}

type listPage struct {
	Title  string
	Status string
	Name   string
	Rows   []instanceRow
	Error  string
}

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
	status := r.URL.Query().Get("status")
	name := r.URL.Query().Get("name")
	page := listPage{Title: "tasuki", Status: status, Name: name}
	list, err := s.client.List(r.Context(), tasuki.InstanceFilter{
		Status: status,
		Name:   name,
		Limit:  200,
	})
	if err != nil {
		page.Error = err.Error()
		s.render(w, "list.html", page, http.StatusInternalServerError)
		return
	}
	for _, inst := range list {
		page.Rows = append(page.Rows, instanceRow{
			ID: inst.ID, Name: inst.Name, Queue: inst.Queue, Status: inst.Status,
		})
	}
	s.render(w, "list.html", page, http.StatusOK)
}

func (s *server) handleDetail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	page, code := s.buildDetail(r, id, "")
	s.render(w, "detail.html", page, code)
}

func (s *server) handleTerminate(w http.ResponseWriter, r *http.Request) {
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
		page, code := s.buildDetail(r, id, err.Error())
		if page.NotFound {
			s.render(w, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

func (s *server) handleSignal(w http.ResponseWriter, r *http.Request) {
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
		page, code := s.buildDetail(r, id, err.Error())
		if page.NotFound {
			s.render(w, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

func (s *server) handleCancel(w http.ResponseWriter, r *http.Request) {
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
		page, code := s.buildDetail(r, id, err.Error())
		if page.NotFound {
			s.render(w, "detail.html", page, http.StatusNotFound)
			return
		}
		if code == http.StatusOK {
			code = http.StatusInternalServerError
		}
		s.render(w, "detail.html", page, code)
		return
	}
	http.Redirect(w, r, "/instances/"+id, http.StatusSeeOther)
}

func (s *server) buildDetail(r *http.Request, id, errMsg string) (detailPage, int) {
	page := detailPage{Title: "tasuki", ID: id, Error: errMsg}
	inst, err := s.client.Get(r.Context(), id)
	if err != nil {
		page.NotFound = true
		if page.Error == "" {
			page.Error = err.Error()
		}
		return page, http.StatusNotFound
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
		if page.Error == "" {
			page.Error = err.Error()
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

func (s *server) render(w http.ResponseWriter, name string, data any, code int) {
	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
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
