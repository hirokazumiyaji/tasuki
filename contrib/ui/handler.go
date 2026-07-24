package ui

import (
	"bytes"
	"embed"
	"html/template"
	"net/http"

	"github.com/hirokazumiyaji/tasuki"
)

//go:embed templates/*
var templateFS embed.FS

type server struct {
	client *tasuki.Client
	tmpl   *template.Template
}

// NewHandler returns a read-only HTTP handler for instance list and journal detail.
func NewHandler(c *tasuki.Client) http.Handler {
	tmpl := template.Must(template.New("").ParseFS(templateFS, "templates/*.html"))
	s := &server{client: c, tmpl: tmpl}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", s.handleList)
	mux.HandleFunc("GET /instances/{id}", s.handleDetail)
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
	Title    string
	ID       string
	Name     string
	Queue    string
	Status   string
	NextSeq  int64
	Events   []eventRow
	Error    string
	NotFound bool
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
	page := detailPage{Title: "tasuki", ID: id}
	inst, err := s.client.Get(r.Context(), id)
	if err != nil {
		page.NotFound = true
		page.Error = err.Error()
		s.render(w, "detail.html", page, http.StatusNotFound)
		return
	}
	page.Name = inst.Name
	page.Queue = inst.Queue
	page.Status = inst.Status
	page.NextSeq = inst.NextSeq
	events, err := s.client.GetJournal(r.Context(), id)
	if err != nil {
		page.Error = err.Error()
		s.render(w, "detail.html", page, http.StatusInternalServerError)
		return
	}
	for _, e := range events {
		page.Events = append(page.Events, eventRow{
			Seq: e.Seq, Type: string(e.Type), Name: e.Name, RefSeq: e.RefSeq,
			Payload: truncStr(string(e.Payload), 256),
		})
	}
	s.render(w, "detail.html", page, http.StatusOK)
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
