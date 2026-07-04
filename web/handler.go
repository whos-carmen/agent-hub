package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	_ "embed"

	"github.com/whos-carmen/agent-hub/engine"
	"github.com/whos-carmen/agent-hub/models"
)

//go:embed static/index.html
var _indexHTML string

// Server handles both the web UI and API endpoints.
type Server struct {
	state   *engine.ConversationState
	subs    *SSESubscriber
	port    int
	server  *http.Server
}

// NewServer creates a new web server.
func NewServer(state *engine.ConversationState, port int) *Server {
	return &Server{
		state: state,
		subs:  NewSSESubscriber(),
		port:  port,
	}
}

// Start begins serving HTTP requests on 0.0.0.0.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /api/messages", s.handleGetMessages)
	mux.HandleFunc("POST /api/send", s.handleSend)
	mux.HandleFunc("GET /api/models", s.handleGetModels)
	mux.HandleFunc("GET /api/stream", s.handleSSE)

	// Session management
	mux.HandleFunc("GET /api/sessions", s.handleGetSessions)
	mux.HandleFunc("POST /api/sessions", s.handleCreateSession)
	mux.HandleFunc("DELETE /api/sessions/{id}", s.handleDeleteSession)
	mux.HandleFunc("PUT /api/sessions/{id}", s.handleRenameSession)
	mux.HandleFunc("GET /api/sessions/{id}/messages", s.handleGetSessionMessages)

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	slog.Info("web server starting", "addr", addr)
	s.server = &http.Server{Addr: addr, Handler: mux}
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

// Broadcast starts forwarding state.Broadcast events to SSE subscribers.
func (s *Server) Broadcast() {
	for evt := range s.state.Broadcast {
		s.subs.Send(evt)
	}
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(_indexHTML))
}

func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	msgs := s.state.GetMessages(sessionID)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handleGetSessionMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	msgs := s.state.GetMessages(id)
	if msgs == nil {
		http.Error(w, "session not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msgs)
}

func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB limit

	var req struct {
		Content string `json:"content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Content == "" {
		http.Error(w, "content required", http.StatusBadRequest)
		return
	}

	select {
	case s.state.Input <- req.Content:
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	default:
		http.Error(w, "system busy, try again", http.StatusServiceUnavailable)
	}
}

func (s *Server) handleGetModels(w http.ResponseWriter, r *http.Request) {
	type ModelInfo struct {
		Name   string `json:"name"`
		Status string `json:"status"`
	}
	models := []ModelInfo{
		{Name: "mimo-pro", Status: "configured"},
		{Name: "deepseek-pro", Status: "configured"},
		{Name: "deepseek-flash", Status: "configured"},
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(models)
}

func (s *Server) handleGetSessions(w http.ResponseWriter, r *http.Request) {
	sessions := s.state.GetSessions()
	activeID := s.state.GetActiveSessionID()
	resp := struct {
		Sessions []*engine.Session `json:"sessions"`
		Active   string           `json:"active"`
	}{Sessions: sessions, Active: activeID}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleCreateSession(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.Name == "" {
		req.Name = fmt.Sprintf("Chat %d", len(s.state.GetSessions())+1)
	}
	sess := s.state.CreateSession(req.Name)

	// Notify SSE clients of session switch
	s.subs.Send(models.StreamEvent{Type: "session_switched", Text: sess.ID})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(sess)
}

func (s *Server) handleDeleteSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if s.state.DeleteSession(id) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "session not found", http.StatusNotFound)
}

func (s *Server) handleRenameSession(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if s.state.RenameSession(id, req.Name) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "session not found", http.StatusNotFound)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := s.subs.Subscribe()
	defer s.subs.Unsubscribe(ch)

	fmt.Fprintf(w, "event: connected\ndata: {}\n\n")
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			data, _ := json.Marshal(evt)
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, data)
			flusher.Flush()
		}
	}
}

// SSESubscriber manages SSE client connections.
type SSESubscriber struct {
	mu      sync.RWMutex
	clients map[chan models.StreamEvent]struct{}
}

func NewSSESubscriber() *SSESubscriber {
	return &SSESubscriber{
		clients: make(map[chan models.StreamEvent]struct{}),
	}
}

func (s *SSESubscriber) Subscribe() chan models.StreamEvent {
	ch := make(chan models.StreamEvent, 64)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *SSESubscriber) Unsubscribe(ch chan models.StreamEvent) {
	s.mu.Lock()
	delete(s.clients, ch)
	s.mu.Unlock()
	close(ch)
}

func (s *SSESubscriber) Send(evt models.StreamEvent) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for ch := range s.clients {
		select {
		case ch <- evt:
		default:
		}
	}
}
