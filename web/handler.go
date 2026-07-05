package web

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/whos-carmen/agent-hub/engine"
	"github.com/whos-carmen/agent-hub/models"
	"github.com/whos-carmen/agent-hub/repo"
)

//go:embed static/index.html
var _indexHTML string

type Server struct {
	state   *engine.ConversationState
	subs    *SSESubscriber
	port    int
	server  *http.Server
}

func NewServer(state *engine.ConversationState, port int) *Server {
	return &Server{
		state: state,
		subs:  NewSSESubscriber(),
		port:  port,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /api/messages", s.handleGetMessages)
	mux.HandleFunc("POST /api/send", s.handleSend)
	mux.HandleFunc("GET /api/models", s.handleGetModels)
	mux.HandleFunc("GET /api/stream", s.handleSSE)

	mux.HandleFunc("GET /api/sessions", s.handleGetSessions)
	mux.HandleFunc("POST /api/sessions", s.handleCreateSession)
	mux.HandleFunc("DELETE /api/sessions/{id}", s.handleDeleteSession)
	mux.HandleFunc("PUT /api/sessions/{id}", s.handleRenameSession)
	mux.HandleFunc("GET /api/sessions/{id}/messages", s.handleGetSessionMessages)

	mux.HandleFunc("GET /api/system", s.handleGetSystem)
	mux.HandleFunc("GET /api/git", s.handleGetGit)
	mux.HandleFunc("POST /api/git/config", s.handleSetGitConfig)

	addr := fmt.Sprintf("0.0.0.0:%d", s.port)
	slog.Info("web server starting", "addr", addr)
	s.server = &http.Server{Addr: addr, Handler: mux}
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}

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
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req struct {
		Content   string   `json:"content"`
		FilePaths []string `json:"file_paths,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Content == "" {
		http.Error(w, "content required", http.StatusBadRequest)
		return
	}

	// If file paths provided, read them and prepend as context
	input := req.Content
	if len(req.FilePaths) > 0 {
		files, err := repo.ReadFiles(req.FilePaths)
		if err != nil {
			http.Error(w, fmt.Sprintf("error reading files: %v", err), http.StatusBadRequest)
			return
		}
		fileContext := repo.FormatFilesAsContext(files)
		input = fileContext + "---\n\n" + input
	}

	select {
	case s.state.Input <- input:
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

func (s *Server) handleGetSystem(w http.ResponseWriter, r *http.Request) {
	hostname, _ := os.Hostname()
	info := map[string]interface{}{
		"hostname":    hostname,
		"platform":    runtime.GOOS + "/" + runtime.GOARCH,
		"server_time": time.Now().Format("2006-01-02 15:04:05"),
		"ip":          getLocalIP(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleGetGit(w http.ResponseWriter, r *http.Request) {
	resp := map[string]interface{}{
		"user_name":  runCmd("git", "config", "--global", "user.name"),
		"user_email": runCmd("git", "config", "--global", "user.email"),
		"gh_auth":    checkGhAuth(),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleSetGitConfig(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Name != "" {
		exec.Command("git", "config", "--global", "user.name", req.Name).Run()
	}
	if req.Email != "" {
		exec.Command("git", "config", "--global", "user.email", req.Email).Run()
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetSessions(w http.ResponseWriter, r *http.Request) {
	sessions := s.state.GetSessions()
	activeID := s.state.GetActiveSessionID()
	resp := struct {
		Sessions []*engine.Session `json:"sessions"`
		Active   string            `json:"active"`
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

// ─── helpers ────────────────────────────────────────────

func getLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "unknown"
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
			return ipnet.IP.String()
		}
	}
	return "unknown"
}

func runCmd(name string, args ...string) string {
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func checkGhAuth() string {
	out, err := exec.Command("gh", "auth", "status").CombinedOutput()
	if err != nil {
		return "not logged in"
	}
	o := string(out)
	if strings.Contains(o, "Logged in") {
		return "logged in"
	}
	return "not logged in"
}
