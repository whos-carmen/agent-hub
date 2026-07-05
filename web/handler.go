package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	_ "embed"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/engine"
	"github.com/whos-carmen/agent-hub/models"
	"github.com/whos-carmen/agent-hub/repo"
)

//go:embed static/index.html
var _indexHTML string

var httpClient = &http.Client{Timeout: 30 * time.Second}

type Server struct {
	state   *engine.ConversationState
	subs    *SSESubscriber
	port    int
	cfg     config.Config
	server  *http.Server
}

func NewServer(state *engine.ConversationState, cfg config.Config) *Server {
	return &Server{
		state: state,
		subs:  NewSSESubscriber(),
		port:  cfg.Port,
		cfg:   cfg,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /", s.handleDashboard)
	mux.HandleFunc("GET /api/messages", s.handleGetMessages)
	mux.HandleFunc("POST /api/send", s.handleSend)
	mux.HandleFunc("GET /api/models", s.handleGetModels)
	mux.HandleFunc("GET /api/files", s.handleGetFiles)
	mux.HandleFunc("POST /api/upload", s.handleUpload)
	mux.HandleFunc("GET /api/stream", s.handleSSE)

	mux.HandleFunc("GET /api/sessions", s.handleGetSessions)
	mux.HandleFunc("POST /api/sessions", s.handleCreateSession)
	mux.HandleFunc("DELETE /api/sessions/{id}", s.handleDeleteSession)
	mux.HandleFunc("PUT /api/sessions/{id}", s.handleRenameSession)
	mux.HandleFunc("PUT /api/sessions/{id}/model", s.handleSetSessionModel)
	mux.HandleFunc("GET /api/sessions/{id}/messages", s.handleGetSessionMessages)

	mux.HandleFunc("GET /api/system", s.handleGetSystem)
	mux.HandleFunc("GET /api/balance", s.handleGetBalance)
	mux.HandleFunc("GET /api/yolo", s.handleGetYOLO)
	mux.HandleFunc("GET /api/stats", s.handleGetStats)
	mux.HandleFunc("POST /api/compact", s.handleCompact)
	mux.HandleFunc("PUT /api/yolo", s.handleSetYOLO)
	mux.HandleFunc("POST /api/approve", s.handleApprove)
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
	// Read from disk so edits take effect without rebuild
	data, err := os.ReadFile("web/static/index.html")
	if err != nil {
		w.Write([]byte(_indexHTML)) // fallback to embedded
		return
	}
	w.Write(data)
}

func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	sessionID := r.URL.Query().Get("session")
	msgs := s.state.GetMessages(sessionID)
	// Enrich with cost info
	type msgWithCost struct {
		models.Message
		Cost    float64 `json:"cost"`
		CostFmt string  `json:"cost_fmt,omitempty"`
	}
	var enriched []msgWithCost
	// Find session model for pricing lookup
	sessions := s.state.GetSessions()
	var sessModel string
	for _, sess := range sessions {
		if sess.ID == sessionID || (sessionID == "" && sess.ID == s.state.GetActiveSessionID()) {
			sessModel = sess.Model
			if sessModel == "" { sessModel = s.cfg.DefaultModel }
			break
		}
	}
	mdef, hasPricing := s.cfg.Models[sessModel]
	
	for _, m := range msgs {
		mc := msgWithCost{Message: m}
		if m.Role == models.RoleModel && m.TokensIn+m.TokensOut > 0 && hasPricing {
			mc.Cost = float64(m.TokensIn)/1_000_000*mdef.PriceIn + float64(m.TokensOut)/1_000_000*mdef.PriceOut
			if mc.Cost < 0.01 {
				mc.CostFmt = "<$0.01"
			} else {
				mc.CostFmt = fmt.Sprintf("$%.2f", mc.Cost)
			}
		}
		enriched = append(enriched, mc)
	}
	if enriched == nil { enriched = []msgWithCost{} }
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(enriched)
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
	var list []ModelInfo
	for name, mdef := range s.cfg.Models {
		status := "configured"
		if mdef.ResolveAPIKey() == "" {
			status = "missing key"
		}
		list = append(list, ModelInfo{Name: name, Status: status})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	if list == nil {
		list = []ModelInfo{}
	}
	resp := struct {
		Models       []ModelInfo `json:"models"`
		DefaultModel string      `json:"default_model"`
	}{Models: list, DefaultModel: s.cfg.DefaultModel}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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

func (s *Server) handleGetYOLO(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]bool{"yolo": s.state.GetYOLO()})
}

func (s *Server) handleSetYOLO(w http.ResponseWriter, r *http.Request) {
	var req struct {
		On bool `json:"on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	s.state.SetYOLO(req.On)
	s.subs.Send(models.StreamEvent{Type: "yolo_changed", Text: fmt.Sprintf("%t", req.On)})
	w.WriteHeader(http.StatusNoContent)
}


func (s *Server) handleApprove(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ID       string `json:"id"`
		Approved bool   `json:"approved"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if s.state.ResolveApproval(req.ID, req.Approved) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "approval not found", http.StatusNotFound)
}

func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	stats := s.state.GetActiveSessionStats()
	activeID := s.state.GetActiveSessionID()
	sessions := s.state.GetSessions()
	
	var activeModel string
	for _, sess := range sessions {
		if sess.ID == activeID {
			activeModel = sess.Model
			if activeModel == "" { activeModel = s.cfg.DefaultModel }
			break
		}
	}
	
	type fullStats struct {
		Turns      int     `json:"turns"`
		TokensIn   int     `json:"tokens_in"`
		TokensOut  int     `json:"tokens_out"`
		Cached     int     `json:"cached_tokens"`
		Cost       float64 `json:"cost"`
		CostFmt    string  `json:"cost_fmt"`
		AvgCost    float64 `json:"avg_cost"`
		AvgCostFmt string  `json:"avg_cost_fmt"`
		CtxPct     int     `json:"ctx_pct"`
		CtxFmt     string  `json:"ctx_fmt"`
		CachePct   int     `json:"cache_pct"`
		CacheFmt   string  `json:"cache_fmt"`
	}
	
	resp := fullStats{
		Turns:     stats.Turns,
		TokensIn:  stats.TokensIn,
		TokensOut: stats.TokensOut,
		Cached:    stats.CachedTokens,
	}
	
	if mdef, ok := s.cfg.Models[activeModel]; ok {
		costIn := float64(stats.TokensIn) / 1_000_000 * mdef.PriceIn
		costOut := float64(stats.TokensOut) / 1_000_000 * mdef.PriceOut
		resp.Cost = costIn + costOut
		if resp.Cost < 0.01 { resp.CostFmt = "<$0.01" } else { resp.CostFmt = fmt.Sprintf("$%.2f", resp.Cost) }
		if stats.Turns > 0 {
			resp.AvgCost = resp.Cost / float64(stats.Turns)
			if resp.AvgCost < 0.01 { resp.AvgCostFmt = "<$0.01" } else { resp.AvgCostFmt = fmt.Sprintf("$%.2f", resp.AvgCost) }
		}
		if mdef.Context > 0 {
			total := stats.TokensIn + stats.TokensOut
			resp.CtxPct = total * 100 / mdef.Context
			resp.CtxFmt = fmt.Sprintf("%d%%", resp.CtxPct)
		} else { resp.CtxFmt = "0%" }
	}
	
	if stats.TokensIn > 0 {
		resp.CachePct = stats.CachedTokens * 100 / stats.TokensIn
		resp.CacheFmt = fmt.Sprintf("%d%%", resp.CachePct)
	} else {
		resp.CacheFmt = "0%"
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	var req struct {
		KeepTurns int `json:"keep_turns"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	if req.KeepTurns <= 0 { req.KeepTurns = 10 }
	
	// Get messages to compact
	activeID := s.state.GetActiveSessionID()
	msgs := s.state.GetMessages(activeID)
	if len(msgs) == 0 {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"removed": 0})
		return
	}
	
	// Find cutoff: keep last keepTurns user messages
	userIdx := []int{}
	for i, m := range msgs {
		if m.Role == models.RoleUser { userIdx = append(userIdx, i) }
	}
	if len(userIdx) <= req.KeepTurns {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]int{"removed": 0})
		return
	}
	cutoff := userIdx[len(userIdx)-req.KeepTurns]
	oldMsgs := msgs[:cutoff]
	
	// Build summarization prompt
	var sb strings.Builder
	sb.WriteString("Summarize this conversation history concisely. Include key decisions, findings, and actions taken.\n\n")
	for _, m := range oldMsgs {
		role := m.Role
		if role == models.RoleModel { role = "assistant" }
		content := m.Content
		if len(content) > 500 { content = content[:500] + "..." }
		sb.WriteString(fmt.Sprintf("[%s]: %s\n", role, content))
	}
	
	// Call default model for summarization
	defaultModel := s.cfg.DefaultModel
	mdef, ok := s.cfg.Models[defaultModel]
	if !ok {
		// Fall back to truncation
		removed := s.state.CompactSession(req.KeepTurns)
		json.NewEncoder(w).Encode(map[string]int{"removed": removed})
		return
	}
	key := os.Getenv(mdef.APIKeyEnv)
	if key == "" {
		removed := s.state.CompactSession(req.KeepTurns)
		json.NewEncoder(w).Encode(map[string]int{"removed": removed})
		return
	}
	
	mc := models.NewModelConfig(defaultModel, key, mdef.BaseURL, mdef.Type, mdef.ModelName, mdef.APIKeyEnv)
	summary, err := models.CallModelNonStreaming(r.Context(), mc, "You summarize conversations. Be brief.", sb.String(), "")
	if err != nil {
		slog.Warn("compaction summary failed", "error", err)
		removed := s.state.CompactSession(req.KeepTurns)
		json.NewEncoder(w).Encode(map[string]int{"removed": removed})
		return
	}
	
	// Store summary as system message and keep recent messages
	s.state.CompactWithSummary(cutoff, summary)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"removed": len(oldMsgs), "summarized": 1})
}

func (s *Server) handleGetBalance(w http.ResponseWriter, r *http.Request) {
	type balanceResp struct {
		DeepSeek  string `json:"deepseek"`
	}
	resp := balanceResp{}
	// DeepSeek balance
	if key := os.Getenv("DEEPSEEK_API_KEY"); key != "" {
		req, _ := http.NewRequest("GET", "https://api.deepseek.com/user/balance", nil)
		req.Header.Set("Authorization", "Bearer "+key)
		res, err := httpClient.Do(req)
		if err == nil {
			defer res.Body.Close()
			body, _ := io.ReadAll(res.Body)
			var ds struct {
				BalanceInfos []struct {
					Currency        string `json:"currency"`
					TotalBalance    string `json:"total_balance"`
					GrantedBalance  string `json:"granted_balance"`
					ToppedUpBalance string `json:"topped_up_balance"`
				} `json:"balance_infos"`
			}
			if json.Unmarshal(body, &ds) == nil && len(ds.BalanceInfos) > 0 {
				b := ds.BalanceInfos[0]
				resp.DeepSeek = fmt.Sprintf("%s %s (granted: %s, topped: %s)", b.TotalBalance, b.Currency, b.GrantedBalance, b.ToppedUpBalance)
			} else {
				resp.DeepSeek = "could not parse balance"
			}
		} else {
			resp.DeepSeek = "API error"
		}
	} else {
		resp.DeepSeek = "no key configured"
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
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

func (s *Server) handleSetSessionModel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req struct {
		Model string `json:"model"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if s.state.SetSessionModel(id, req.Model) {
		s.subs.Send(models.StreamEvent{Type: "model_switched", Text: req.Model})
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Error(w, "session not found", http.StatusNotFound)
}

func (s *Server) handleGetFiles(w http.ResponseWriter, r *http.Request) {
	root := s.cfg.ProjectsDir
	if root == "" {
		root = "."
	}

	// Directories to skip entirely
	skipDirs := map[string]bool{
		".cache": true, ".config": true, ".local": true, ".npm": true,
		".crosstalk": true, ".reasonix": true, ".mimocode": true,
		".aws": true, ".ssh": true, ".git": true, "node_modules": true,
		"go": true, "snap": true,
	}

	var files []map[string]interface{}
	maxFiles := 500
	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.IsDir() {
			if skipDirs[info.Name()] && path != root {
				return filepath.SkipDir
			}
			if strings.HasPrefix(info.Name(), ".") && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(info.Name(), ".") {
			return nil
		}
		if len(files) >= maxFiles {
			return filepath.SkipAll
		}
		rel, _ := filepath.Rel(root, path)
		files = append(files, map[string]interface{}{
			"name": rel,
			"size": info.Size(),
			"dir":  filepath.Dir(rel),
		})
		return nil
	})

	if files == nil {
		files = []map[string]interface{}{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(files)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 50<<20) // 50MB limit

	if err := r.ParseMultipartForm(50 << 20); err != nil {
		http.Error(w, "too large or invalid form", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "no file in form field 'file'", http.StatusBadRequest)
		return
	}
	defer file.Close()

	root := s.cfg.ProjectsDir
	if root == "" {
		root = "."
	}
	destPath := filepath.Join(root, header.Filename)

	os.MkdirAll(filepath.Dir(destPath), 0755)
	dst, err := os.Create(destPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("create file: %v", err), http.StatusInternalServerError)
		return
	}
	defer dst.Close()

	written, err := io.Copy(dst, file)
	if err != nil {
		http.Error(w, fmt.Sprintf("write file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"ok":   true,
		"path": header.Filename,
		"size": written,
	})
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
