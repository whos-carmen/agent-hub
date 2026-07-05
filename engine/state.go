package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/whos-carmen/agent-hub/models"
)

// Session represents a single chat conversation.
type Session struct {
	ID        string           `json:"id"`
	Name      string           `json:"name"`
	Model     string           `json:"model,omitempty"`     // per-session model override
	CreatedAt time.Time        `json:"created_at"`
	Messages  []models.Message `json:"messages"`
}

// GetModel returns the session's model override, or the defaultModel if unset.
func (s *Session) GetModel(defaultModel string) string {
	if s.Model != "" {
		return s.Model
	}
	return defaultModel
}

// ConversationState is the single source of truth for the hub.
type ConversationState struct {
	mu              sync.RWMutex
	Sessions        []*Session
	ActiveSessionID string
	Dispatches      []models.Dispatch

	// Broadcast sends events to all UI subscribers (SSE, TUI).
	Broadcast chan models.StreamEvent

	// Input is the channel for receiving new user messages.
	Input chan string

	// persistPath is the filesystem path for session persistence.
	persistPath string
}

// NewState creates a new conversation state with a default session.
func NewState() *ConversationState {
	return NewPersistentState("")
}

// NewPersistentState creates state that persists to file. If path is empty, no persistence.
func NewPersistentState(path string) *ConversationState {
	s := &ConversationState{
		Broadcast:   make(chan models.StreamEvent, 256),
		Input:       make(chan string, 32),
		persistPath: path,
	}

	// Try to load from disk
	if path != "" {
		if loaded := loadState(path); loaded != nil {
			s.Sessions = loaded.Sessions
			s.ActiveSessionID = loaded.ActiveSessionID
			return s
		}
	}

	// No saved state — create a default session
	defaultSession := &Session{
		ID:   generateID(),
		Name: "Chat 1",
	}
	s.Sessions = append(s.Sessions, defaultSession)
	s.ActiveSessionID = defaultSession.ID
	s.maybeSave()
	return s
}

// GetActiveSessionID returns the active session ID.
func (s *ConversationState) GetActiveSessionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ActiveSessionID
}

func (s *ConversationState) getActiveSessionLocked() *Session {
	for _, sess := range s.Sessions {
		if sess.ID == s.ActiveSessionID {
			return sess
		}
	}
	if len(s.Sessions) > 0 {
		return s.Sessions[0]
	}
	return nil
}

// AddMessage appends a message to the active session.
func (s *ConversationState) AddMessage(msg models.Message) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.CreatedAt = time.Now()
	session := s.getActiveSessionLocked()
	if session == nil {
		return -1
	}
	idx := len(session.Messages)
	session.Messages = append(session.Messages, msg)
	s.maybeSaveLocked()
	return idx
}

// GetMessages returns messages for a session (active by default).
func (s *ConversationState) GetMessages(sessionID ...string) []models.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id := s.ActiveSessionID
	if len(sessionID) > 0 && sessionID[0] != "" {
		id = sessionID[0]
	}
	for _, sess := range s.Sessions {
		if sess.ID == id {
			out := make([]models.Message, len(sess.Messages))
			copy(out, sess.Messages)
			return out
		}
	}
	return nil
}

// CreateSession creates a new session and returns it.
func (s *ConversationState) CreateSession(name string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess := &Session{
		ID:   generateID(),
		Name: name,
	}
	s.Sessions = append(s.Sessions, sess)
	s.ActiveSessionID = sess.ID
	s.maybeSaveLocked()
	return sess
}

// SwitchSession switches the active session.
func (s *ConversationState) SwitchSession(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.Sessions {
		if sess.ID == id {
			s.ActiveSessionID = id
			s.maybeSaveLocked()
			return true
		}
	}
	return false
}

// DeleteSession removes a session by ID.
func (s *ConversationState) DeleteSession(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, sess := range s.Sessions {
		if sess.ID == id {
			s.Sessions = append(s.Sessions[:i], s.Sessions[i+1:]...)
			if s.ActiveSessionID == id && len(s.Sessions) > 0 {
				s.ActiveSessionID = s.Sessions[0].ID
			}
			s.maybeSaveLocked()
			return true
		}
	}
	return false
}

// GetSessions returns a copy of all sessions (without messages for listing).
func (s *ConversationState) GetSessions() []*Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Session, len(s.Sessions))
	for i, sess := range s.Sessions {
		out[i] = &Session{
			ID:        sess.ID,
			Name:      sess.Name,
			Model:     sess.Model,
			CreatedAt: sess.CreatedAt,
			Messages:  nil, // don't include messages in listing
		}
	}
	return out
}

// RenameSession renames a session.
func (s *ConversationState) RenameSession(id, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.Sessions {
		if sess.ID == id {
			sess.Name = name
			s.maybeSaveLocked()
			return true
		}
	}
	return false
}

// SetSessionModel sets the model override for a session.
func (s *ConversationState) SetSessionModel(id, model string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sess := range s.Sessions {
		if sess.ID == id {
			sess.Model = model
			s.maybeSaveLocked()
			return true
		}
	}
	return false
}

// GetActiveSessionModel returns the active session's model, or defaultModel.
func (s *ConversationState) GetActiveSessionModel(defaultModel string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if sess := s.getActiveSessionLocked(); sess != nil {
		return sess.GetModel(defaultModel)
	}
	return defaultModel
}

// AddDispatch registers a new dispatch.
func (s *ConversationState) AddDispatch(d models.Dispatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Dispatches = append(s.Dispatches, d)
}

// UpdateDispatchStatus updates the status of a dispatch.
func (s *ConversationState) UpdateDispatchStatus(id, status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.Dispatches {
		if s.Dispatches[i].ID == id {
			s.Dispatches[i].Status = status
			return
		}
	}
}

// BroadcastEvent sends an event to all UI subscribers. Non-blocking.
func (s *ConversationState) BroadcastEvent(evt models.StreamEvent) {
	select {
	case s.Broadcast <- evt:
	default:
	}
}

// ─── Persistence ────────────────────────────────────

type persistedState struct {
	ActiveSessionID string     `json:"active"`
	Sessions        []*Session `json:"sessions"`
}

// maybeSave writes state to disk if persistence is enabled. Must hold s.mu.
func (s *ConversationState) maybeSaveLocked() {
	if s.persistPath == "" {
		return
	}
	data, err := json.Marshal(persistedState{
		ActiveSessionID: s.ActiveSessionID,
		Sessions:        s.Sessions,
	})
	if err != nil {
		return
	}
	dir := filepath.Dir(s.persistPath)
	os.MkdirAll(dir, 0755)
	os.WriteFile(s.persistPath, data, 0644)
}

// maybeSave is the public, lock-acquiring variant.
func (s *ConversationState) maybeSave() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maybeSaveLocked()
}

func loadState(path string) *persistedState {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var ps persistedState
	if err := json.Unmarshal(data, &ps); err != nil {
		return nil
	}
	return &ps
}
