package engine

import (
	"sync"
	"time"

	"github.com/whos-carmen/agent-hub/models"
)

// ConversationState is the single source of truth for the hub.
type ConversationState struct {
	mu        sync.RWMutex
	Messages  []models.Message
	Dispatches []models.Dispatch

	// Broadcast sends events to all UI subscribers (SSE, TUI).
	Broadcast chan models.StreamEvent

	// Input is the channel for receiving new user messages.
	Input chan string
}

// NewState creates a new conversation state.
func NewState() *ConversationState {
	return &ConversationState{
		Broadcast: make(chan models.StreamEvent, 256),
		Input:     make(chan string, 32),
	}
}

// AddMessage appends a message and returns its index.
func (s *ConversationState) AddMessage(msg models.Message) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg.CreatedAt = time.Now()
	idx := len(s.Messages)
	s.Messages = append(s.Messages, msg)
	return idx
}

// GetMessages returns a copy of all messages.
func (s *ConversationState) GetMessages() []models.Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]models.Message, len(s.Messages))
	copy(out, s.Messages)
	return out
}

// AddDispatch registers a new dispatch and returns its ID.
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
		// drop if no consumers
	}
}
