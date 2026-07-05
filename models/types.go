package models

import "time"

// Role represents who sent a message.
type Role string

const (
	RoleUser    Role = "user"
	RoleModel   Role = "model"
	RoleSystem  Role = "system"
)

// MessageStatus tracks the lifecycle of a model response.
type MessageStatus string

const (
	StatusPending   MessageStatus = "pending"
	StatusStreaming MessageStatus = "streaming"
	StatusComplete  MessageStatus = "complete"
	StatusError     MessageStatus = "error"
)

// Message is a single entry in the conversation log.
type Message struct {
	ID        string        `json:"id"`
	Role      Role          `json:"role"`
	Content   string        `json:"content"`
	ModelName string        `json:"model_name,omitempty"`
	Status    MessageStatus `json:"status"`
	CreatedAt time.Time     `json:"created_at"`
	ParentID  string        `json:"parent_id,omitempty"`
	DispatchID string       `json:"dispatch_id,omitempty"`
	TokensIn  int           `json:"tokens_in,omitempty"`
	TokensOut int           `json:"tokens_out,omitempty"`
}

// StreamEvent is pushed to UI subscribers as model responses arrive.
type StreamEvent struct {
	Type      string     `json:"type"`
	Message   Message    `json:"message,omitempty"`
	Chunk     string     `json:"chunk,omitempty"`
	Text      string     `json:"text,omitempty"`
	Error     string     `json:"error,omitempty"`
	ToolCalls []ToolCall `json:"tool_calls,omitempty"`
	TokensIn  int        `json:"tokens_in,omitempty"`
	TokensOut int        `json:"tokens_out,omitempty"`
	CachedTokens int    `json:"cached_tokens,omitempty"`
}

// DispatchPlan describes how the router decided to handle user input.
type DispatchPlan struct {
	Kind       string   `json:"kind"`       // "single" | "parallel" | "discussion"
	Models     []string `json:"models"`
	Prompt     string   `json:"prompt"`
	Aggregate  bool     `json:"aggregate,omitempty"`   // parallel: feed results back to primary?
	Rounds     int      `json:"rounds,omitempty"`      // discussion: number of rounds
	FilePaths  []string `json:"file_paths,omitempty"`  // files to read for context
}

// Dispatch tracks an in-flight dispatch for the UI.
type Dispatch struct {
	ID        string      `json:"id"`
	Plan      DispatchPlan `json:"plan"`
	Status    string       `json:"status"` // "running" | "complete" | "error"
	Responses []Message    `json:"responses,omitempty"`
	CreatedAt time.Time    `json:"created_at"`
}

// ModelConfig is the runtime representation of a configured model.
type ModelConfig struct {
	Name       string
	APIKey     string
	BaseURL    string
	Type       string // "deepseek" | "openai"
	ModelName  string
	EnvVarName string
}

// ─── Tool calling types ──────────────────────────

// Tool represents an OpenAI-compatible tool definition.
type Tool struct {
	Type     string       `json:"type"` // "function"
	Function ToolFunction `json:"function"`
}

// ToolFunction is the function descriptor inside a Tool.
type ToolFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

// ToolCall represents a model's request to call a tool.
type ToolCall struct {
	ID       string              `json:"id"`
	Type     string              `json:"type"` // "function"
	Function ToolCallFunction    `json:"function"`
}

// ToolCallFunction holds the name and arguments of a tool call.
type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON-encoded args
}

// ToolCallID is the tool_call_id field on a tool-role message.
type ToolCallID struct {
	ToolCallID string `json:"tool_call_id"`
}
