package models

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var httpClient = &http.Client{Timeout: 120 * time.Second}

// ChatMessage is the standard OpenAI/Dify chat message format.
type ChatMessage struct {
	Role       string     `json:"role"`
	Content    string     `json:"content,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
}

// ChatRequest is the standard request body.
type ChatRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Stream      bool          `json:"stream"`
	Tools       []Tool        `json:"tools,omitempty"`
	ToolChoice  string        `json:"tool_choice,omitempty"`
	Temperature float64       `json:"temperature,omitempty"`
}

// ChatChunk is a single SSE line from a streaming response.
type ChatChunk struct {
	Choices []struct {
		Delta struct {
			Role             string           `json:"role,omitempty"`
			Content          string           `json:"content,omitempty"`
			ReasoningContent string           `json:"reasoning_content,omitempty"`
			ToolCalls        []toolCallDelta  `json:"tool_calls,omitempty"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Usage *struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
		TotalTokens      int `json:"total_tokens"`
	} `json:"usage,omitempty"`
}

type toolCallDelta struct {
	Index    int    `json:"index"`
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Function struct {
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
	} `json:"function,omitempty"`
}

// StreamResponse calls the model API with streaming and returns chunks via a channel.
func StreamResponse(ctx context.Context, cfg ModelConfig, systemPrompt string, userPrompt string, fileContext string, chunkChan chan<- StreamEvent) {
	defer close(chunkChan)

	messages := buildMessages(systemPrompt, userPrompt, fileContext)

	reqBody := ChatRequest{
		Model:    cfg.ModelName,
		Messages: messages,
		Stream:   true,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("marshal request: %v", err)}
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.BaseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("create request: %v", err)}
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	// DeepSeek-specific headers
	if cfg.Type == "deepseek" {
		req.Header.Set("Accept", "text/event-stream")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("http request: %v", err)}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("API error %d: %s", resp.StatusCode, string(body))}
		return
	}

	// Increase scanner buffer from 64KB to 1MB for large SSE lines
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var fullText strings.Builder
	sentDone := false

	for scanner.Scan() {
		line := scanner.Text()

		// SSE format: "data: {...}"
		if !strings.HasPrefix(line, "data: ") {
			continue
		}

		data := strings.TrimPrefix(line, "data: ")

		// [DONE] signal from DeepSeek
		if data == "[DONE]" {
			continue
		}

		var chunk ChatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			// Ignore malformed lines — some providers send non-JSON status lines
			continue
		}

		if len(chunk.Choices) > 0 {
			d := chunk.Choices[0].Delta
			if d.ReasoningContent != "" {
				chunkChan <- StreamEvent{Type: "reasoning", Chunk: d.ReasoningContent}
			}
			delta := d.Content
			if delta != "" {
				fullText.WriteString(delta)
				chunkChan <- StreamEvent{Type: "chunk", Chunk: delta}
			}

			if chunk.Choices[0].FinishReason != nil && !sentDone {
				sentDone = true
				chunkChan <- StreamEvent{
					Type: "done",
					Text: fullText.String(),
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("stream read error: %v", err)}
		return
	}

	// Send done only if we never sent it during the stream
	if !sentDone {
		fullTextStr := fullText.String()
		if fullTextStr != "" {
			chunkChan <- StreamEvent{Type: "done", Text: fullTextStr}
		}
	}
}

// CallModelNonStreaming sends a non-streaming request and returns the full text.
func CallModelNonStreaming(ctx context.Context, cfg ModelConfig, systemPrompt string, userPrompt string, fileContext string) (string, error) {
	messages := buildMessages(systemPrompt, userPrompt, fileContext)

	reqBody := ChatRequest{
		Model:    cfg.ModelName,
		Messages: messages,
		Stream:   false,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.BaseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)

	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	if resp.StatusCode != 200 {
		return "", fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("parse response: %w (body: %s)", err, string(body))
	}

	if len(result.Choices) == 0 {
		return "", fmt.Errorf("no choices in response")
	}

	return result.Choices[0].Message.Content, nil
}

func buildMessages(systemPrompt, userPrompt, fileContext string) []ChatMessage {
	var msgs []ChatMessage

	if systemPrompt != "" {
		msgs = append(msgs, ChatMessage{Role: "system", Content: systemPrompt})
	}

	var fullPrompt strings.Builder
	fullPrompt.WriteString(userPrompt)

	if fileContext != "" {
		fullPrompt.WriteString("\n\n---\nFile context:\n")
		fullPrompt.WriteString(fileContext)
	}

	msgs = append(msgs, ChatMessage{Role: "user", Content: fullPrompt.String()})
	return msgs
}

// StreamChatWithTools sends a conversation (messages) to the model with tool definitions,
// and returns both text chunks and tool calls through the channel.
func StreamChatWithTools(ctx context.Context, cfg ModelConfig, messages []ChatMessage, tools []Tool, chunkChan chan<- StreamEvent) {
	defer close(chunkChan)

	reqBody := ChatRequest{
		Model:      cfg.ModelName,
		Messages:   messages,
		Stream:     true,
		Tools:      tools,
		ToolChoice: "auto",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("marshal request: %v", err)}
		return
	}

	req, err := http.NewRequestWithContext(ctx, "POST", cfg.BaseURL, bytes.NewReader(bodyBytes))
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("create request: %v", err)}
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+cfg.APIKey)
	if cfg.Type == "deepseek" {
		req.Header.Set("Accept", "text/event-stream")
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("http request: %v", err)}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("API error %d: %s", resp.StatusCode, string(body))}
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	var fullText strings.Builder
	sentDone := false

	type tcbuilder struct{ id, name string; args strings.Builder }
	tcMap := make(map[int]*tcbuilder)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") { continue }
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" { continue }

		var chunk ChatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil { continue }

		if len(chunk.Choices) > 0 {
			delta := chunk.Choices[0].Delta

			if delta.ReasoningContent != "" {
				chunkChan <- StreamEvent{Type: "reasoning", Chunk: delta.ReasoningContent}
			}
			if delta.Content != "" {
				fullText.WriteString(delta.Content)
				chunkChan <- StreamEvent{Type: "chunk", Chunk: delta.Content}
			}

			for _, tcd := range delta.ToolCalls {
				b := tcMap[tcd.Index]
				if b == nil { b = &tcbuilder{}; tcMap[tcd.Index] = b }
				if tcd.ID != "" { b.id = tcd.ID }
				if tcd.Function.Name != "" { b.name = tcd.Function.Name }
				b.args.WriteString(tcd.Function.Arguments)
			}

			finish := chunk.Choices[0].FinishReason
			if finish != nil && !sentDone {
				sentDone = true
				if *finish == "tool_calls" {
					var calls []ToolCall
					for i := 0; ; i++ {
						b, ok := tcMap[i]
						if !ok { break }
						calls = append(calls, ToolCall{
							ID:   b.id, Type: "function",
							Function: ToolCallFunction{Name: b.name, Arguments: b.args.String()},
						})
					}
					chunkChan <- StreamEvent{Type: "tool_calls", Text: fullText.String(), ToolCalls: calls}
				} else {
					chunkChan <- StreamEvent{Type: "done", Text: fullText.String()}
				}
			}
		}
	}

	if err := scanner.Err(); err != nil {
		chunkChan <- StreamEvent{Type: "error", Error: fmt.Sprintf("stream read error: %v", err)}
		return
	}

	if !sentDone {
		fullTextStr := fullText.String()
		if fullTextStr != "" {
			chunkChan <- StreamEvent{Type: "done", Text: fullTextStr}
		}
	}
}
