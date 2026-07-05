package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/models"
)

// RunLoop starts the conversation loop with tool support.
func RunLoop(state *ConversationState, cfg config.Config) {
	modelConfigs := buildModelConfigs(cfg)
	tools := NewToolRegistry()
	slog.Info("conversation loop started", "default_model", cfg.DefaultModel, "tools", len(tools.Names()))

	for input := range state.Input {
		if input == "" {
			continue
		}

		state.AddMessage(models.Message{
			ID:      generateID(),
			Role:    models.RoleUser,
			Content: input,
			Status:  models.StatusComplete,
		})

		state.BroadcastEvent(models.StreamEvent{Type: "status", Text: "Thinking..."})

		sessionModel := state.GetActiveSessionModel(cfg.DefaultModel)
		plan := Route(input, cfg, sessionModel)
		modelName := plan.Models[0]
		mc, ok := modelConfigs[modelName]
		if !ok {
			state.AddMessage(newErrorMessage("model not configured: "+modelName, modelName, ""))
			continue
		}

		slog.Info("routed", "model", modelName)

		msgs := buildConversation(state, mc, cfg, modelConfigs)

		// Agent loop: model → tool_calls → execute → feedback → loop
		maxTurns := 8
		for turn := 0; turn < maxTurns; turn++ {
			chunkChan := make(chan models.StreamEvent, 128)
			go models.StreamChatWithTools(context.Background(), mc, msgs, tools.OpenAIFormats(), chunkChan)

			var fullText strings.Builder
			var toolCalls []models.ToolCall
			closed := false

			for !closed {
				select {
				case evt, ok := <-chunkChan:
					if !ok {
						closed = true
						continue
					}
					switch evt.Type {
					case "chunk":
						fullText.WriteString(evt.Chunk)
						state.BroadcastEvent(models.StreamEvent{
							Type:  "chunk",
							Chunk: evt.Chunk,
							Message: models.Message{ModelName: modelName},
						})
					case "tool_calls":
						toolCalls = evt.ToolCalls
						closed = true // stop reading, we got tool calls
						// Drain remaining events
						go func() { for range chunkChan {} }()
					case "done":
						if fullText.Len() == 0 && evt.Text != "" {
							fullText.WriteString(evt.Text)
						}
						closed = true
					case "error":
						state.BroadcastEvent(evt)
						closed = true
					}
				}
			}

			if len(toolCalls) > 0 {
				slog.Info("executing tools", "count", len(toolCalls))
				yolo := state.GetYOLO()
				for _, tc := range toolCalls {
					dangerous := tools.IsDangerous(tc.Function.Name)
					var args map[string]any
					json.Unmarshal([]byte(tc.Function.Arguments), &args)

					if dangerous && !yolo {
						approvalID := "approve-" + generateID()
						ch := state.RequestApproval(approvalID, tc.Function.Name, args)
						state.BroadcastEvent(models.StreamEvent{
							Type: "approval_needed",
							Text: approvalID,
							Message: models.Message{
								ModelName: tc.Function.Name,
								Content:  tc.Function.Arguments,
								ID:       approvalID,
							},
						})
						slog.Info("waiting for approval", "tool", tc.Function.Name, "id", approvalID)
						approved := <-ch
						if !approved {
							state.BroadcastEvent(models.StreamEvent{Type: "status", Text: "✗ Denied: " + tc.Function.Name})
							msgs = append(msgs, models.ChatMessage{Role:"assistant", ToolCalls: []models.ToolCall{{ID:tc.ID, Type:"function", Function: models.ToolCallFunction{Name:tc.Function.Name, Arguments:tc.Function.Arguments}}}})
							msgs = append(msgs, models.ChatMessage{Role:"tool", ToolCallID: tc.ID, Content: "User denied this tool call."})
							continue
						}
					}
					state.BroadcastEvent(models.StreamEvent{
						Type: "status",
						Text: "Running: " + tc.Function.Name,
					})

					result, execErr := tools.Execute(tc.Function.Name, args)
					content := result
					if execErr != nil {
						content = "Error: " + execErr.Error()
					}

					msgs = append(msgs, models.ChatMessage{
						Role: "assistant",
						ToolCalls: []models.ToolCall{{
							ID: tc.ID, Type: "function",
							Function: models.ToolCallFunction{Name: tc.Function.Name, Arguments: tc.Function.Arguments},
						}},
					})
					msgs = append(msgs, models.ChatMessage{
						Role:       "tool",
						ToolCallID: tc.ID,
						Content:    content,
					})

					if len(content) > 200 {
						content = content[:200]
					}
					state.BroadcastEvent(models.StreamEvent{
						Type:    "status",
						Text:    "Result: " + tc.Function.Name,
						Message: models.Message{Content: content},
					})
				}

				if fullText.Len() > 0 {
					state.AddMessage(models.Message{
						ID: generateID(), Role: models.RoleModel,
						ModelName: modelName, Content: fullText.String(),
						Status: models.StatusComplete,
					})
				}

				fullText.Reset()
				toolCalls = nil
				state.BroadcastEvent(models.StreamEvent{Type: "status", Text: "Continuing..."})
				continue
			}

			// Text response
			if fullText.Len() > 0 {
				state.RecordUsage(0, len(fullText.String())/4) // rough token estimate
				state.AddMessage(models.Message{
					ID: generateID(), Role: models.RoleModel,
					ModelName: modelName, Content: fullText.String(),
					Status: models.StatusComplete,
				})
				state.BroadcastEvent(models.StreamEvent{
					Type: "done",
					Message: models.Message{
						ModelName: modelName, Content: fullText.String(),
						Status: models.StatusComplete,
					},
				})
			}
			break
		}
	}
}

func buildConversation(state *ConversationState, mc models.ModelConfig, cfg config.Config, modelConfigs map[string]models.ModelConfig) []models.ChatMessage {
	var msgs []models.ChatMessage

	sysPrompt := hubSystemPrompt(mc.Name, modelConfigs, cfg.DefaultModel)
	sysPrompt += "\nYou have access to tools. Use them to help the user. When you call a tool, no need to explain — just call it. The tool result will be fed back to you automatically."
	msgs = append(msgs, models.ChatMessage{Role: "system", Content: sysPrompt})

	sessionID := state.GetActiveSessionID()
	history := state.GetMessages(sessionID)
	for _, msg := range history {
		if msg.Role == models.RoleUser {
			msgs = append(msgs, models.ChatMessage{Role: "user", Content: msg.Content})
		} else if msg.Role == models.RoleModel {
			msgs = append(msgs, models.ChatMessage{Role: "assistant", Content: msg.Content})
		}
	}
	return msgs
}

func buildModelConfigs(cfg config.Config) map[string]models.ModelConfig {
	mc := make(map[string]models.ModelConfig)
	for name, mdef := range cfg.Models {
		apiKey := mdef.ResolveAPIKey()
		if apiKey == "" {
			slog.Warn("model %q has no API key", name)
			continue
		}
		mc[name] = models.NewModelConfig(name, apiKey, mdef.BaseURL, mdef.Type, mdef.ModelName, mdef.APIKeyEnv)
	}
	return mc
}
