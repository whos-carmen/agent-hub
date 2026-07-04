package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/whos-carmen/agent-hub/models"
)

// AggregateAndSynthesizes takes multiple model responses and asks the primary
// model to synthesize them into a coherent response.
func AggregateAndSynthesize(responses []models.Message, primaryCfg models.ModelConfig, dispatchID string, state *ConversationState) {
	var sb strings.Builder
	sb.WriteString("I received responses from multiple models on the same topic. Please synthesise them into a coherent response, highlighting consensus and disagreements:\n\n")

	for _, r := range responses {
		sb.WriteString(fmt.Sprintf("--- %s ---\n%s\n\n", r.ModelName, r.Content))
	}

	sb.WriteString("Please provide a consolidated summary.")

	prompt := sb.String()
	chunkChan := make(chan models.StreamEvent, 64)
	msgID := generateID()

	state.AddMessage(models.Message{
		ID:         msgID,
		Role:       models.RoleModel,
		ModelName:  primaryCfg.Name,
		Content:    "",
		Status:     models.StatusStreaming,
		DispatchID: dispatchID,
	})

	state.BroadcastEvent(models.StreamEvent{
		Type: "dispatch",
		Text: fmt.Sprintf("● %s: Synthesising parallel responses...", primaryCfg.Name),
	})

	go models.StreamResponse(context.Background(), primaryCfg, "You are a synthesiser. Combine the following model responses into a clear, concise summary.", prompt, "", chunkChan)

	var fullText strings.Builder
	for evt := range chunkChan {
		switch evt.Type {
		case "chunk":
			fullText.WriteString(evt.Chunk)
			state.BroadcastEvent(models.StreamEvent{
				Type:  "chunk",
				Chunk: evt.Chunk,
				Message: models.Message{
					ModelName: primaryCfg.Name,
					ID:        msgID,
				},
			})
		case "done":
			state.AddMessage(models.Message{
				ID:         generateID(),
				Role:       models.RoleModel,
				ModelName:  primaryCfg.Name,
				Content:    evt.Text,
				Status:     models.StatusComplete,
				DispatchID: dispatchID,
				ParentID:   msgID,
			})
			state.BroadcastEvent(models.StreamEvent{
				Type: "done",
				Message: models.Message{
					ModelName: primaryCfg.Name,
					Content:   evt.Text,
					Status:    models.StatusComplete,
				},
			})
		case "error":
			state.AddMessage(newErrorMessage(evt.Error, primaryCfg.Name, dispatchID))
		}
	}
}
