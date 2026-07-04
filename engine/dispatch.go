package engine

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/whos-carmen/agent-hub/models"
	"crypto/rand"
	"encoding/hex"
)

// Dispatch executes a dispatch plan and broadcasts results.
func Dispatch(plan models.DispatchPlan, state *ConversationState, modelConfigs map[string]models.ModelConfig) {
	dispatchID := generateID()

	dispatch := models.Dispatch{
		ID:        dispatchID,
		Plan:      plan,
		Status:    "running",
		CreatedAt: time.Now(),
	}
	state.AddDispatch(dispatch)

	state.BroadcastEvent(models.StreamEvent{
		Type: "dispatch",
		Text: fmt.Sprintf("Dispatch: %s → %s", plan.Kind, strings.Join(plan.Models, ", ")),
	})

	switch plan.Kind {
	case "single":
		dispatchSingle(plan, dispatchID, state, modelConfigs)
	case "parallel":
		dispatchParallel(plan, dispatchID, state, modelConfigs)
	case "discussion":
		dispatchDiscussion(plan, dispatchID, state, modelConfigs)
	}

	state.UpdateDispatchStatus(dispatchID, "complete")
}

func dispatchSingle(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig) {
	if len(plan.Models) == 0 {
		return
	}
	modelName := plan.Models[0]
	cfg, ok := modelConfigs[modelName]
	if !ok {
		errMsg := fmt.Sprintf("model %q not configured", modelName)
		state.AddMessage(newErrorMessage(errMsg, modelName, dispatchID))
		return
	}

	chunkChan := make(chan models.StreamEvent, 64)
	msgID := generateID()

	// No placeholder message — stream starts via broadcast, final message added on "done"

	go models.StreamResponse(cfg, "", plan.Prompt, "", chunkChan)

	for evt := range chunkChan {
		switch evt.Type {
		case "chunk":
			state.BroadcastEvent(models.StreamEvent{
				Type:  "chunk",
				Chunk: evt.Chunk,
				Message: models.Message{
					ModelName: modelName,
					ID:        msgID,
				},
			})
		case "done":
			state.AddMessage(models.Message{
				ID:         generateID(),
				Role:       models.RoleModel,
				ModelName:  modelName,
				Content:    evt.Text,
				Status:     models.StatusComplete,
				DispatchID: dispatchID,
			})
			state.BroadcastEvent(models.StreamEvent{
				Type: "done",
				Message: models.Message{
					ModelName: modelName,
					Content:   evt.Text,
					Status:    models.StatusComplete,
				},
			})
		case "error":
			state.AddMessage(newErrorMessage(evt.Error, modelName, dispatchID))
			state.BroadcastEvent(models.StreamEvent{
				Type:  "error",
				Error: evt.Error,
				Message: models.Message{
					ModelName: modelName,
				},
			})
		}
	}
}

func dispatchParallel(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig) {
	var wg sync.WaitGroup
	responses := make([]models.Message, 0, len(plan.Models))
	var mu sync.Mutex

	for _, modelName := range plan.Models {
		cfg, ok := modelConfigs[modelName]
		if !ok {
			errMsg := fmt.Sprintf("model %q not configured", modelName)
			state.AddMessage(newErrorMessage(errMsg, modelName, dispatchID))
			continue
		}

		wg.Add(1)
		go func(name string, cfg models.ModelConfig) {
			defer wg.Done()

			chunkChan := make(chan models.StreamEvent, 64)
			msgID := generateID()

			go models.StreamResponse(cfg, "", plan.Prompt, "", chunkChan)

			var fullText strings.Builder
			for evt := range chunkChan {
				switch evt.Type {
				case "chunk":
					fullText.WriteString(evt.Chunk)
					state.BroadcastEvent(models.StreamEvent{
						Type:  "chunk",
						Chunk: evt.Chunk,
						Message: models.Message{
							ModelName: name,
							ID:        msgID,
						},
					})
				case "done":
					msg := models.Message{
						ID:         generateID(),
						Role:       models.RoleModel,
						ModelName:  name,
						Content:    evt.Text,
						Status:     models.StatusComplete,
						DispatchID: dispatchID,
					}
					state.AddMessage(msg)
					state.BroadcastEvent(models.StreamEvent{
						Type: "done",
						Message: models.Message{
							ModelName: name,
							Content:   evt.Text,
							Status:    models.StatusComplete,
						},
					})
					mu.Lock()
					responses = append(responses, msg)
					mu.Unlock()
				case "error":
					state.AddMessage(newErrorMessage(evt.Error, name, dispatchID))
					state.BroadcastEvent(models.StreamEvent{
						Type:  "error",
						Error: evt.Error,
						Message: models.Message{
							ModelName: name,
						},
					})
				}
			}
		}(modelName, cfg)
	}

	wg.Wait()

	// Aggregate if needed — feed results back to the primary model
	if plan.Aggregate && len(responses) > 0 {
		if primaryCfg, ok := modelConfigs[plan.Models[0]]; ok {
			AggregateAndSynthesize(responses, primaryCfg, dispatchID, state)
		}
	}
}

func dispatchDiscussion(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig) {
	if len(plan.Models) < 2 {
		dispatchSingle(plan, dispatchID, state, modelConfigs)
		return
	}

	modelsList := plan.Models
	rounds := plan.Rounds
	if rounds < 1 {
		rounds = 3
	}

	currentPrompt := plan.Prompt

	for round := 0; round < rounds; round++ {
		for _, modelName := range modelsList {
			cfg, ok := modelConfigs[modelName]
			if !ok {
				continue
			}

			chunkChan := make(chan models.StreamEvent, 64)
			msgID := generateID()

			go models.StreamResponse(cfg, "You are participating in a multi-model discussion. Respond to the current topic concisely.", currentPrompt, "", chunkChan)

			var reply strings.Builder
			for evt := range chunkChan {
				switch evt.Type {
				case "chunk":
					reply.WriteString(evt.Chunk)
					state.BroadcastEvent(models.StreamEvent{
						Type:  "chunk",
						Chunk: evt.Chunk,
						Message: models.Message{
							ModelName: modelName,
							ID:        msgID,
						},
					})
				case "done":
					replyText := evt.Text
					if replyText == "" {
						replyText = reply.String()
					}
					state.AddMessage(models.Message{
						ID:         generateID(),
						Role:       models.RoleModel,
						ModelName:  modelName,
						Content:    replyText,
						Status:     models.StatusComplete,
						DispatchID: dispatchID,
						ParentID:   msgID,
					})
					// Feed this response as context for the next model
					currentPrompt = fmt.Sprintf("Previous response from %s:\n%s\n\nContinue the discussion.", modelName, replyText)
				case "error":
					state.AddMessage(newErrorMessage(evt.Error, modelName, dispatchID))
				}
			}
		}
	}
}

func generateID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newErrorMessage(err, modelName, dispatchID string) models.Message {
	return models.Message{
		ID:         generateID(),
		Role:       models.RoleModel,
		ModelName:  modelName,
		Content:    fmt.Sprintf("Error: %s", err),
		Status:     models.StatusError,
		DispatchID: dispatchID,
	}
}
