package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/whos-carmen/agent-hub/models"
	"crypto/rand"
	"encoding/hex"
)

// Dispatch executes a dispatch plan and broadcasts results.
func Dispatch(plan models.DispatchPlan, state *ConversationState, modelConfigs map[string]models.ModelConfig, defaultModel string) {
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
		dispatchSingle(plan, dispatchID, state, modelConfigs, defaultModel)
	case "parallel":
		dispatchParallel(plan, dispatchID, state, modelConfigs, defaultModel)
	case "discussion":
		dispatchDiscussion(plan, dispatchID, state, modelConfigs, defaultModel)
	}

	state.UpdateDispatchStatus(dispatchID, "complete")
}

func dispatchSingle(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig, defaultModel string) {
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

	sysPrompt := hubSystemPrompt(modelName, modelConfigs, defaultModel)

	// No placeholder message — stream starts via broadcast, final message added on "done"

	go models.StreamResponse(context.Background(), cfg, sysPrompt, plan.Prompt, "", chunkChan)

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

func dispatchParallel(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig, defaultModel string) {
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

			go models.StreamResponse(context.Background(), cfg, hubSystemPrompt(name, modelConfigs, defaultModel), plan.Prompt, "", chunkChan)

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
			AggregateAndSynthesize(responses, primaryCfg, dispatchID, state, modelConfigs, defaultModel)
		}
	}
}

func dispatchDiscussion(plan models.DispatchPlan, dispatchID string, state *ConversationState, modelConfigs map[string]models.ModelConfig, defaultModel string) {
	if len(plan.Models) < 2 {
		dispatchSingle(plan, dispatchID, state, modelConfigs, defaultModel)
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

			go models.StreamResponse(context.Background(), cfg, hubSystemPrompt(modelName, modelConfigs, defaultModel)+" You are participating in a multi-model discussion. Respond concisely.", currentPrompt, "", chunkChan)

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

// hubSystemPrompt builds a system prompt that tells the model about the hub.
func hubSystemPrompt(modelName string, allModels map[string]models.ModelConfig, defaultModel string) string {
	var modelList []string
	for name := range allModels {
		if name != modelName {
			modelList = append(modelList, name)
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("You are %s, one of several AI models running in agent-hub — a multi-model conversation orchestrator.\n\n", modelName))
	sb.WriteString("Other models available in this hub:\n")
	for _, name := range modelList {
		sb.WriteString(fmt.Sprintf("  - %s\n", name))
	}
	if defaultModel != "" && defaultModel != modelName {
		sb.WriteString(fmt.Sprintf("\nThe default model (used for single-model queries) is %s.\n", defaultModel))
	}
	sb.WriteString("\nWhen responding, you can suggest delegating work to other models by prefixing with @parallel (for code review, security audits, testing) or @discuss (for debate, brainstorming). The hub will route accordingly.\n")
	sb.WriteString("\nTools marked 🔴 DANGEROUS require YOLO mode to be enabled by the user. Tools marked 🟢 SAFE always work. When YOLO is off and you need a dangerous tool, tell the user to enable YOLO first.\n")
	sb.WriteString("You are talking to a user through a web dashboard or CLI. Be concise and helpful.\n")
	return sb.String()
}
