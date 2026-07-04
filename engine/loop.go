package engine

import (
	"log/slog"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/models"
)

// RunLoop starts the conversation loop. It reads user input from state.Input,
// routes it, dispatches it, and broadcasts results. Runs until stop is closed.
func RunLoop(state *ConversationState, cfg config.Config) {
	modelConfigs := buildModelConfigs(cfg)

	slog.Info("conversation loop started", "default_model", cfg.DefaultModel)

	for input := range state.Input {
		if input == "" {
			continue
		}

		// Add user message to log
		state.AddMessage(models.Message{
			ID:      generateID(),
			Role:    models.RoleUser,
			Content: input,
			Status:  models.StatusComplete,
		})

		state.BroadcastEvent(models.StreamEvent{
			Type: "status",
			Text: "Routing...",
		})

		// Route
		plan := Route(input, cfg)

		slog.Info("routed", "kind", plan.Kind, "models", plan.Models)

		state.BroadcastEvent(models.StreamEvent{
			Type: "status",
			Text: "Dispatching...",
		})

		// Dispatch
		Dispatch(plan, state, modelConfigs)
	}
}

func buildModelConfigs(cfg config.Config) map[string]models.ModelConfig {
	mc := make(map[string]models.ModelConfig)
	for name, mdef := range cfg.Models {
		apiKey := mdef.ResolveAPIKey()
		if apiKey == "" {
			slog.Warn("model %q has no API key (set %s), skipping", name, mdef.APIKeyEnv)
			continue
		}
		mc[name] = models.NewModelConfig(name, apiKey, mdef.BaseURL, mdef.Type, mdef.ModelName, mdef.APIKeyEnv)
	}
	return mc
}
