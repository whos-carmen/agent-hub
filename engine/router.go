package engine

import (
	"strings"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/models"
)

// Route decides how to handle user input based on routing rules.
func Route(input string, cfg config.Config) models.DispatchPlan {
	inputLower := strings.ToLower(input)
	plan := models.DispatchPlan{
		Prompt: input,
	}

	// Check for manual overrides
	if strings.Contains(inputLower, "@single") {
		plan.Kind = "single"
		plan.Models = []string{cfg.DefaultModel}
		plan.Prompt = strings.TrimSpace(strings.ReplaceAll(input, "@single", ""))
		return plan
	}

	if strings.Contains(inputLower, "@parallel") {
		plan.Kind = "parallel"
		plan.Models = cfg.Routing.ParallelModels
		plan.Aggregate = true
		plan.Prompt = strings.TrimSpace(strings.ReplaceAll(input, "@parallel", ""))
		return plan
	}

	// Check discuss keywords
	for _, kw := range cfg.Routing.DiscussOn {
		if strings.Contains(inputLower, kw) {
			plan.Kind = "discussion"
			plan.Models = cfg.Routing.DiscussModels
			plan.Rounds = cfg.Routing.DiscussRounds
			return plan
		}
	}

	// Check parallel keywords
	for _, kw := range cfg.Routing.ParallelOn {
		if strings.Contains(inputLower, kw) {
			plan.Kind = "parallel"
			plan.Models = cfg.Routing.ParallelModels
			plan.Aggregate = true
			return plan
		}
	}

	// Default: single model
	plan.Kind = "single"
	plan.Models = []string{cfg.DefaultModel}
	return plan
}
