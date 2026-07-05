package engine

import (
	"strings"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/models"
)

// Route decides how to handle user input based on routing rules.
// sessionModel overrides the default model when set.
func Route(input string, cfg config.Config, sessionModel string) models.DispatchPlan {
	inputLower := strings.ToLower(input)
	plan := models.DispatchPlan{
		Prompt: input,
	}

	// Determine base model: session override > config default
	baseModel := cfg.DefaultModel
	if sessionModel != "" {
		baseModel = sessionModel
	}

	// Check for manual overrides
	if strings.Contains(inputLower, "@single") {
		plan.Kind = "single"
		plan.Models = []string{baseModel}
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

	// Default: single model — use implement_model for code tasks, review_model for review tasks
	plan.Kind = "single"
	plan.Models = []string{baseModel}
	// If review keywords matched and we didn't dispatch parallel, suggest review model
	if cfg.Routing.ReviewModel != "" {
		for _, kw := range cfg.Routing.ParallelOn {
			if strings.Contains(inputLower, kw) {
				plan.Models = []string{cfg.Routing.ReviewModel}
				break
			}
		}
	}
	if cfg.Routing.ImplementModel != "" {
		for _, kw := range []string{"implement", "build", "write", "create", "add", "fix", "code"} {
			if strings.Contains(inputLower, kw) {
				plan.Models = []string{cfg.Routing.ImplementModel}
				break
			}
		}
	}
	return plan
}
