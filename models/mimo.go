package models

import "fmt"

// ListConfiguredModels returns a human-readable summary of configured models.
func ListConfiguredModels(models map[string]ModelConfig) string {
	var result string
	for name, cfg := range models {
		status := "✓"
		if cfg.APIKey == "" {
			status = "✗ no key"
		}
		result += fmt.Sprintf("  %s %-20s (%s)\n", status, name, cfg.Type)
	}
	return result
}
