package models

// Model adapters are thin wrappers — the core streaming client is in client.go.
// These files exist for provider-specific adjustments (headers, response format).
// Currently all providers use the OpenAI-compatible streaming format, so they
// share the same client. Provider-specific overrides go here if needed.

import "fmt"

// NewModelConfig creates a ModelConfig from a provider definition.
func NewModelConfig(name, apiKey, baseURL, providerType, modelName string) ModelConfig {
	if modelName == "" {
		modelName = name
	}
	return ModelConfig{
		Name:      name,
		APIKey:    apiKey,
		BaseURL:   baseURL,
		Type:      providerType,
		ModelName: modelName,
	}
}

// Validate checks that the config has the minimum required fields.
func (m ModelConfig) Validate() error {
	if m.APIKey == "" {
		return fmt.Errorf("model %q: API key is empty (check %s env var)", m.Name, m.APIKey)
	}
	if m.BaseURL == "" {
		return fmt.Errorf("model %q: base URL is empty", m.Name)
	}
	if m.Type == "" {
		return fmt.Errorf("model %q: type is empty (must be 'deepseek' or 'openai')", m.Name)
	}
	return nil
}
