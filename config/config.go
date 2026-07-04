package config

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

type ModelDef struct {
	APIKeyEnv string `toml:"api_key_env"`
	BaseURL   string `toml:"base_url"`
	Type      string `toml:"type"` // "deepseek" | "openai"
	ModelName string `toml:"model_name"` // override the model name sent in API calls
}

type Routing struct {
	ParallelOn     []string `toml:"parallel_on"`
	DiscussOn      []string `toml:"discuss_on"`
	DiscussRounds  int      `toml:"discuss_rounds"`
	ParallelModels []string `toml:"parallel_models"`
	DiscussModels  []string `toml:"discuss_models"`
}

type Config struct {
	DefaultModel string              `toml:"default_model"`
	Port         int                 `toml:"port"`
	Models       map[string]ModelDef `toml:"models"`
	Routing      Routing             `toml:"routing"`
}

func Default() Config {
	return Config{
		DefaultModel: "mimo-pro",
		Port:         8080,
		Models: map[string]ModelDef{
			"mimo-pro": {
				APIKeyEnv: "MIMO_API_KEY",
				BaseURL:   "https://api.mimocode.com/v1/chat/completions",
				Type:      "openai",
				ModelName: "mimo-pro",
			},
			"mimo": {
				APIKeyEnv: "MIMO_API_KEY",
				BaseURL:   "https://api.mimocode.com/v1/chat/completions",
				Type:      "openai",
				ModelName: "mimo",
			},
			"deepseek-pro": {
				APIKeyEnv: "DEEPSEEK_API_KEY",
				BaseURL:   "https://api.deepseek.com/v1/chat/completions",
				Type:      "deepseek",
				ModelName: "deepseek-chat",
			},
			"deepseek-flash": {
				APIKeyEnv: "DEEPSEEK_API_KEY",
				BaseURL:   "https://api.deepseek.com/v1/chat/completions",
				Type:      "deepseek",
				ModelName: "deepseek-v4-flash",
			},
		},
		Routing: Routing{
			ParallelOn:     []string{"review", "security", "refactor", "test", "audit"},
			DiscussOn:      []string{"discuss", "debate", "brainstorm", "compare"},
			DiscussRounds:  3,
			ParallelModels: []string{"deepseek-pro", "mimo-pro"},
			DiscussModels:  []string{"deepseek-pro", "mimo-pro"},
		},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil // return defaults if no config file
		}
		return cfg, fmt.Errorf("reading config: %w", err)
	}

	md, err := toml.Decode(string(data), &cfg)
	if err != nil {
		return cfg, fmt.Errorf("parsing config: %w", err)
	}

	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		return cfg, fmt.Errorf("unknown config keys: %v", undecoded)
	}

	return cfg, nil
}

func LoadFromEnvOrDefault() (Config, error) {
	path := os.Getenv("HUB_CONFIG")
	if path == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Default(), nil
		}
		path = filepath.Join(home, ".hub", "config.toml")
	}
	return Load(path)
}

func (m ModelDef) ResolveAPIKey() string {
	return os.Getenv(m.APIKeyEnv)
}
