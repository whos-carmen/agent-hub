package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/whos-carmen/agent-hub/config"
	"github.com/whos-carmen/agent-hub/engine"
	"github.com/whos-carmen/agent-hub/models"
	"github.com/whos-carmen/agent-hub/web"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: hub [subcommand] [options]\n\n")
		fmt.Fprintf(os.Stderr, "Subcommands:\n")
		fmt.Fprintf(os.Stderr, "  hub              Start the web dashboard + conversation loop (default)\n")
		fmt.Fprintf(os.Stderr, "  hub ask <model> <prompt>   Send a prompt to a model\n")
		fmt.Fprintf(os.Stderr, "  hub discuss <prompt>        Run a multi-model discussion\n")
		fmt.Fprintf(os.Stderr, "  hub models                  List configured models\n")
		fmt.Fprintf(os.Stderr, "  hub version                 Print version\n")
		fmt.Fprintf(os.Stderr, "\nFlags:\n")
		flag.PrintDefaults()
	}

	cfgPath := flag.String("config", "", "path to config file")
	flag.Parse()

	// Load config
	cfg, err := resolveConfig(*cfgPath)
	if err != nil {
		slog.Error("config", "error", err)
		os.Exit(1)
	}

	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo})))

	args := flag.Args()

	if len(args) == 0 {
		// Default: start web server + conversation loop
		runServer(cfg)
		return
	}

	switch args[0] {
	case "ask":
		runAsk(cfg, args[1:])
	case "discuss":
		runDiscuss(cfg, args[1:])
	case "models":
		runModels(cfg)
	case "version":
		fmt.Println("agent-hub v0.1.0")
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", args[0])
		flag.Usage()
		os.Exit(1)
	}
}

func resolveConfig(path string) (config.Config, error) {
	if path != "" {
		return config.Load(path)
	}
	return config.LoadFromEnvOrDefault()
}

func runServer(cfg config.Config) {
	slog.Info("starting agent-hub", "port", cfg.Port, "default_model", cfg.DefaultModel)

	state := engine.NewState()

	// Start conversation loop in background
	go engine.RunLoop(state, cfg)

	// Start web server
	srv := web.NewServer(state, cfg.Port)
	go srv.Broadcast()

	slog.Info("web dashboard at", "url", fmt.Sprintf("http://localhost:%d", cfg.Port))

	// Handle shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		if err := srv.Start(); err != nil {
			slog.Error("server error", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")
}

func runAsk(cfg config.Config, args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: hub ask <model> <prompt>\n")
		os.Exit(1)
	}

	modelName := args[0]
	prompt := args[1]

	modelConfigs := buildAskModelConfigs(cfg)
	mc, ok := modelConfigs[modelName]
	if !ok {
		fmt.Fprintf(os.Stderr, "model %q not configured\n", modelName)
		os.Exit(1)
	}

	chunkChan := make(chan models.StreamEvent, 64)
	go models.StreamResponse(mc, "", prompt, "", chunkChan)

	for evt := range chunkChan {
		switch evt.Type {
		case "chunk":
			fmt.Print(evt.Chunk)
		case "done":
			fmt.Println()
		case "error":
			fmt.Fprintf(os.Stderr, "\nError: %s\n", evt.Error)
			os.Exit(1)
		}
	}
}

func runDiscuss(cfg config.Config, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: hub discuss <prompt>\n")
		os.Exit(1)
	}

	prompt := args[0]

	state := engine.NewState()
	modelConfigs := buildAskModelConfigs(cfg)

	go func() {
		// Consume broadcast events for display
		for evt := range state.Broadcast {
			switch evt.Type {
			case "dispatch":
				fmt.Printf("\n[%s]\n", evt.Text)
			case "chunk":
				fmt.Print(evt.Chunk)
			case "done":
				fmt.Println("\n")
			}
		}
	}()

	plan := models.DispatchPlan{
		Kind:   "discussion",
		Models: cfg.Routing.DiscussModels,
		Prompt: prompt,
		Rounds: cfg.Routing.DiscussRounds,
	}

	engine.Dispatch(plan, state, modelConfigs)

	_ = state // state isn't printed but used for context
}

func runModels(cfg config.Config) {
	fmt.Println("Configured models:")
	for name, mdef := range cfg.Models {
		key := mdef.ResolveAPIKey()
		status := "✓"
		if key == "" {
			status = "✗ no key"
		}
		fmt.Printf("  %s %-20s (%s) → %s\n", status, name, mdef.Type, mdef.BaseURL)
	}
	fmt.Println()
	fmt.Printf("Default model: %s\n", cfg.DefaultModel)
	fmt.Printf("Routing: parallel on %v | discuss on %v\n", cfg.Routing.ParallelOn, cfg.Routing.DiscussOn)
}

func buildAskModelConfigs(cfg config.Config) map[string]models.ModelConfig {
	mc := make(map[string]models.ModelConfig)
	for name, mdef := range cfg.Models {
		apiKey := mdef.ResolveAPIKey()
		if apiKey != "" {
			mc[name] = models.NewModelConfig(name, apiKey, mdef.BaseURL, mdef.Type, mdef.ModelName)
		}
	}
	return mc
}
