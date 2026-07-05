package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/whos-carmen/agent-hub/models"
)

// ToolDef describes a tool available to models.
type ToolDef struct {
	Name        string            // tool name exposed to the model
	Description string            // human description for the model
	Dangerous   bool              // requires approval (unless YOLO)
	Parameters  map[string]any    // JSON Schema parameters
	Handler     ToolHandler       // execution function
}

// ToolHandler receives the tool name and parsed arguments, returns result text.
type ToolHandler func(args map[string]any) (string, error)

// ToolRegistry maps tool names to their definitions.
type ToolRegistry struct {
	tools map[string]ToolDef
}

// NewToolRegistry returns a registry with all built-in tools registered.
func NewToolRegistry() *ToolRegistry {
	tr := &ToolRegistry{tools: make(map[string]ToolDef)}

	// ─── bash ──────────────────────────────────────
	tr.Register(ToolDef{
		Name:        "bash",
		Description: "Run a shell command in the agent-hub working directory. Use for building, testing, git operations, file listing, etc.",
		Dangerous:   true,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"command": map[string]any{
					"type":        "string",
					"description": "The shell command to execute",
				},
			},
			"required": []string{"command"},
		},
		Handler: func(args map[string]any) (string, error) {
			cmd, _ := args["command"].(string)
			if cmd == "" {
				return "", fmt.Errorf("command is required")
			}
			out, err := exec.Command("bash", "-c", cmd).CombinedOutput()
			if err != nil {
				return fmt.Sprintf("EXIT %v\n%s", err, string(out)), nil
			}
			if len(out) == 0 {
				return "(no output)", nil
			}
			if len(out) > 8000 {
				out = out[:8000]
			}
			return string(out), nil
		},
	})

	// ─── read_file ─────────────────────────────────
	tr.Register(ToolDef{
		Name:        "read_file",
		Description: "Read a file from the filesystem. Returns the file contents.",
		Dangerous:   false,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Path to the file to read",
				},
			},
			"required": []string{"path"},
		},
		Handler: func(args map[string]any) (string, error) {
			path, _ := args["path"].(string)
			if path == "" {
				return "", fmt.Errorf("path is required")
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return "", fmt.Errorf("read %s: %w", path, err)
			}
			if len(data) > 10000 {
				data = data[:10000]
			}
			return string(data), nil
		},
	})

	// ─── write_file ────────────────────────────────
	tr.Register(ToolDef{
		Name:        "write_file",
		Description: "Write content to a file, creating parent directories if needed. Overwrites existing content.",
		Dangerous:   true,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Path to the file to write",
				},
				"content": map[string]any{
					"type":        "string",
					"description": "Content to write to the file",
				},
			},
			"required": []string{"path", "content"},
		},
		Handler: func(args map[string]any) (string, error) {
			path, _ := args["path"].(string)
			content, _ := args["content"].(string)
			if path == "" {
				return "", fmt.Errorf("path is required")
			}
			dir := filepath.Dir(path)
			if err := os.MkdirAll(dir, 0755); err != nil {
				return "", fmt.Errorf("mkdir %s: %w", dir, err)
			}
			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				return "", fmt.Errorf("write %s: %w", path, err)
			}
			return fmt.Sprintf("Wrote %d bytes to %s", len(content), path), nil
		},
	})

	// ─── git_status ────────────────────────────────
	tr.Register(ToolDef{
		Name:        "git_status",
		Description: "Show git status (modified files, untracked files).",
		Dangerous:   false,
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Handler: func(args map[string]any) (string, error) {
			out, err := exec.Command("git", "-C", ".", "status", "--short").CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git status: %w: %s", err, string(out))
			}
			o := string(out)
			if strings.TrimSpace(o) == "" {
				return "(clean — no changes)", nil
			}
			return o, nil
		},
	})

	// ─── git_diff ──────────────────────────────────
	tr.Register(ToolDef{
		Name:        "git_diff",
		Description: "Show git diff of unstaged changes.",
		Dangerous:   false,
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Handler: func(args map[string]any) (string, error) {
			out, err := exec.Command("git", "-C", ".", "diff").CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git diff: %w: %s", err, string(out))
			}
			o := string(out)
			if strings.TrimSpace(o) == "" {
				return "(no unstaged changes)", nil
			}
			return o, nil
		},
	})

	// ─── git_log ───────────────────────────────────
	tr.Register(ToolDef{
		Name:        "git_log",
		Description: "Show recent git commit history.",
		Dangerous:   false,
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Handler: func(args map[string]any) (string, error) {
			out, err := exec.Command("git", "-C", ".", "log", "-10", "--oneline").CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git log: %w: %s", err, string(out))
			}
			return string(out), nil
		},
	})

	// ─── git_commit ────────────────────────────────
	tr.Register(ToolDef{
		Name:        "git_commit",
		Description: "Stage all changes and commit with a message.",
		Dangerous:   true,
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"message": map[string]any{
					"type":        "string",
					"description": "Commit message",
				},
			},
			"required": []string{"message"},
		},
		Handler: func(args map[string]any) (string, error) {
			msg, _ := args["message"].(string)
			if msg == "" {
				return "", fmt.Errorf("message is required")
			}
			// Stage
			if out, err := exec.Command("git", "-C", ".", "add", "-A").CombinedOutput(); err != nil {
				return "", fmt.Errorf("git add: %w: %s", err, string(out))
			}
			out, err := exec.Command("git", "-C", ".", "commit", "-m", msg).CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("git commit: %w: %s", err, string(out))
			}
			return string(out), nil
		},
	})

	// ─── hub_build ─────────────────────────────────
	tr.Register(ToolDef{
		Name:        "hub_build",
		Description: "Rebuild the agent-hub binary (go build -o bin/hub). Returns build output.",
		Dangerous:   false,
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Handler: func(args map[string]any) (string, error) {
			out, err := exec.Command("go", "build", "-o", "bin/hub", ".").CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("build failed: %w\n%s", err, string(out))
			}
			if len(out) == 0 {
				return "Build succeeded", nil
			}
			return string(out), nil
		},
	})

	// ─── hub_restart ───────────────────────────────
	tr.Register(ToolDef{
		Name:        "hub_restart",
		Description: "Restart the agent-hub process (after a build). Sends SIGTERM to old process and starts new one.",
		Dangerous:   true,
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Handler: func(args map[string]any) (string, error) {
			// Find and kill old hub processes
			exec.Command("pkill", "hub").Run()
			return "Restart signal sent. The hub will restart via its supervisor.", nil
		},
	})

	return tr
}

// Register adds a tool to the registry.
func (tr *ToolRegistry) Register(td ToolDef) {
	tr.tools[td.Name] = td
}

// Get returns a tool definition by name.
func (tr *ToolRegistry) Get(name string) (ToolDef, bool) {
	td, ok := tr.tools[name]
	return td, ok
}

// Execute runs a tool by name with the given arguments.
func (tr *ToolRegistry) Execute(name string, args map[string]any) (string, error) {
	td, ok := tr.tools[name]
	if !ok {
		return "", fmt.Errorf("unknown tool: %s", name)
	}
	return td.Handler(args)
}

// IsDangerous returns true if the tool requires approval (when not in YOLO mode).
func (tr *ToolRegistry) IsDangerous(name string) bool {
	td, ok := tr.tools[name]
	if !ok {
		return true // unknown tools are dangerous by default
	}
	return td.Dangerous
}

// OpenAIFormats returns all tools in OpenAI function-calling format.
func (tr *ToolRegistry) OpenAIFormats() []models.Tool {
	var tools []models.Tool
	for _, td := range tr.tools {
		tools = append(tools, models.Tool{
			Type: "function",
			Function: models.ToolFunction{
				Name:        td.Name,
				Description: td.Description,
				Parameters:  td.Parameters,
			},
		})
	}
	return tools
}

// Names returns all registered tool names.
func (tr *ToolRegistry) Names() []string {
	names := make([]string, 0, len(tr.tools))
	for name := range tr.tools {
		names = append(names, name)
	}
	return names
}
