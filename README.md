# agent-hub

A terminal-native conversation hub for orchestrating multiple AI models — parallel dispatch, multi-turn discussion, live TUI, and a router that decides when to involve which model.

--- No AI-written code yet. This is the design document and plan. ---

---

## Why this exists — the evolution

This project went through several iterations before landing on this design. The journey itself explains why the hub works the way it does.

### 1. The P2P agent-link idea

**Original thought**: Put an agent on every machine, have them talk to each other over a message bus. Each machine runs Reasonix (or Mimo), and they exchange messages like Slack bots.

**Problem discovered**: This requires every machine to run a persistent agent daemon with a live MQTT connection. It needs certificates, AWS IoT Core setup, offline queuing policies, and a protocol for inter-agent conversation state. Too much infrastructure for a personal setup. And the agents don't really have anything to say to each other unless a human asks them to.

### 2. The MQTT/WebSocket approach

**Refinement**: Use AWS IoT Core with SigV4 authentication (no certificates) over MQTT WebSocket. Build a Rust binary using `rumqttc` with a `set_request_modifier` hook for SigV4 signing.

**Problem discovered**: Relies on a persistent network connection and a cloud broker. Adds latency through SQS/MQTT round-trips. The "live push" feature (MCP stderr) is unreliable — different hosts (Reasonix, Mimo, Claude Desktop) handle MCP stderr differently or not at all. The complexity of session affinity, offline queueing, and reconnection logic doesn't justify itself for the use case.

### 3. The hub realization

**Breakthrough**: The models themselves are accessible via HTTPS APIs. They don't need to be "running" on a machine — they're always available through DeepSeek's and Mimo's API endpoints. The only thing that needs to run is a **conversation orchestrator** that:

- Maintains conversation state
- Routes tasks to the right models
- Handles parallel dispatch
- Aggregates results and feeds them back to the primary model
- Shows everything happening in a live TUI

**This eliminates**: Persistent daemons, certificates, MQTT brokers, cross-machine networking, offline queueing, agent session management. The entire architecture collapses to one Go binary that makes HTTPS calls.

### 4. The conversation loop

**Final realization**: The hub isn't a message bus — it's a **conversation engine**. Models don't talk to each other directly; the hub maintains a conversation thread and decides which model speaks at each turn. "Models talking" is just the hub calling API A, storing the result, then calling API B with A's output in the prompt. The router decides when to do this serially (discussion), in parallel (code review + tests), or single-model (quick questions).

---

## Architecture

```
┌─ TUI (Bubble Tea) ───────────────────────────────┐
│  Terminal dashboard                               │
│  • scrollable message log (all agent messages)    │
│  • text input box                                 │
│  • status bar (model health, task counts)         │
│  • live streaming indicators                      │
└──────────────────────┬───────────────────────────┘
                       │ user input / state updates
                       ▼
┌─ Conversation Engine ────────────────────────────┐
│                                                    │
│  State (single source of truth):                   │
│  ┌────────────────────────────────────────────┐   │
│  │ MessageLog []Message                        │   │
│  │   • role: "user" | "flash" | "deepseek-pro"│   │
│  │         | "mimo-pro" | "mimo" | "system"   │   │
│  │   • content: string                         │   │
│  │   • timestamp, stream_state, model_info     │   │
│  │                                              │   │
│  │ Inbox: map[model_id]ResponseQueue            │   │
│  │ Pending: map[dispatch_id][]DispatchTarget    │   │
│  │ Config: models, routing_rules, default_model │   │
│  └────────────────────────────────────────────┘   │
│                                                    │
│  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │
│  │ Router   │  │Dispatch  │  │ Conversation   │  │
│  │ decides  │──│parallel  │──│ Loop           │  │
│  │ which    │  │model     │  │ feeds results  │  │
│  │ models   │  │calls     │  │ back to flash  │  │
│  └──────────┘  └──────────┘  └────────────────┘  │
└──────────────────────┬───────────────────────────┘
                       │ HTTPS API calls
                       ▼
┌─ Model API Layer ───────────────────────────────┐
│  • DeepSeek adapter                              │
│  • Mimo adapter                                  │
│  • OpenAI-compatible adapter (any provider)      │
│  • Streaming response support                    │
└─────────────────────────────────────────────────┘
```

---

## How the router works

The router sits between user input and model dispatch. It decides which models to call and in what order. Two modes:

### Automatic routing (configurable heuristics)

```go
func route(input string, context Context) DispatchPlan {
    switch {
    case containsAny(input, "review", "security", "refactor"):
        return Parallel{models: ["deepseek-pro", "mimo-pro"], aggregate: true}
    case containsAny(input, "test", "deploy"):
        return Parallel{models: ["flash", "deepseek-pro"], aggregate: true}
    case containsAny(input, "discuss", "debate", "brainstorm"):
        return Discussion{models: ["deepseek-pro", "mimo-pro"], rounds: 3}
    default:
        return Single{model: config.default_model} // flash
    }
}
```

### Manual override

```
User: "review auth.rs"                    → automatic: parallel deepseek-pro + mimo-pro
User: "review auth.rs @single"            → force single model (flash only)
User: "review auth.rs @parallel"          → force parallel
User: "discuss auth design deepseek,mimo" → multi-turn, 3 rounds default
```

---

## Conversation loop

The hub runs a synchronous conversation loop. Each turn:

1. **Receive user input** from TUI text input or CLI
2. **Evaluate router** against input + context
3. **If dispatch needed**: fire model calls (parallel or sequential), stream results to TUI, store in message log
4. **If aggregation needed**: feed results back to primary model, wait for synthesis
5. **Update state**: append all messages to log, update pending/complete counts
6. **Return to idle** — TUI renders updated state

All model I/O happens in goroutines. Each goroutine pushes chunks and completion events to Bubble Tea's event loop via `Program.Send()`. The TUI never blocks.

---

## TUI layout

```
┌─ Hub v0.1.0 ──── 3 models ● ──── Ctrl+C quit ──┐
│                                                   │
│ [14:02:01]  You: review auth.rs for security     │
│ [14:02:03]  ● Flash: Routing to deepseek-pro...  │
│ [14:02:05]  ◇ Dispatched: deepseek-pro + mimo    │
│ [14:02:06]  ├ DeepSeek-Pro ··· streaming...      │
│ [14:02:08]  ├ Mimo-Pro     ··· streaming...      │
│ [14:02:10]  ✓ DeepSeek-Pro: 3 issues found       │
│ [14:02:12]  ✓ Mimo-Pro: 2 issues found           │
│ [14:02:14]  ● Flash: Consolidated review:        │
│              2 critical, 3 high priority issues   │
│                                                   │
│ Status: idle · T:4 ✓ · D:2 · C:1                 │
│ ┌────────────────────────────────────────────────┐│
│ │ > review auth.rs                                ││
│ └────────────────────────────────────────────────┘│
└───────────────────────────────────────────────────┘
```

Top section is a scrollable viewport with the message log. Bottom is the text input. The status bar shows model health, task counts, and connection state.

Messages use a consistent format:
```
[timestamp]  icon  ModelName: content
```

Icons: `●` = currently responding, `◇` = system event, `├` = dispatch target, `✓` = completed, `✗` = failed, `⌛` = waiting.

---

## CLI interface

The TUI is the primary interface, but CLI subcommands exist for scripting:

```
hub tui                    # Start interactive dashboard
hub ask <model> <prompt>   # Send a prompt, print the response
hub discuss <topic>        # Single-use discussion, print result
hub models                 # List configured models and health
hub version                # Print version
```

The TUI is the full experience. CLI subcommands exist for CI/automation or quick checks without starting the dashboard.

---

## Plan — phases

### Phase 1: Scaffold + config + CLI routing
- `go mod init github.com/whos-carmen/agent-hub`
- `main.go` — CLI dispatch (`tui`, `ask`, `models`, `version`)
- `config/config.go` — TOML config parsing (model API keys, endpoints, default model, routing rules)
- `config.go` types: `ModelConfig`, `HubConfig`, `RouterRule`
- Verifiable: `hub models` reads config, lists configured models

### Phase 2: Conversation engine
- `engine/state.go` — `ConversationState` struct: message log, pending dispatches, model registry
- `engine/message.go` — `Message` type with role, content, timestamp, stream_state, model_info
- `engine/router.go` — `Route()` function: input → dispatch plan (single, parallel, discussion)
- `engine/loop.go` — conversation loop: receive input → route → dispatch → aggregate → update state
- Verifiable: unit tests for router decisions, state mutations

### Phase 3: Model API adapters
- `models/deepseek.go` — DeepSeek chat completions API (flash, pro)
- `models/mimo.go` — Mimo API (OpenAI-compatible format)
- `models/openai.go` — Generic OpenAI-compatible adapter
- `models/client.go` — shared streaming HTTP client, response parsing
- Verifiable: `hub ask deepseek "hello"` returns a response

### Phase 4: Dispatcher + aggregation
- `engine/dispatch.go` — `Dispatch()`: fire N model calls in parallel goroutines, push stream chunks to TUI via channel, collect full responses
- `engine/aggregate.go` — `Aggregate()`: take N model outputs, format them as context for flash, return combined
- Verifiable: `hub discuss "design auth" --models deepseek-pro,mimo-pro` runs multi-turn

### Phase 5: TUI — message log + text input
- `tui/root.go` — Bubble Tea model (state, update, view)
- `tui/log.go` — scrollable viewport rendering the message log
- `tui/input.go` — text input widget
- `tui/status.go` — status bar (model health, task counts)
- Wire: user types → conversation loop → model goroutines push updates to TUI
- Verifiable: `hub tui` starts, you can type messages and see responses stream in

### Phase 6: TUI — live streaming + dispatch visualization
- `tui/stream.go` — model response streaming indicators (progress bars per model)
- `tui/dispatch.go` — tree view of parallel dispatches (which models, status of each)
- Auto-scroll: viewport follows new messages unless user scrolls up
- Verifiable: parallel dispatch shows multiple models streaming simultaneously

### Phase 7: Repo manager
- `repo/manager.go` — git operations (clone, read files, branch, apply diffs, commit)
- Model never touches filesystem — hub reads files and includes in prompt
- Context injection: `repo_read(paths[])` → file contents → appended to user prompt
- Verifiable: `hub ask deepseek "review this" --file src/main.rs` includes file content

### Phase 8: Package + cross-compile
- `Makefile` with targets for `linux/amd64`, `windows/amd64`
- `config.toml.example` — documented template
- `README.md` with install and usage docs

---

## Dependencies

```
github.com/charmbracelet/bubbletea   # TUI
github.com/charmbracelet/bubbles     # TUI widgets (viewport, textinput, spinner)
github.com/charmbracelet/lipgloss    # TUI styling
github.com/BurntSushi/toml           # config parsing
github.com/go-git/go-git/v5          # git operations
```

Everything else is Go standard library (`net/http`, `encoding/json`, `log/slog`, `os/signal`).

---

## Non-goals (decided against)

- **MQTT / WebSocket / P2P networking** — models are API calls, not peers
- **Certificate management** — SigV4 was a workaround for a bridge we don't need
- **Persistent agent daemons** — hub is stateless, runs on demand
- **Cross-machine message routing** — one hub, one LXC, one focus
- **MCP protocol / plugin system** — hub is its own interface, not a tool for another agent
- **Authentication / multi-user** — personal tool, single user
- **Persistent storage** — in-memory only. History lives in the terminal scrollback
