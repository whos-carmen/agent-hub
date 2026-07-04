# agent-hub

A terminal-native conversation hub for orchestrating multiple AI models — parallel dispatch, multi-turn discussion, live TUI, optional web dashboard, and a router that decides when to involve which model.

**Language**: Go (static binary, zero runtime deps)
**Platforms**: Linux LXC (primary), Windows (cross-compiled `.exe`)

--- No AI-written code yet. This is the design document and plan. ---

---

## Why this exists — the evolution

This project went through several iterations before landing on this design. The journey itself explains why the hub works the way it does.

### 1. The P2P agent-link idea

**Original thought**: Put an agent on every machine, have them talk to each other over a message bus. Each machine runs Reasonix (or Mimo), and they exchange messages like Slack bots.

**Problem discovered**: This requires every machine to run a persistent agent daemon with a live MQTT connection. It needs certificates, AWS IoT Core setup, offline queuing policies, and a protocol for inter-agent conversation state. Too much infrastructure for a personal setup. And the agents don't really have anything to say to each other unless a human asks them to.

### 2. The MQTT/WebSocket approach

**Refinement**: Use AWS IoT Core with SigV4 authentication (no X.509 certificates) over MQTT WebSocket. Build a Rust binary using `rumqttc` with a `set_request_modifier` hook for SigV4 signing.

**Problem discovered**: Relies on a persistent network connection and a cloud broker. The "live push" feature (MCP stderr) is unreliable — different hosts (Reasonix, Mimo, Claude Desktop) handle MCP stderr differently or not at all. The complexity of session affinity, offline queueing, and reconnection logic doesn't justify itself for the use case.

### 3. The hub realization

**Breakthrough**: The models themselves are accessible via HTTPS APIs. They don't need to be "running" on a machine — they're always available through DeepSeek's and Mimo's API endpoints. The only thing that needs to run is a **conversation orchestrator** that:

- Maintains conversation state
- Routes tasks to the right models
- Handles parallel dispatch
- Aggregates results and feeds them back to the primary model (flash)
- Shows everything happening in a live TUI / web dashboard

**This eliminates**: Persistent daemons, certificates, MQTT brokers, cross-machine networking, offline queueing, agent session management. The entire architecture collapses to one Go binary that makes HTTPS calls.

### 4. The conversation loop

The hub isn't a message bus — it's a **conversation engine**. Models don't talk to each other directly; the hub maintains a conversation thread and decides which model speaks at each turn. "Models talking" is just the hub calling API A, storing the result, then calling API B with A's output in the prompt. The router decides when to do this serially (discussion), in parallel (code review + tests), or single-model (quick questions).

### 5. The web dashboard

The hub started as a TUI-only tool, but a web dashboard reachable from the host machine (and others on the LAN) is a natural addition. The Go binary embeds a web server that shares the same `ConversationState` struct — the TUI and web UI are two views of the same state, served from the same process.

### 6. Language decision

**Go, not Rust or Zig**, for this specific project:

| Factor | Go | Rust | Zig |
|---|---|---|---|
| TUI framework | Bubble Tea — mature, pure Go, production-grade | ratatui — excellent but more code | **None** — would need to write raw ANSI or bind ncurses |
| Web server | `net/http` + `//go:embed` — stdlib, one-liner | axum — excellent but external crate | `std.http.Server` — minimal, manual routing |
| WebSocket | gorilla/websocket — mature, stable | tokio-tungstenite — good, more explicit | **None** — would need to write from scratch or bind C |
| Git operations | go-git — pure Go, no C deps | git2 — wraps libgit2 (C library, needs linking) | **None** — would need to shell out to git CLI |
| JSON streaming | `encoding/json` — stdlib, adequate | serde_json — fastest, derive macros | `std.json` — token-by-token manual parsing |
| Concurrency model | Goroutines + channels — designed for dispatch/aggregate pattern | tokio async/await — equally good but more explicit about Send + lifetimes | Threads + atomics. Async still evolving |
| Cross-compile Windows | `GOOS=windows go build` — one flag | `--target x86_64-pc-windows-gnu` — needs toolchain | `-target x86_64-windows` — works but unproven at this scale |

The bottleneck is model API latency (500ms-30s per call), not binary execution speed. There is no meaningful performance difference between Go and Rust for this workload. Go wins on simplicity: Bubble Tea for the TUI, `net/http` for the web server, `go-git` for repo operations — all pure Go, zero C dependencies, one binary.

**Zig is a non-starter.** It has no TUI framework, no git library, no WebSocket library, no high-level HTTP client. Building this in Zig would mean writing half the infrastructure from scratch — 3-5x the code with no benefit for an I/O-bound HTTP application.

---

## Architecture

```
┌─ Process ───────────────────────────────────────────┐
│                                                       │
│  ┌── TUI (Bubble Tea) ─────┐  ┌── Web server ──────┐│
│  │ • message log           │  │ • / → dashboard     ││
│  │ • text input            │  │ • /api/messages     ││
│  │ • status bar            │  │ • /api/send         ││
│  │ • streaming indicators  │  │ • /api/models       ││
│  │ • dispatch tree view    │  │ • /stream (SSE)     ││
│  └──────────┬──────────────┘  └──────────┬──────────┘│
│             │                             │            │
│             └───── ConversationState ─────┘            │
│                     (shared struct)                    │
│                                                       │
│  ┌─ Conversation Engine ────────────────────────────┐ │
│  │  Router → Dispatch → Aggregate → Loop            │ │
│  │  (goroutines per model, results via channel)      │ │
│  └──────────────────────┬───────────────────────────┘ │
│                          │ HTTPS API calls             │
│                          ▼                             │
│  ┌─ Model API Layer ─────────────────────────────────┐ │
│  │  DeepSeek (flash, pro) · Mimo · OpenAI-compatible │ │
│  │  (streaming HTTP clients in goroutines)            │ │
│  └───────────────────────────────────────────────────┘ │
│                                                       │
└───────────────────────────────────────────────────────┘
```

Both UIs share the same `ConversationState` struct. The TUI uses Bubble Tea's event loop. The web server uses `net/http` and SSE for live streaming. Model goroutines push updates to both via channels.

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
hub ask deepseek "review auth.rs"                   → single model
hub ask deepseek,mimo "review auth.rs"               → parallel (specified model list)
hub discuss deepseek,mimo "design the auth flow"     → multi-turn discussion (default 3 rounds)
```

---

## Conversation loop

The hub runs a synchronous conversation loop. Each turn:

1. **Receive user input** from TUI text input, web UI form, or CLI
2. **Evaluate router** against input + context
3. **If dispatch needed**: spawn goroutines for each model call, stream results to both UIs, store in message log
4. **If aggregation needed**: feed results back to primary model, wait for synthesis
5. **Update state**: append all messages to log, update pending/complete counts
6. **Return to idle** — both UIs render updated state

All model I/O happens in goroutines. Each goroutine pushes chunks and completion events to both Bubble Tea's event loop (via `Program.Send()`) and the web UI (via SSE channel).

---

## TUI layout

```
┌─ Hub v0.1.0 ──── 3 models ● ──── Ctrl+C quit ──┐
│                                                    │
│ [14:02:01]  You: review auth.rs for security      │
│ [14:02:03]  ● Flash: Routing to deepseek-pro...   │
│ [14:02:05]  ◇ Dispatched: deepseek-pro + mimo     │
│ [14:02:06]  ├ DeepSeek-Pro ··· streaming...       │
│ [14:02:08]  ├ Mimo-Pro     ··· streaming...       │
│ [14:02:10]  ✓ DeepSeek-Pro: 3 issues found        │
│ [14:02:12]  ✓ Mimo-Pro: 2 issues found            │
│ [14:02:14]  ● Flash: Consolidated review:         │
│              2 critical, 3 high priority issues    │
│                                                    │
│ Status: idle · T:4 ✓ · D:2 · C:1                  │
│ ┌─────────────────────────────────────────────────┐│
│ │ > review auth.rs                                 ││
│ └─────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────┘
```

Top section is a scrollable viewport with the message log. Bottom is the text input. The status bar shows model health, task counts, and connection state.

Messages use a consistent format: `[timestamp] icon ModelName: content`.

Icons: `●` = currently responding, `◇` = system event, `├` = dispatch target, `✓` = completed, `✗` = failed, `⌛` = waiting.

---

## Web UI

The web UI is a proper chat-style interface — visually distinct from the TUI but sharing the same feature set and backing state. Served via `//go:embed`, zero build tooling.

Served on `http://localhost:8080` (or `0.0.0.0:8080` for LAN access from other machines on the network).

### Stack

| Layer | Technology | Why |
|---|---|---|
| HTML | Single `index.html` served via `//go:embed` | Zero build step, one binary |
| CSS | Tailwind via CDN (`<script src="https://cdn.tailwindcss.com">`) | Utility-first, responsive, dark mode, no npm |
| JS | Vanilla (~150 lines) | SSE listener, auto-scroll, form submit, model status polling |
| Backend | `net/http` + SSE endpoint | Same Go process, same conversation state |

### Layout

```
┌─ agent-hub ───────────────────────────────────────────┐
│ ● deepseek-pro  ● mimo-pro  ● flash                   │
├───────────────────────────────────────────────────────┤
│                                                        │
│  ┌─────────────────────────────────────────────┐      │
│  │ 14:02:01                                     │      │
│  │ You: review auth.rs for security             │      │
│  └─────────────────────────────────────────────┘      │
│                                                        │
│  ┌─────────────────────────────────────────────┐      │
│  │ 14:02:03  ● Flash                           │      │
│  │ Routing to deepseek-pro + mimo for review   │      │
│  └─────────────────────────────────────────────┘      │
│                                                        │
│  14:02:06                                              │
│  ┌─────────────────┐  ┌─────────────────┐             │
│  │ DeepSeek-Pro    │  │ Mimo-Pro        │             │
│  │ ████████████░░  │  │ ████████░░░░░░  │             │
│  │ reviewing...    │  │ reviewing...    │             │
│  └─────────────────┘  └─────────────────┘             │
│                                                        │
│  ┌─────────────────────────────────────────────┐      │
│  │ 14:02:10  ✓ DeepSeek-Pro                     │      │
│  │ Found: SQL injection in login handler (line 142)  │
│  │ Found: Missing rate limiting on /api/auth         │
│  │ Found: Hardcoded JWT secret in config              │
│  └─────────────────────────────────────────────┘      │
│                                                        │
│  ┌─────────────────────────────────────────────┐      │
│  │ 14:02:12  ✓ Mimo-Pro                         │      │
│  │ Found: Session tokens lack rotation mechanism│      │
│  │ Found: No CSRF protection on POST endpoints  │      │
│  └─────────────────────────────────────────────┘      │
│                                                        │
│  ┌─────────────────────────────────────────────┐      │
│  │ 14:02:14  ● Flash                           │      │
│  │ Consolidated review:                        │      │
│  │ 2 critical, 3 high priority issues          │      │
│  │ Overlap: input validation in login handler  │      │
│  └─────────────────────────────────────────────┘      │
│                                                        │
│  ┌──────────────────────────────────────────┐ [Send]  │
│  │ review auth.rs                           │         │
│  └──────────────────────────────────────────┘         │
└───────────────────────────────────────────────────────┘
```

### Key design decisions

**Side-by-side parallel dispatch cards**: when the router dispatches to multiple models simultaneously, the web UI shows them as adjacent cards with independent progress bars. Each card streams its response live. This is the one visual element that's *better* than the TUI — the TUI can only interleave lines, the web UI can use true parallel layout.

**SSE streaming**: `GET /api/stream` pushes `event: message` and `event: status` events. The JS handler appends them to the DOM as they arrive. Same channel backs both TUI and web UI — the SSE handler is a thin wrapper around the shared conversation state broadcast.

**No refresh needed**: messages arrive via SSE, model status updates arrive via SSE, router dispatch events arrive via SSE. The page is live from connection to close.

**Responsive**: on narrow screens, parallel cards stack vertically. On wide screens, they sit side-by-side.

**Dark mode**: respects `prefers-color-scheme` via Tailwind's `dark:` variant. No toggle needed.

### Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/` | GET | Web dashboard (single HTML file) |
| `/api/messages` | GET | Fetch full message log (JSON) |
| `/api/send` | POST | Submit a message |
| `/api/models` | GET | Model health and status |
| `/api/stream` | GET | SSE stream (live message push) |

---

## CLI interface

```
hub tui                    # Start interactive dashboard (TUI + web server)
hub ask <model> <prompt>   # Send a prompt, print the response
hub discuss <topic>        # Single-use discussion, print result
hub models                 # List configured models and health
hub version                # Print version
```

The TUI is the primary interface. CLI subcommands exist for automation or quick checks.

---

## Why this is different from existing tools

Not a framework (CrewAI), not an app builder (Dify), not a multi-model chat client (LobeHub). It occupies a different niche:

- **Live parallel streaming in TUI + web UI** — watch multiple models respond simultaneously in your terminal or browser. The web UI shows parallel dispatches as side-by-side cards with independent progress bars — something no existing tool does.
- **Conversation loop with routing** — the hub *decides* which models to involve based on what you're asking. The router is the core feature, not a plugin or manual toggle.
- **Zero-infra single binary** — `go build`, `scp`, `./hub tui`. No Docker compose, no database, no Python venv, no npm install, no web server setup. The web UI is embedded in the binary via `//go:embed`.
- **Design for the model, not the role** — CrewAI makes you define agent backstories. The hub doesn't role-play. The difference between DeepSeek Pro and Mimo Pro is their *training and architecture*, not a prompt describing their personality.
- **Repo ownership** — the hub owns the git repo, reads files for context, applies diffs. Models never touch the filesystem.
- **Web UI that's better than the TUI** — the web dashboard has true parallel layout (side-by-side streaming cards), smooth animations, dark mode, and works on mobile. Same features, better visuals. Tailwind CDN + vanilla JS, no build step.

---

## Plan — phases

### Phase 1: Scaffold + config + CLI routing
- `go mod init github.com/whos-carmen/agent-hub`
- `main.go` — CLI dispatch (`tui`, `ask`, `models`, `version`)
- `config/config.go` — TOML config parsing (model API keys, endpoints, default model, routing rules)
- `models/types.go` — shared types accessible from all packages
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
- `engine/dispatch.go` — `Dispatch()`: fire N model calls in parallel goroutines, push stream chunks to UI channel, collect full responses
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

### Phase 7: Web dashboard
- `web/handler.go` — HTTP server with `net/http` serving: `/` (dashboard), `/api/send` (POST), `/api/messages` (GET), `/api/models` (GET), `/api/stream` (SSE)
- `web/static/index.html` — single HTML file, embedded via `//go:embed`
  - Tailwind CSS via CDN (`cdn.tailwindcss.com`)
  - Vanilla JS (~150 lines): SSE listener for live streaming, auto-scroll, form submit, model status polling
  - Side-by-side dispatch cards for parallel model responses
  - Dark mode via `prefers-color-scheme`
  - Responsive layout (cards stack on narrow screens)
- `web/sse.go` — SSE endpoint that broadcasts from the same channel the TUI uses
- Wire: `ConversationState` broadcasts updates to all connected SSE clients
- Verifiable: open `http://lxc-ip:8080` in a browser, see live streaming parallel dispatch

### Phase 8: Repo manager
- `repo/manager.go` — git operations (clone, read files, branch, apply diffs, commit)
- Model never touches filesystem — hub reads files and includes in prompt via context injection
- Verifiable: `hub ask deepseek "review this" --file src/main.rs` includes file content in the prompt

### Phase 9: Package + cross-compile
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

Everything else is Go standard library (`net/http`, `encoding/json`, `log/slog`, `os/signal`, `embed`).

---

## Non-goals (decided against)

- **MQTT / WebSocket / P2P networking** — models are API calls, not peers
- **Certificate management** — SigV4 was a workaround for a bridge we don't need
- **Persistent agent daemons** — hub is stateless, runs on demand
- **Cross-machine message routing** — one hub, one LXC, one focus
- **MCP protocol / plugin system** — hub is its own interface, not a tool for another agent
- **Authentication / multi-user** — personal tool, single user (web dashboard is LAN-only, no auth)
- **Persistent storage** — in-memory only. History lives in the terminal scrollback
- **Role-playing agents** — CrewAI-style agent personalities. Models are selected by capability, not persona
- **Rust / Zig** — evaluated both; Go is the right fit for this I/O-bound, TUI-heavy, multi-platform project
