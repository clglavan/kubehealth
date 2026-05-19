# KubeHealth

A Kubernetes event dashboard with AI-powered Root Cause Analysis. Fetches events from your cluster, visualises ownership chains, correlates cross-namespace events by timestamp, and lets you analyse any object with a local LLM running in LM Studio — all from a clean dark-themed web UI.

## Features

### Cluster visibility
- **Namespace grid dashboard** — cards per namespace showing warning/normal counts and top objects, with per-namespace AI analysis
- **Ownership chain detection** — links related objects (Deployment → ReplicaSet → Pod, CronJob → Job → Pod, StatefulSet → Pod, DaemonSet → Pod) using naming conventions; zero extra API calls
- **Cascade failure detection** — flags a chain red when the root and all leaf members have warnings
- **Sidebar + detail panel** — sticky sidebar with search and warnings-only filter; detail panel shows full event timeline for the selected object
- **Object describe** — click any object name to see live spec, status, conditions, and labels; supports built-in types and all CRDs via dynamic API discovery
- **Rate-limit-safe fetching** — single background worker refreshes one namespace every 2 s, stalest-first; `QPS=5 / Burst=10` cap on the k8s REST client

### Per-object action buttons

Every object in the namespace view shows up to four action buttons:

| Button | What it does | K8s API? | LLM? |
|--------|-------------|----------|------|
| **Logs** | Fetches the last N container log lines. Tries the current run first, then `Previous=true` (covers CrashLoopBackOff). | ✅ | ❌ |
| **Correlate** | Scans the in-memory event cache across **all** namespaces for events whose timestamp falls within ±5 min of the selected object's events. | ❌ | ❌ |
| **RCA** | Multi-step agent loop — sends events + describe + pod logs to the LLM with 4 tools; model calls tools iteratively until confident, then produces a structured Root Cause + Resolution + Confidence. | ✅ | ✅ |
| **AI** | Single-shot analysis: streams events + describe + pod logs to the LLM and shows tokens as they arrive. | ✅ | ✅ |

Results are collapsible panels (expanded by default, except Correlate which is collapsed). AI and RCA results persist server-side — refreshing the page reconnects to a still-running analysis.

### AI / RCA (powered by LM Studio)

- **Streaming responses** — regular AI uses `stream: true`; tokens update the UI every 300 ms via server-side cache polling
- **Streaming cursor** — a blinking `▋` appears while tokens are arriving; the panel style is identical before and after completion (no visual jump)
- **RCA agent loop** — up to N iterations (configurable); the model can call `get_pod_logs`, `get_object_describe`, `get_object_events`, and `get_correlated_events` as tools
- **Initial context** always includes events + object describe + pod logs (including previous-run logs for crashed pods)
- **Context overflow detection** — if LM Studio returns a 400 with the "Trying to keep the first N tokens" message, the UI shows a clear "increase your context length" guide instead of hanging
- **Concurrent call limit** — configurable max simultaneous requests to LM Studio; excess requests are rejected immediately
- **Page-refresh safe** — all analysis goroutines run against `context.Background()` and survive browser navigation

### Settings (⚙ Settings modal)
| Setting | Effect |
|---------|--------|
| Log Lines | Lines fetched by Logs button and included in AI / RCA context |
| LLM Timeout | Minutes before giving up — large objects like Elasticsearch need 5–10 min for prompt processing |
| Max Concurrent Calls | Prevents overloading a single-GPU local model |
| RCA Max Iterations | Tool-calling rounds before the agent is forced to conclude |

### AI Agent (✦ AI Agent modal)
- Edit the **system prompt** that is injected as the `system` role message on every LLM call
- Live **context window estimate** — shows system prompt + events + logs + tool definitions as % of `loaded_context_length` (fetched from LM Studio's `/api/v0/models`)
- **Response format** is hardcoded in the user message (Summary / Issues / Actions) and cannot be broken by the system prompt
- **Reset to default** restores the initial system prompt

### Docs (Docs modal)
Interactive system architecture diagram showing all data flows:
- Animated particles travel along connection lines matching actual code paths
- Hover any action chip (AI / Logs / Correlate / RCA / Settings / AI Agent) to highlight the exact nodes and paths that action uses and read a one-line description

---

## Requirements

- Go 1.21+
- A valid `kubeconfig` (defaults to `~/.kube/config`)
- Read access to `events` in the target namespaces
- [LM Studio](https://lmstudio.ai) running locally with a model loaded (for AI / RCA features)

### LM Studio setup

1. Download and install LM Studio
2. Load a model — **qwen/qwen3-4b-2507** is the tested default
3. Start the local server (default: `http://localhost:1234`)
4. For large objects (Elasticsearch, etc.) increase the model's **Context Length** via the gear icon → Context Length → Reload — 8192 or 16384 tokens recommended

---

## Environment variables

All AI settings can be overridden at startup:

| Variable | Default | Description |
|----------|---------|-------------|
| `LMSTUDIO_URL` | `http://localhost:1234/v1` | LM Studio OpenAI-compat base URL |
| `LMSTUDIO_MODEL` | `qwen/qwen3-4b-2507` | Model identifier |
| `AI_ENABLED` | `true` | Set to `false` or `0` to disable AI features |
| `AI_SYSTEM_PROMPT` | *(built-in SRE prompt)* | Default system prompt |
| `KUBECONFIG` | `~/.kube/config` | Path to kubeconfig |

Runtime settings (log lines, timeout, max calls, RCA iterations) are configured in the UI under ⚙ Settings and take effect immediately without restart.

---

## Build

```bash
go build -o kubehealth .
```

## Usage

```bash
# Watch all namespaces (default)
./kubehealth

# Watch specific namespaces
./kubehealth -namespaces default,kube-system,monitoring

# Custom port and kubeconfig
./kubehealth -port 9090 -kubeconfig /path/to/kubeconfig
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8080` | HTTP listen port |
| `-kubeconfig` | `~/.kube/config` | Path to kubeconfig |
| `-namespaces` | *(all)* | Comma-separated namespace list |

Open `http://localhost:8080` in your browser.

---

## API Endpoints

### Data
| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | `{"loaded": N, "total": M, "ready": bool}` — namespace loading progress |
| `GET /api/events` | Full dashboard data JSON |

### AI
| Endpoint | Description |
|----------|-------------|
| `POST /api/ai/analyze/{ns\|object}/{…}` | Trigger AI analysis for a namespace or object |
| `GET /api/ai/insights/{ns\|object}/{…}` | Poll AI result (`partial`, `content`, `error`) |
| `GET /api/ai/config` | Read system prompt + LM Studio model info |
| `POST /api/ai/config` | Update system prompt |

### RCA
| Endpoint | Description |
|----------|-------------|
| `POST /api/rca/{ns}/{kind}/{name}` | Start RCA agent loop |
| `GET /api/rca/status/{ns}/{kind}/{name}` | Poll RCA state (`iteration`, `steps[]`, `result`) |

### Logs & Correlate
| Endpoint | Description |
|----------|-------------|
| `GET /api/logs/{ns}/{pod}` | Pod logs with Previous fallback |
| `GET /api/correlate/{ns}/{kind}/{name}` | Cross-namespace correlated events |

### Settings
| Endpoint | Description |
|----------|-------------|
| `GET /api/settings` | Read runtime settings |
| `POST /api/settings` | Update log lines, timeout, max calls, RCA iterations |

---

## Architecture

```
main.go          — HTTP server, routing, template rendering
ai.go            — AI streaming (callLMStudio, streamLMStudio), insight cache
ai_config.go     — System prompt store, LM Studio model-info fetch (/api/v0/models)
rca.go           — RCA agent loop, tool dispatcher, RCA state store
correlate.go     — Cross-namespace event correlation (pure nsCache read)
logs.go          — Pod log fetching with Previous-run fallback
settings.go      — Runtime settings (log lines, timeout, max calls, RCA iterations)

templates/
├─ dashboard.html      — Namespace grid, per-namespace AI button, streaming pollAI
├─ namespace.html      — Sidebar + detail panels, all action buttons, JS agent functions
├─ ai_modal.html       — AI Agent config modal (system prompt, context window estimate)
├─ settings_modal.html — Settings modal (log lines, timeouts, concurrency, RCA iterations)
├─ docs_modal.html     — Interactive architecture diagram with animated data flows
├─ loading.html        — Animated loading screen
└─ object.html         — YAML viewer
```

### Data flow summary

```
Kubernetes API
    │  events poll (Worker, every 2 s)
    ▼
nsCache (sync.Map)  ◄──── feeds everything below
    │
    ├── Dashboard / Namespace UI  (HTTP handlers, read-only)
    │
    ├── Correlate  ──► scans nsCache, zero K8s API call, zero LLM
    │
    ├── Logs  ──► K8s GetLogs (current + Previous fallback)
    │
    ├── AI Analysis  ──► nsCache + K8s describe + K8s logs
    │                        │
    │                        └──► LM Studio (stream=true, SSE)
    │                                 │ tokens every 300 ms
    │                                 ▼
    │                          aiInsightCache  ◄── browser polls 500 ms
    │
    └── RCA Agent  ──► nsCache + K8s describe + K8s logs (initial context)
                            │
                            └──► LM Studio (stream=false, with tools)
                                      │ tool_calls response
                                      ├─ get_object_events      → nsCache
                                      ├─ get_correlated_events  → nsCache
                                      ├─ get_object_describe    → K8s API
                                      └─ get_pod_logs           → K8s API
                                      │ (loops up to maxIter)
                                      ▼
                                 RCA State  ◄── browser polls 2 s
```

### Ownership detection rules

All detection uses Kubernetes naming conventions only (zero API calls):

| Child kind | Suffix pattern | Parent kind |
|------------|---------------|-------------|
| Pod | `-<5-char alphanum>` (from RS) | ReplicaSet |
| Pod | `-<5-char alphanum>` (from Job) | Job |
| Pod | `-<5-char alphanum>` (from DS) | DaemonSet |
| Pod | `-<digits only>` | StatefulSet |
| ReplicaSet | `-<6–12 char alphanum hash>` | Deployment |
| Job | `-<8–10 digit unix-minutes>` | CronJob |

### CRD support

`kindToGVR` first checks a static map of built-in types (fast path), then falls back to `client.Discovery().ServerGroupsAndResources()` which queries the API server for all installed CRDs. Results are cached in a `sync.Map`; the first request for an unknown kind pays the discovery cost, subsequent requests are instant.

---

## Dependencies

- `k8s.io/client-go` v0.35 — Kubernetes REST client
- `k8s.io/api` / `k8s.io/apimachinery` v0.35 — Kubernetes API types
- `sigs.k8s.io/yaml` — YAML marshalling

All HTML templates are embedded into the binary via `//go:embed templates/*.html`. No external frontend build step is required.
