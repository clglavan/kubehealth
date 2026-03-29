# KubeHealth

A lightweight Kubernetes event dashboard written in Go. Fetches events from your cluster and presents them as a navigable web UI — grouped by object, linked via ownership chains, with drill-down into YAML.

## Features

- **Namespace overview** — cards for every namespace showing warning/normal event counts and top objects
- **Ownership chain detection** — links related objects (Deployment → ReplicaSet → Pod, CronJob → Job → Pod, StatefulSet → Pod, DaemonSet → Pod) using naming conventions only; zero extra API calls
- **Cascade failure detection** — flags a chain red when the root and all leaf members have warnings
- **Sidebar + detail panel layout** — 260 px sticky sidebar with search and warnings-only filter; remaining viewport shows the selected object's full event timeline
- **YAML viewer** — click any object name to view its live YAML with a copy button
- **Rate-limit-safe fetching** — single background worker refreshes one namespace every 2 s, stalest-first; `QPS=5 / Burst=10` cap on the REST client
- **Loading screen** — animated spinner + skeleton tiles while the first data loads; live counter shows progress
- **Auto-refresh** — dashboard polls `/api/status` and reloads itself as namespaces populate

## Requirements

- Go 1.21+
- A valid `kubeconfig` (defaults to `~/.kube/config`)
- Read access to `events` in the target namespaces

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
| `-kubeconfig` | `~/.kube/config` | Path to kubeconfig file |
| `-namespaces` | *(all)* | Comma-separated list of namespaces to watch |

Then open `http://localhost:8080` in your browser.

## Pages

| Route | Description |
|-------|-------------|
| `/` | Namespace grid dashboard |
| `/namespace/{ns}` | Sidebar + detail panel for a single namespace |
| `/object/{ns}/{kind}/{name}` | YAML viewer for a specific object |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/status` | `{"loaded": N, "total": M, "ready": bool}` |
| `GET /api/events/{ns}` | Raw JSON event list for a namespace |
| `GET /api/refresh` | Triggers a UI reload (redirects to `/`) |

## Architecture

```
main.go
 ├─ runWorker()          — single goroutine, fetches one namespace every 2 s
 ├─ detectParent()       — naming-convention ownership detection (no API calls)
 ├─ buildFamilies()      — assembles ownership trees, marks cascade failures
 ├─ assembleDashboard()  — rebuilds global DashboardData from nsCache
 ├─ handleNamespace()    — serves namespace page (calls buildFamilies per request)
 └─ handleObject()       — fetches and renders live YAML

templates/
 ├─ loading.html         — animated loading screen, polls /api/status
 ├─ dashboard.html       — namespace card grid with live progress bar
 ├─ namespace.html       — sidebar + detail panel, ownership trees, event timelines
 └─ object.html          — YAML viewer with copy button
```

### Ownership detection rules

All detection is based purely on Kubernetes naming conventions:

| Child kind | Suffix pattern | Parent kind |
|------------|---------------|-------------|
| Pod | `-<5-char alphanum>` (from RS) | ReplicaSet |
| Pod | `-<5-char alphanum>` (from Job) | Job |
| Pod | `-<5-char alphanum>` (from DS) | DaemonSet |
| Pod | `-<digits only>` | StatefulSet |
| ReplicaSet | `-<6–12 char alphanum hash>` | Deployment |
| Job | `-<8–10 digit unix-minutes>` | CronJob |

### Caching

Each namespace gets its own `sync.RWMutex`-guarded cache entry. The worker goroutine selects the namespace with the oldest `lastFetch` time, fetches its events, updates the cache, and sleeps 2 s before the next iteration. The namespace list itself is re-resolved every 5 minutes. All HTTP handlers read from cache and never contact the API server directly.

## Dependencies

- `k8s.io/client-go` v0.35 — Kubernetes REST client
- `k8s.io/api` / `k8s.io/apimachinery` v0.35 — Kubernetes API types
- `sigs.k8s.io/yaml` — YAML marshalling for the object viewer

HTML templates are embedded into the binary via `//go:embed templates/*.html`.
