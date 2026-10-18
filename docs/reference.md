# KubeHealth reference

The details behind the [README](../README.md): how problems are built, what each button does, configuration, the API and the code layout.

- [The UI](#the-ui)
- [Actions on an object](#actions-on-an-object)
- [AI and RCA](#ai-and-rca)
- [Configuration](#configuration)
- [Building and releasing](#building-and-releasing)
- [API](#api)
- [Architecture](#architecture)

## The UI

![The overview: a brief, the AI note on the most urgent problem, and problems ranked by how new they are](images/01-overview.png)

- **Three panes** — the namespace rail on the left is always there. Its first item, **Overview**, shows the brief and the ranked problems. Pick a namespace and the middle pane lists its objects while the right pane shows the namespace's status; pick an object and the right pane shows that object. Moving between namespaces and objects swaps the panes in place without reloading the page, and the URL follows along (`/namespace/<ns>?focus=Kind/name`), so links, Back and Forward work. Below 1100 px wide the rail folds behind a button; below 760 px the panes stack.
- **Triage overview** — one row per failing workload rather than per event, ranked into **New** (first seen < 2 h ago), **Ongoing** (2–24 h) and **Chronic** (> 24 h). Rows show occurrences (the sum of each event's `count`), when the problem started and when it last fired. A row turns idle after 15 min without a new occurrence. The page updates in place every 3 s.

![Namespace status: live counts, what needs attention, its problems](images/02-namespace-status.png)

- **Namespace status** — the first entry in a namespace's object list. It shows a live snapshot (Deployments, StatefulSets and DaemonSets ready, CronJobs with failing or suspended ones, Jobs, Pods by phase and restarts, volume claims) and lists what needs attention: workloads not fully ready, CronJobs whose last run failed, pods grouped by why they are unhealthy. Below that are the namespace's problems from the overview, its most recent warnings, and the AI analysis button. The snapshot costs seven list calls when you open the namespace and is cached for 30 s (↻ Refresh fetches it again). The cluster-wide entry shows Nodes ready and PersistentVolumes by phase instead.
- **Owner grouping** — failing Pods, Jobs and ReplicaSets are grouped under their workload (CronJob, Deployment, DaemonSet, StatefulSet or an operator's CR). Names give a first guess for free, then a background resolver confirms one member per problem through `ownerReferences`, walking up to the top of the chain (at most two GETs per second, only while there is something to confirm; answers cached 30 min). For CronJobs it also reads `status.lastSuccessfulTime`, because each run's events expire after about an hour and would otherwise make a long-failing CronJob look new.
- **Patterns** — problems in different namespaces are merged into one row when they started within 10 s of each other, share a workload name and reason, or share an infrastructure reason (pull secrets, scheduling, mounts, …) in 3+ namespaces. Problems in different tiers are never merged.
- **Brief** — a few sentences above the problem list: where to start, what started in the last 2 h, which problems look linked, what is cluster-wide and what is old. They are computed from the problem list, not written by a model, so they are always accurate, appear instantly and work with AI turned off. Every problem they mention is a chip that jumps to its row.
- **AI note on the top problem** — the model reads the most urgent problem's events plus up to 30 log lines from its newest failing pod (stack frames dropped, error lines kept first, trimmed to a quarter of the loaded context) and suggests what to check first. The note names the model and the evidence it read, links to the full AI analysis, and is rewritten only when the top problem changes (after 30 s, so a flapping ranking doesn't keep the model busy). It never takes a model slot you need: if you start an AI analysis or RCA while it runs, it stops and retries later. The note is requested with `chat_template_kwargs: {"enable_thinking": false}`, so a thinking model (Qwen3) doesn't spend its 220 output tokens reasoning; any reasoning that still comes back is never shown as the note. Tested on the bundled 0.6B model, asking for a summary of several problems at once produced invented links between them every time, so the model is only ever given one problem and its own evidence.
- **Cluster-wide scope** — events about cluster-scoped objects (Nodes, PersistentVolumes) get their own row and their own page (`/namespace/_cluster`) instead of counting against `default`, where Kubernetes records them
- **Namespace rail** — searchable list (press `/`, Enter opens the first match) with active namespaces first, normal-only next, and quiet namespaces folded away. Each namespace shows its problem count and is highlighted while open.
- **Cluster switching** — the context button in the header lists every kubeconfig context, grouped by environment, and switches the watched cluster in place. Only the active cluster is polled, so API load doesn't grow with the number of clusters. Clusters you switch away from stay in memory: switching back is instant, and tabs still open on them keep working against that cluster (they show a banner saying their data stopped updating). KubeHealth never changes your kubeconfig's current-context.
- **Cluster identity** — every page shows the context and an environment tag read from its name (`prod`, `nprd`, `stg`, `dev`, `kind-…`; non-production words are checked first, so `nprd` isn't mistaken for `prd`). Production gets a red stripe across the top of every page. Names that don't say show `?`; pick the environment in ⚙ Settings and it's saved in `prefs.json` under your user config directory (`~/Library/Application Support/kubehealth/` on macOS). The header also shows how old the warning data is and the AI model's status.
- **Ownership chain detection** — links related objects (Deployment → ReplicaSet → Pod, CronJob → Job → Pod, StatefulSet → Pod, DaemonSet → Pod) using naming conventions; zero extra API calls
- **Cascade failure detection** — flags a chain red when the root and all leaf members have warnings
- **Object list + detail** — the object list has search and a warnings-only filter; the detail pane shows the full event timeline for the selected object. Repeated events show when they started (`×23 · since 11:01`) as well as when they last fired. The list uses the overview's marks: ▲ cascade, ● warning, ○ idle.
- **Deep links** — overview rows open `/namespace/<ns>?focus=Kind/name`, selecting that object (or, for a CronJob without events of its own, its newest failing Job). The row's RCA button adds `&rca=1&pod=<newest failing pod>` and starts the analysis on arrival; the AI note's link does the same with `&ai=1`.
- **Object describe** — click any object name to see live spec, status, conditions, and labels; supports built-in types and all CRDs via dynamic API discovery
- **Rate-limit-safe fetching** — single background worker makes at most one list call every 2 s. Every 10 s that call is one cluster-wide list of Warning events (`fieldSelector=type=Warning`, filtered by the API server); the other ticks refresh the stalest namespace's full event list. `QPS=5 / Burst=10` cap on the k8s REST client. When cluster-wide listing is forbidden, or `-namespaces` is set, warnings come from the namespace sweep alone.

| | |
|---|---|
| ![Cluster switcher](images/07-switcher.png) | ![A production cluster](images/08-prod.png) |
| ![Cluster-wide objects](images/06-cluster-wide.png) | ![Docs: how a feature works](images/09-docs.png) |

## Actions on an object

![An object's ownership chain and event timeline](images/03-object.png)

Every object in the namespace view shows its counts, then the same buttons in the same order: Logs (Pods only) · Correlate · RCA · ✦ AI · YAML.

| Button | What it does | K8s API? | LLM? |
|--------|-------------|----------|------|
| **Logs** | Fetches the last N container log lines. Tries the current run first, then `Previous=true` (covers CrashLoopBackOff). | ✅ | ❌ |
| **Correlate** | Scans the in-memory event cache across **all** namespaces for events whose timestamp falls within ±5 min of the selected object's events. | ❌ | ❌ |
| **RCA** | Multi-step agent loop — sends events + describe + pod logs to the LLM with 4 tools; model calls tools iteratively until confident, then produces a structured Root Cause + Resolution + Confidence. | ✅ | ✅ |
| **AI** | Single-shot analysis: streams events + describe + pod logs to the LLM and shows tokens as they arrive. For a workload it also sends the warnings of the objects it owns and its newest failing Pod's logs, like RCA. | ✅ | ✅ |
| **YAML** | Opens the object's live YAML in a panel that slides in from the left, coloured, with line numbers. `status`, `managedFields`, the last-applied annotation and ids/timestamps are hidden by default (chips toggle them; a hidden block shows as one line you can click to reveal). Any key folds; **Collapse all** leaves the top-level keys. **Copy** copies the YAML without the hidden fields, like `kubectl neat`. **Why it fails:** the fields the object's warnings and container state point at (an image that won't pull, a memory limit an OOM kill hit, a failing probe, a missing ConfigMap or Secret, a StorageClass that doesn't exist, a quota or LimitRange, a Job deadline…) are highlighted with the reason, listed at the top, and the panel scrolls to the first one. **References** (ConfigMaps, Secrets, PVCs, ServiceAccounts, Nodes, StorageClasses, PVs, owners, claimRef, roleRef, subjects …) are links: each opens that object in a new column to the right, so a chain like Deployment → ServiceAccount → its pull Secret stays on screen; an object already open to the left can't be opened again, and columns wider than the screen scroll sideways by dragging. **Secret values** never reach the page: they show masked, with a per-key Reveal that fetches and decodes one value. Click outside or press Esc to close (Esc closes the last column); Cmd/Ctrl-click opens the YAML as a page instead. | ✅ | ❌ |

Results are collapsible panels (expanded by default, except Correlate which is collapsed). AI and RCA results persist server-side — refreshing the page reconnects to a still-running analysis.

| | |
|---|---|
| ![Pod logs inline](images/04-logs.png) | ![Root cause analysis](images/05-rca.png) |
| ![YAML panel: the line that explains the failure](images/10-yaml-why.png) | ![YAML references open to the right](images/11-yaml-references.png) |
| ![YAML panel: Secrets masked](images/12-yaml-secrets.png) | |

## AI and RCA

- **Streaming responses** — regular AI uses `stream: true`; tokens update the UI every 300 ms via server-side cache polling
- **Streaming cursor** — a blinking `▋` appears while tokens are arriving; the panel style is identical before and after completion (no visual jump)
- **RCA agent loop** — up to N iterations (configurable); the model can call `get_pod_logs`, `get_object_describe`, `get_object_events`, and `get_correlated_events` as tools
- **Initial context** always includes events + object describe + pod logs (including previous-run logs for crashed pods). For a workload (Deployment, StatefulSet, DaemonSet, Job, CronJob), whose own events rarely say what is wrong, it also includes the warnings of the ReplicaSets, Jobs and Pods it owns (at most 12) and the logs of its newest failing Pod.
- **Context budget** — the first prompt (system prompt + evidence + tool definitions) uses at most 60% of the model's loaded context, when the server reports it (LM Studio's `/api/v0/models`, llama-server's `/v1/models`). Over that, the evidence is trimmed in this order: log lines (the newest and error-looking ones are kept), then member warnings, then the oldest events; the describe is never trimmed. The prompt says what was trimmed. The ✦ AI analysis of one object uses the same budget.
- **Linked evidence** — before asking the model, KubeHealth reads the objects the anchor's evidence clearly names and adds a short block about them after the logs of the RCA's first prompt and of the ✦ AI analysis (one hop: only what the anchor's events, member warnings, the Pods they name and the fetched logs name). A Pod waiting on a claim (FailedScheduling about volumes, FailedMount naming it, or Pending on an unbound claim) → the PVC (phase, StorageClass and whether it exists, size, access modes, its newest warnings); an unbound PVC anchor → its StorageClass. An error line (connection refused, no such host, i/o timeout, …) naming a Service by name, DNS name or cluster IP → its selector, ports, ready endpoints and, when none is ready, the Pods behind it (phase, readiness, container state, newest warning), the workload whose template matches, or the closest label values when the selector matches nothing; a targetPort its Pods don't declare. `configmap/secret "x" not found` or `couldn't find key` → whether it exists and its key names (never values). FailedScheduling about taints, affinity, selectors or Insufficient resources → up to six nodes (readiness, taints, allocatable cpu and memory, the labels the Pod selects on). A refused image pull → the Pod's and its ServiceAccount's imagePullSecrets and whether each exists. Each object once, never the anchor; at most 15% of the loaded context, the least important lines trimmed first (the block says how many). The RCA trail lists every linked object as an `initial ctx` step. Off with `AI_LINK_FOLLOW=false` or in ⚙ Settings. The exact format is in `linked.go`.
- **Describe for the model** — the describe shows only what is set, in a fixed layout: labels and annotations (without `last-applied-configuration` and values over 200 characters); for Pods and every workload's pod template the service account, node selector, tolerations (without the two default not-ready/unreachable ones), affinity types, priority class, and per container (init containers first) image, requests/limits, command/args, ports, probes (kubectl style), `envFrom` and `valueFrom` references, mounts and security settings; volumes; selectors and volume claim templates for workloads; selector and EndpointSlice endpoints for Services; claim/volume/node/HPA/Ingress summaries; key names of ConfigMaps and Secrets; `status.conditions` of custom resources. Secret and ConfigMap values and literal env values are never shown.
- **Init containers** — a failing init container (waiting on anything but its turn, or last ended non-zero) is named `<name> (init)` in the container state line and its logs come first, under `=== init container: <name> ===`.
- **Reasoning stays out of the answer** — reasoning streamed as `reasoning_content` (llama-server, LM Studio) is kept apart: the panel shows "Thinking…" meanwhile and the answer alone afterwards; the insight's `reasoning` field keeps it.
- **Container state** — above the logs, one line per container says how often it restarted, how its last run ended and what it is waiting on (`exit code 137, SIGKILL: usually out of memory, memory limit 48Mi`). An OOMKilled container rarely logs anything about it, so without this line the model can't tell. The overview's AI note gets the same line.
- **Low temperature** — analysis runs at 0.3 and RCA at 0.2 (servers default to about 0.8): both read evidence, and a small model wanders off it at higher temperatures.
- **Formatting** — results render `**Label**:` sections, bullet and numbered lists, inline code and fenced code blocks
- **Context overflow detection** — if LM Studio returns a 400 with the "Trying to keep the first N tokens" message, or llama-server an `exceed_context_size_error` ("exceeds the available context size"), the UI shows a clear "increase your context length" guide instead of hanging
- **No answer format** — analysis and RCA ask a plain question (what is wrong, the evidence, the fix, how sure) and show the answer as the model writes it. Nothing reads the answer's structure, and a small model's effort is better spent on the diagnosis than on matching headings, so there are no required sections and no rewrite requests.
- **Token estimate** — prompt budgets use a per-character-class estimate (digits and punctuation cost more than letters), fitted on real kubehealth prompts for Qwen-family tokenizers: it never undercounts by more than ~4% (the old characters/3.5 undercounted log- and IP-heavy prompts by up to 2.4×).
- **Concurrent call limit** — configurable max simultaneous requests to LM Studio; excess requests are rejected immediately. The overview's AI note only uses a free slot and hands it over when you start an analysis.
- **Page-refresh safe** — all analysis goroutines run against `context.Background()` and survive browser navigation
- **Cluster-safe** — an analysis keeps the cluster it was started on, even if you switch while it runs, and results are cached per cluster, so `default` on one cluster never shows another cluster's analysis

### Settings (⚙)
| Setting | Effect |
|---------|--------|
| Log Lines | Lines fetched by Logs button and included in AI / RCA context |
| LLM Timeout | Minutes before giving up — large objects like Elasticsearch need 5–10 min for prompt processing |
| Max Concurrent Calls | Prevents overloading a single-GPU local model |
| RCA Max Iterations | Tool-calling rounds before the agent is forced to conclude |
| Linked evidence | Follow what the evidence names before asking the model (see Linked evidence above); default from `AI_LINK_FOLLOW` |

### AI Agent (✦)
- Edit the **system prompt** that is injected as the `system` role message on every AI analysis and RCA call. The overview's AI note uses its own stricter prompt (use only the given facts and log lines, don't guess causes), because the analysis prompt asks for root causes and tempts a small model to invent one.
- Live **context window estimate** — shows system prompt + events + logs + tool definitions as % of `loaded_context_length` (fetched from LM Studio's `/api/v0/models`)
- **Response format** is hardcoded in the user message (Summary / Issues / Actions) and cannot be broken by the system prompt
- **Reset to default** restores the initial system prompt

### Docs
Pick a feature to see its path through the system (browser, server, per-cluster state, Kubernetes, the model) as numbered steps, with what it costs the API and the model.

## Configuration

### Flags
| Flag | Default | Description |
|------|---------|-------------|
| `-port` | `8080` | HTTP listen port |
| `-kubeconfig` | `~/.kube/config` | Path to kubeconfig |
| `-namespaces` | *(all)* | Comma-separated namespace list |
| `-version` | | Print the version and exit |

### Environment variables
All AI settings can be overridden at startup:

| Variable | Default | Description |
|----------|---------|-------------|
| `LMSTUDIO_URL` | `http://localhost:1234/v1` | LM Studio OpenAI-compat base URL |
| `LMSTUDIO_MODEL` | `qwen/qwen3-4b-2507` | Model identifier |
| `AI_ENABLED` | `true` | Set to `false` or `0` to disable AI features |
| `AI_SYSTEM_PROMPT` | *(built-in SRE prompt)* | Default system prompt |
| `AI_LINK_FOLLOW` | `true` | Set to `false` or `0` to leave out the linked evidence (see AI and RCA) |
| `KUBECONFIG` | `~/.kube/config` | Path to kubeconfig; a colon-separated list of files is merged as kubectl does |

Runtime settings (log lines, timeout, max calls, RCA iterations, linked evidence) are configured in the UI under ⚙ Settings and take effect immediately without restart.

### LM Studio setup

1. Download and install LM Studio
2. Load a model — **qwen/qwen3-4b-2507** is the tested default
3. Start the local server (default: `http://localhost:1234`)
4. For large objects (Elasticsearch, etc.) increase the model's **Context Length** via the gear icon → Context Length → Reload — 8192 or 16384 tokens recommended

## Building and releasing

`make release` cross-compiles the regular build for macOS, Linux and Windows into `dist/`, and `make release-embed` packages the embedded build for the machine you run it on. Both stamp the version from `git describe` (see `kubehealth -version`).

`make embed` is shorthand for `make fetch-assets && make build-embed`. The fetch step pulls the GGUF from [HuggingFace](https://huggingface.co/clglavan/magos-k8s-0.6b) (override with `MODEL_URL=...` for a different quantization, or `MODEL_SRC=/path/to.gguf` to use a local file) and downloads a matching `llama-server` from [llama.cpp releases](https://github.com/ggerganov/llama.cpp/releases). Both go into `embedded/` (gitignored) and get pulled in via `//go:embed`.

The embedded binary extracts its bundled assets to `~/Library/Caches/kubehealth/embedded/` on first run, spawns `llama-server` on a free local port, and points the rest of the app at it. Setting `LMSTUDIO_URL` skips the embedded server and falls back to the regular flow.

## Benchmark

`kubehealth bench` measures the model behind the AI features on frozen incidents, with no cluster. It replays recorded cluster state through KubeHealth's real code paths: the same prompts, context budget, tools and log fetching as the UI.

```
kubehealth bench record  -kubeconfig demo/kubeconfig -context shop-staging -out bench/snapshots/shop-staging
kubehealth bench run     -cases bench/cases -llm http://127.0.0.1:8080/v1 -model base -out results/base.json
kubehealth bench run     -cases bench/cases -llm http://127.0.0.1:8080/v1 -model base -seeds 1,2,3 -out results/base.json
kubehealth bench run     -cases bench/cases -llm http://127.0.0.1:8080/v1 -model base -link-follow=false -out results/base-off.json
kubehealth bench score   -results results/base.json,results/v2.json [-cases bench/cases] -out scorecard.md
kubehealth bench inspect -snapshot bench/snapshots/shop-staging -cases bench/cases
```

- **record** lists every object the AI paths read (Pods, workloads, ReplicaSets, Jobs, CronJobs, Services, EndpointSlices, PVCs, ConfigMaps, Secrets, HPAs, Ingresses, ServiceAccounts, Events; Nodes, PVs, StorageClasses, Namespaces) in every namespace except the system ones (`-skip-namespaces`), or only in the namespaces `-namespaces` lists (so one cluster can hold several snapshots, each with its own overview note), plus each container's current and previous logs (init containers too, `-tail` lines, as KubeHealth fetches them) and API discovery. It never stores values: ConfigMaps and Secrets keep their key names only, literal env vars lose their values, last-applied annotations and managedFields are dropped. `snapshot.yaml` holds the recording time.
- **run** builds each snapshot as an in-memory cluster (client-go fakes) replayed at `-at`: every timestamp moves by (replay time − recording time), so every age is what it was, and the clock stays still for the whole snapshot. The default, `-at recorded`, replays at the recording time, so the prompts are the same on every run; `-at now` (or an RFC 3339 time) shifts everything, which also moves the absolute times KubeHealth prints (a CronJob's Last Schedule, a Job's Start Time). It then runs the overview note (as Regenerate does), and for every case the ✦ AI analysis and the RCA agent on the anchor, one at a time, against any OpenAI-compatible server. `-seed` (default 1234) and `"cache_prompt": false` (`-no-prompt-cache`, default on) are added to every chat request, so llama-server samples the same way on every run (its answers change once part of a prompt comes from its cache). The results file keeps, per item, the final text, every request and answer and the latency; `run` ends with its scorecard. `-seeds 1,2,3` runs every item once per seed (each run's requests carry its seed) and keeps every run in the results file (`seeds`, and `runs`: one `{seed, snapshots, notes, cases}` per seed); with one seed, the default, the results file keeps its items at the top level as before.
- **score** compares results files on what the model diagnosed, read from the WHOLE answer (never its layout): rule-based keyword checks — RCA right root cause / finds the cause in another object (`deep` on `needs_tool` cases) / names the decisive detail (`deep` on the other cases) / fix (the fix also needs an action: create, set, roll back, kubectl …) / finished / called a tool when the cause was elsewhere (`needs_tool` cases whose RCA made at least one tool call; the logs KubeHealth puts in the first prompt don't count) / tool calls per case (the average over all fault cases, not a share), analysis right issue / action, healthy objects get no invented fault, the note names the right check and is not thinking. `-cases bench/cases` scores with the current case files instead of the copies the results file kept (same ids, snapshots and anchors), so keyword or `needs_tool` fixes apply to earlier runs. A results file with several seeds scores every check per seed and shows the mean, `±` half the range of the per-seed scores, and n per seed.
- **inspect** shows how KubeHealth ranks a snapshot's problems and, with `-cases`, whether each anchor exists, has events, member warnings and logs, and which objects link-following adds (`rule:Kind/name`; `-linked` prints the block).
- **-link-follow** (`run`, default true) turns the linked evidence on or off for the run, so the same model can be measured with and without it. The results file keeps the setting (`link_follow`; the scorecard column reads `<model> (links on|off)`) and, per case, which objects the RCA's first prompt listed (`rca.linked`, `rca.linked_trimmed`).

### Cases

One file per case, `bench/cases/<id>.yaml`:

```yaml
id: orders-worker-cascade                # = the file name
snapshot: shop-staging                   # a directory in bench/snapshots
anchor: {ns: orders, kind: Deployment, name: orders-worker}   # ns _cluster for cluster-scoped objects
truth: "worker can't resolve orders-db, which never started: its PVC's StorageClass is missing"
cause: [["orders-db"], ["no such host", "dns", "lookup"]]      # every group needs one keyword
deep: [["fast-ssd", "storageclass", "pvc"]]                    # optional: see needs_tool
fix: [["storageclass", "fast-ssd", "volume"]]                  # the target of the fix (the scorer also wants an action word)
needs_tool: true                         # the decisive cause is in another object, not in KubeHealth's first RCA prompt:
                                         #   deep = what only that object's evidence shows (required). Without it,
                                         #   deep = the decisive detail of a cause the anchor's own evidence shows (a tag, a limit)
healthy: false                           # true: the right answer is that nothing is broken (no cause/fix)
note_target: false                       # true for at most one case per snapshot: the problem the note should be about
source: kubehealth demo (demo/setup.sh)
license: …
```

Keywords are case-insensitive substrings: pick words only a right answer contains, and avoid words the anchor's own events already print where you can. Mind what the first prompt holds: a workload's logs come from its newest Pod with a Warning event, so a Job or CronJob whose Pods just exit (restartPolicy Never, no BackOff) gets no logs at all, and a cause that only those logs show is a `needs_tool` case.

### Incidents

`bench/snapshots/bench-a` … `bench-d` were recorded from one kind cluster, `kh-bench`, built by `bench/incidents/setup.sh`: one directory per case with its manifest (`bench/incidents/<id>/manifest.yaml`) and, for the fake applications, their busybox scripts (`app/<binary>`, baked into local images so describe shows ordinary images and arguments). The snapshots are namespace groups: bench-a = `tidepool`, `tidepool-ingest`; bench-b = `larkspur-books`, `larkspur-notify`; bench-c = `quillmoss-render`, `quillmoss-sandbox`; bench-d = `quillmoss-ops`; each also holds `default`, where cluster-scoped objects' events live. 30 faults (13 with `needs_tool`: the decisive evidence is a dependency's missing Secret, StorageClass, access mode, Service selector or targetPort, image, LimitRange, CPU request or nodeSelector, a Node's taint, or a failed Job's Pod logs) and 12 healthy objects, some next to harmless noise (a rollout restart, a scale-down, a Job that retried). setup.sh starts the second namespace of every group 150 s after the first, because KubeHealth links problems in different namespaces that start within 10 s of each other into one pattern row, and the note is about the top row. Record within the hour (Kubernetes keeps events for an hour), once crash loops have restarted a few times and the CronJobs have run twice (about 8 minutes).

To add a case, record a snapshot of a cluster that shows the incident (or reuse one), check it with `bench inspect -cases`, write the case file with keywords that only a right answer contains, and run it. A snapshot is plain YAML and log files: review it before committing (no values are stored, but names, images and log lines are).

## API

### Data
| Endpoint | Description |
|----------|-------------|
| `GET /api/home` | Overview data: ranked problems, patterns, brief, rail counts, data age, model status |
| `GET /api/snapshot/{ns}[?refresh=1]` | Live namespace snapshot (`_cluster` for Nodes and PVs), cached 30 s |
| `GET /fragment/namespace/{ns}` | The namespace's object list and detail panes as HTML, swapped in by the page |
| `GET /api/status` | `{"loaded": N, "total": M, "ready": bool}` — namespace loading progress |
| `GET /api/events` | Full dashboard data JSON |
| `GET /api/object/{ns}/{kind}/{name}` | Live object describe (built-in types and CRDs) |
| `GET /api/yaml/{ns}/{kind}/{name}[?reveal=key]` | The YAML panel: the object as YAML with Secret values masked, the masked keys, and the fields its warnings and container state point at (`_cluster` for cluster-scoped objects; 404 when it no longer exists). `?reveal=<key>` returns one Secret value, decoded |

### Clusters
| Endpoint | Description |
|----------|-------------|
| `GET /api/contexts` | Kubeconfig contexts with environment tags; re-read on every call |
| `POST /api/context` | Switch the watched cluster: `{"name": "..."}` |
| `POST /api/context/env` | Set a context's environment: `{"name": "...", "env": "prod"}` (`""` reads it from the name) |

Every page sends `X-KubeHealth-Context` with its API calls, so a tab keeps talking to the cluster it was opened on after you switch elsewhere.

### AI
| Endpoint | Description |
|----------|-------------|
| `POST /api/ai/analyze/{ns\|object}/{…}` | Trigger AI analysis for a namespace or object |
| `GET /api/ai/insights/{ns\|object}/{…}` | Poll AI result (`partial`, `content`, `error`) |
| `POST /api/ai/brief` | Rewrite the overview's AI note now |
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
| `POST /api/settings` | Update log lines, timeout, max calls, RCA iterations, `link_follow` |

## Architecture

```
main.go          — HTTP server, routing, template rendering, per-cluster worker
cluster.go       — Per-context cluster state, switching, environment tags, saved preferences
ai.go            — AI streaming (callLMStudio, streamLMStudio), insight cache
ai_config.go     — System prompt store, LM Studio model-info fetch (/api/v0/models)
rca.go           — RCA agent loop, tool dispatcher, RCA state store, first-prompt evidence and budget
linked.go        — Link-following: the objects the evidence names, fetched for the first prompts
problems.go      — Overview problem model: owner groups, tiers, cascades, patterns, /api/home, /api/status
brief.go         — The brief: computed facts plus the AI note on the top problem, /api/ai/brief
owners.go        — Owner inference from names + background ownerReferences resolver
warnings.go      — Cluster-wide Warning event feed, merged into every namespace's events
snapshot.go      — Live namespace snapshot (workloads, pods, volume claims), cached 30 s, /api/snapshot
correlate.go     — Cross-namespace event correlation (pure cache read)
logs.go          — Pod log fetching with Previous-run fallback
settings.go      — Runtime settings (log lines, timeout, max calls, RCA iterations)

rca_test.go, problems_test.go, brief_test.go, logs_test.go, linked_test.go — unit tests (go test ./...)
demo/            — kind clusters with realistic failures, for trying KubeHealth locally

templates/
├─ header.html         — Shared header: cluster switcher, data age, model status, stale-tab banner
├─ app.html            — The page shell: namespace rail, overview (brief, ranked problems), in-place navigation, live updates from /api/home
├─ ns_view.html        — Namespace panes: object list, status panel, detail panels, all action buttons, JS agent functions
├─ ai_modal.html       — AI Agent config modal (system prompt, context window estimate)
├─ settings_modal.html — Settings modal (log lines, timeouts, concurrency, RCA iterations)
├─ docs_modal.html     — Interactive architecture diagram with animated data flows
├─ yaml_drawer.html    — YAML panel: columns, colouring, folding, hiding noisy fields, references, why-it-fails highlights, copy
└─ object.html         — YAML as a page (the panel's ↗, or Cmd/Ctrl-click on YAML)
```

### Data flow summary

```
Kubernetes API (the active context; each context has its own caches below)
    │  Worker, one list call every 2 s:
    │    every 10 s → cluster-wide Warning events  ──► warnFeed
    │    otherwise  → stalest namespace, all events ──► nsCache
    ▼
namespaceEvents() = nsCache merged with warnFeed  ◄──── feeds everything below
    │
    ├── Home (/api/home)  ──► problems.go groups warnings by owner
    │                           ├──► owners.go confirms owners (≤ 2 GET/s, cached)
    │                           └──► brief.go: facts + AI note on the top problem
    │                                (events + pod log lines → LM Studio, free slot only)
    │
    ├── Namespace view  (object list from the merged cache, read-only)
    │       └──► snapshot.go: 7 list calls on open, cached 30 s
    │
    ├── Correlate  ──► scans the merged cache, zero K8s API call, zero LLM
    │
    ├── Logs  ──► K8s GetLogs (current + Previous fallback)
    │
    ├── AI Analysis  ──► merged cache + K8s describe + K8s logs
    │                        │
    │                        └──► LM Studio (stream=true, SSE)
    │                                 │ tokens every 300 ms
    │                                 ▼
    │                          aiInsightCache  ◄── browser polls 500 ms
    │
    └── RCA Agent  ──► merged cache + K8s describe + K8s logs (initial context)
                            │
                            └──► LM Studio (stream=false, with tools)
                                      │ tool_calls response
                                      ├─ get_object_events      → merged cache
                                      ├─ get_correlated_events  → merged cache
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

## Dependencies

- `k8s.io/client-go` v0.35 — Kubernetes REST client
- `k8s.io/api` / `k8s.io/apimachinery` v0.35 — Kubernetes API types
- `sigs.k8s.io/yaml` — YAML marshalling

All HTML templates are embedded into the binary via `//go:embed templates/*.html`. No external frontend build step is required.
