package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ── State ─────────────────────────────────────────────────────────────────────

type RCAStep struct {
	Iter    int    `json:"iter"`
	Tool    string `json:"tool"`
	Label   string `json:"label"`
	Status  string `json:"status"`            // "running" | "done" | "error" | "unavailable"
	Initial bool   `json:"initial,omitempty"` // true for steps baked into the initial context
}

type RCAState struct {
	sync.RWMutex
	Status    string    `json:"status"` // "running" | "done" | "error"
	Iteration int       `json:"iteration"`
	MaxIter   int       `json:"max_iter"`
	Steps     []RCAStep `json:"steps"`
	Result    string    `json:"result"`
	Error     string    `json:"error,omitempty"`
	ErrorType string    `json:"error_type,omitempty"` // "context_overflow" | ""
	UpdatedAt string    `json:"updated_at"`
	// Linked names the objects the linked evidence in the first prompt lists,
	// as rule:Kind/name (linked.go); LinkedTrimmed is how many of its lines
	// were trimmed to fit.
	Linked        []string `json:"linked,omitempty"`
	LinkedTrimmed int      `json:"linked_trimmed,omitempty"`
}

var (
	rcaCache   sync.Map // rcaKey → *RCAState
	rcaRunning sync.Map // rcaKey → struct{}
)

// rcaCacheKey is scoped to the cluster: the same namespace/kind/name can exist
// in several clusters.
func rcaCacheKey(cl *clusterState, ns, kind, name string) string {
	return cl.name + "|" + ns + "/" + kind + "/" + name
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

func handleAPIRCAStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !aiConfig.enabled {
		http.Error(w, "AI not enabled", http.StatusServiceUnavailable)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/api/rca/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		http.Error(w, "expected /api/rca/{namespace}/{kind}/{name}", http.StatusBadRequest)
		return
	}
	ns, kind, name := parts[0], parts[1], parts[2]
	cl := apiCluster(w, r)
	if cl == nil {
		return
	}
	key := rcaCacheKey(cl, ns, kind, name)

	if _, loaded := rcaRunning.LoadOrStore(key, struct{}{}); loaded {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "already_running"})
		return
	}

	maxIter := getRCAMaxIter()
	state := &RCAState{Status: "running", MaxIter: maxIter}
	rcaCache.Store(key, state)

	go func() {
		defer rcaRunning.Delete(key)
		runRCAAgent(cl, ns, kind, name, state)
	}()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func handleAPIRCAStatus(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/rca/status/")
	parts := strings.SplitN(path, "/", 3)
	if len(parts) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	cl := apiCluster(w, r)
	if cl == nil {
		return
	}
	key := rcaCacheKey(cl, parts[0], parts[1], parts[2])
	w.Header().Set("Content-Type", "application/json")
	if v, ok := rcaCache.Load(key); ok {
		state := v.(*RCAState)
		state.RLock()
		defer state.RUnlock()
		json.NewEncoder(w).Encode(state)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "not_found"})
}

// ── RCA system prompt ─────────────────────────────────────────────────────────

const rcaSystemPromptSuffix = `

You are now performing a Root Cause Analysis (RCA).

The initial context already contains the anchor object's events and describe. If the anchor is a Pod, it also contains the Pod's logs: you do NOT need to call get_pod_logs for it. If the anchor is a workload (Deployment, StatefulSet, DaemonSet, Job, CronJob), it also contains the warnings of the ReplicaSets, Jobs and Pods it owns and the logs of its newest failing Pod; read those first, they usually explain the failure.

You have tools to gather additional evidence. Use them SELECTIVELY:
- get_correlated_events: call this when you suspect cluster-wide or cross-namespace issues (node pressure, network problems, shared infrastructure failures). Do this early.
- get_pod_logs: The anchor pod's logs are already in the initial context (including previous-run logs for crash loops). If the initial context says "no output found / FailedScheduling", do NOT call get_pod_logs for the anchor — there is nothing to fetch. DO call it for any correlated Pod that has Warning or Error events.
- get_object_describe: call this to inspect resource limits, image, node assignment, replicas, or conditions of any object that looks suspicious.
- get_object_events: call this to get the full event history of a specific correlated object.

Stop calling tools as soon as you have sufficient evidence. Do not call tools redundantly.

When you have enough evidence, stop calling tools and answer in short plain text: what is wrong (the specific cause), the evidence that shows it, how to fix it, and how sure you are.`

// ── Tool definitions ──────────────────────────────────────────────────────────

func rcaToolDefs() []rcaTool {
	mk := func(name, desc string, props map[string]rcaToolProp, req []string) rcaTool {
		t := rcaTool{Type: "function"}
		t.Function.Name = name
		t.Function.Description = desc
		t.Function.Parameters = rcaToolParam{Type: "object", Properties: props, Required: req}
		return t
	}
	s := func(desc string) rcaToolProp { return rcaToolProp{Type: "string", Description: desc} }
	return []rcaTool{
		mk("get_pod_logs",
			"Fetch the last N lines of logs from a pod's containers. Use when a pod is crashing, restarting, or shows OOMKilled.",
			map[string]rcaToolProp{"namespace": s("Kubernetes namespace"), "pod_name": s("Pod name")},
			[]string{"namespace", "pod_name"},
		),
		mk("get_object_describe",
			"Get spec, status, and conditions of any Kubernetes object. Use to check resource limits, image, node, replicas, and current conditions.",
			map[string]rcaToolProp{
				"namespace": s("Namespace (empty for cluster-scoped resources)"),
				"kind":      s("Resource kind, e.g. Pod, Deployment, Node, Service"),
				"name":      s("Resource name"),
			},
			[]string{"namespace", "kind", "name"},
		),
		mk("get_object_events",
			"Get cached Kubernetes events for a specific object. Use to inspect a correlated object's event history.",
			map[string]rcaToolProp{
				"namespace": s("Kubernetes namespace"),
				"kind":      s("Resource kind"),
				"name":      s("Resource name"),
			},
			[]string{"namespace", "kind", "name"},
		),
		mk("get_correlated_events",
			"Find events across ALL namespaces in the same time window as the anchor object. Use when suspecting cluster-level issues.",
			map[string]rcaToolProp{
				"namespace": s("Namespace of the anchor object"),
				"kind":      s("Kind of the anchor object"),
				"name":      s("Name of the anchor object"),
			},
			[]string{"namespace", "kind", "name"},
		),
	}
}

// ── Tool dispatcher ───────────────────────────────────────────────────────────

func rcaStepLabel(toolName string, args map[string]string) string {
	get := func(k string) string { return args[k] }
	switch toolName {
	case "get_pod_logs":
		return get("namespace") + " / " + get("pod_name")
	case "get_object_describe", "get_object_events":
		return get("namespace") + " / " + get("kind") + " / " + get("name")
	case "get_correlated_events":
		return get("namespace") + " / " + get("kind") + " / " + get("name") + " (cross-ns)"
	}
	return toolName
}

func executeRCATool(cl *clusterState, tc rcaToolCall, anchorNS string) string {
	var args map[string]string
	if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
		return fmt.Sprintf("error parsing tool arguments: %v", err)
	}
	get := func(k string) string {
		if v := args[k]; v != "" {
			return v
		}
		return ""
	}

	switch tc.Function.Name {

	case "get_pod_logs":
		ns, pod := get("namespace"), get("pod_name")
		if ns == "" {
			ns = anchorNS
		}
		logs, err := fetchLogsForAI(cl, ns, pod)
		if err != nil {
			return fmt.Sprintf("error fetching logs for %s/%s: %v", ns, pod, err)
		}
		if strings.TrimSpace(logs) == "" {
			return fmt.Sprintf("no logs available for pod %s/%s", ns, pod)
		}
		return fmt.Sprintf("Pod logs for %s/%s (last %d lines):\n%s", ns, pod, getLogTailLines(), logs)

	case "get_object_describe":
		ns, kind, name := get("namespace"), get("kind"), get("name")
		info := cl.fetchObjectInfo(ns, kind, name)
		if info.NotFound {
			return fmt.Sprintf("%s/%s/%s no longer exists in the cluster", ns, kind, name)
		}
		if info.Error != "" {
			return fmt.Sprintf("error describing %s/%s/%s: %s", ns, kind, name, info.Error)
		}
		return rcaFormatDescribe(info)

	case "get_object_events":
		ns, kind, name := get("namespace"), get("kind"), get("name")
		var events []EventSummary
		for _, ev := range cl.scopeEvents(ns) {
			if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
				events = append(events, ev)
			}
		}
		if len(events) == 0 {
			return fmt.Sprintf("no cached events for %s/%s/%s", ns, kind, name)
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "Events for %s/%s/%s (%d total):\n", ns, kind, name, len(events))
		for _, ev := range events {
			fmt.Fprintf(&sb, "- [%s] %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
		return sb.String()

	case "get_correlated_events":
		ns, kind, name := get("namespace"), get("kind"), get("name")
		cr := buildCorrelateResponse(cl, ns, kind, name)
		if len(cr.Correlated) == 0 {
			return "no correlated events found in the time window"
		}
		var sb strings.Builder
		fmt.Fprintf(&sb, "Correlated events (±%d min, %d events across namespaces):\n",
			cr.BufferMin, len(cr.Correlated))
		prevNS := ""
		for _, ev := range cr.Correlated {
			if ev.Namespace != prevNS {
				fmt.Fprintf(&sb, "\n[%s]\n", ev.Namespace)
				prevNS = ev.Namespace
			}
			fmt.Fprintf(&sb, "- [%s] %s/%s  %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Kind, ev.ObjectName, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
		return sb.String()
	}

	return fmt.Sprintf("unknown tool: %s", tc.Function.Name)
}

// rcaFormatDescribe converts ObjectInfo into a concise prompt-friendly string.
func rcaFormatDescribe(info ObjectInfo) string {
	return fmt.Sprintf("Describe %s/%s/%s:\n", info.Namespace, info.Kind, info.Name) + describeBody(info)
}

// ── Initial context ───────────────────────────────────────────────────────────

// rcaInitCtx carries the assembled initial prompt plus metadata about what was gathered.
type rcaInitCtx struct {
	text      string
	logsPod   string // the Pod whose logs were read: the anchor itself, or a workload's newest failing Pod
	podLogs   string // "" = not a pod or fetch returned nothing
	podLogErr error  // non-nil = hard fetch error

	linked        []linkedItem // linked evidence the prompt lists
	linkedTrimmed int
}

// rcaMemberWarnings caps how many of a workload's member warnings go into the
// first prompt, so a small model's context still has room for logs and tools.
const rcaMemberWarnings = 12

// promptShare is the percentage of the loaded context a first prompt (system
// prompt, evidence and tool definitions) may use; the rest is left for tool
// results and the answer.
const promptShare = 60

// promptBudget is how many tokens a first prompt's evidence may use once the
// overhead (system prompt, tool definitions) is paid for, or 0 when the
// model's context length is unknown and nothing is trimmed.
func promptBudget(ctxLen, overhead int) int {
	if ctxLen <= 0 {
		return 0
	}
	return max(ctxLen*promptShare/100-overhead, 1)
}

// modelCtxLen is the loaded context length, or 0 when the server doesn't say.
func modelCtxLen() int {
	if mi := fetchLMModelInfo(); mi != nil {
		return mi.ctxLen()
	}
	return 0
}

// ownedBy reports whether an owner chain (object first, root last) passes
// through kind/name above the object itself.
func ownedBy(chain []objRef, kind, name string) bool {
	for _, r := range chain[1:] {
		if strings.EqualFold(r.Kind, kind) && r.Name == name {
			return true
		}
	}
	return false
}

// memberEvidence collects the warnings of the objects a workload owns
// (ReplicaSets, Jobs, Pods) and its failing Pods, newest first. A workload's
// own events rarely say what is wrong ("ScalingReplicaSet"); its Pods' do.
func memberEvidence(cl *clusterState, ns, kind, name string) (lines []string, pods []string) {
	evs, pods := memberWarnings(cl, ns, kind, name)
	for _, ev := range evs {
		lines = append(lines, memberLine(ev))
	}
	return lines, pods
}

// memberLine is how a member warning is printed.
func memberLine(ev EventSummary) string {
	return fmt.Sprintf("- %s %s %s: %s  (×%d, %s ago)", ev.Kind, ev.ObjectName, ev.Reason, ev.Message, ev.Count, ev.Age)
}

// memberWarnings are the member warnings memberEvidence prints (at most
// rcaMemberWarnings, one per object and reason) and the failing Pods.
func memberWarnings(cl *clusterState, ns, kind, name string) (evs []EventSummary, pods []string) {
	seen := map[string]bool{}
	podLast := map[string]time.Time{}
	for _, ev := range cl.scopeEvents(ns) {
		if ev.Type != "Warning" || ev.ObjectName == "" || (strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name) {
			continue
		}
		chain, _ := cl.ownerChain(ns, objRef{ev.Kind, ev.ObjectName})
		if !ownedBy(chain, kind, name) {
			continue
		}
		key := ev.Kind + "/" + ev.ObjectName + "/" + ev.Reason
		if !seen[key] && len(evs) < rcaMemberWarnings {
			seen[key] = true
			evs = append(evs, ev)
		}
		if ev.Kind == "Pod" && ev.LastTime.After(podLast[ev.ObjectName]) {
			podLast[ev.ObjectName] = ev.LastTime
		}
	}
	for p := range podLast {
		pods = append(pods, p)
	}
	// Newest first; Pods last seen in the same second go by name, so the same
	// events always pick the same Pod.
	sort.Slice(pods, func(i, j int) bool {
		if !podLast[pods[i]].Equal(podLast[pods[j]]) {
			return podLast[pods[i]].After(podLast[pods[j]])
		}
		return pods[i] < pods[j]
	})
	return evs, pods
}

// objectEvents returns the cached events about one object.
func objectEvents(cl *clusterState, ns, kind, name string) []EventSummary {
	var events []EventSummary
	for _, ev := range cl.scopeEvents(ns) {
		if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
			events = append(events, ev)
		}
	}
	return events
}

// newestEvents keeps the n most recently seen events, in their order.
func newestEvents(evs []EventSummary, n int) []EventSummary {
	if n >= len(evs) {
		return evs
	}
	byAge := make([]int, len(evs))
	for i := range byAge {
		byAge[i] = i
	}
	sort.SliceStable(byAge, func(a, b int) bool { return evs[byAge[a]].LastTime.After(evs[byAge[b]].LastTime) })
	keep := map[int]bool{}
	for _, i := range byAge[:n] {
		keep[i] = true
	}
	var out []EventSummary
	for i, ev := range evs {
		if keep[i] {
			out = append(out, ev)
		}
	}
	return out
}

// evidence is an object's evidence for a first prompt (RCA or ✦ AI
// analysis), kept in parts so it can be trimmed to fit the model's context.
type evidence struct {
	head     string         // what to analyse; never trimmed
	events   []EventSummary // the object's own events
	describe string         // never trimmed
	kind     string         // the workload's kind, for the members heading
	members  []string       // warnings on the objects a workload owns
	logsHead string         // heading over logs, e.g. "\nPod logs (last 150 lines):\n"
	logs     string         // as fetchLogsForAI returns them
	note     string         // instead of logs, e.g. why there are none; never trimmed
	tail     string         // instructions; never trimmed

	memberEvents  []EventSummary // the member warnings, as events (for link-following)
	linked        string         // the linked-evidence block (linked.go); has its own budget, never trimmed here
	linkedItems   []linkedItem   // what it lists
	linkedTrimmed int            // lines it trimmed
}

// addWorkload adds what a workload's members say: the warnings on the
// objects it owns and the logs of its newest failing Pod (a Pod that has
// since been deleted has none, so the next is tried). It returns that Pod.
func (e *evidence) addWorkload(cl *clusterState, ns, kind, name string) (logsPod string) {
	evs, pods := memberWarnings(cl, ns, kind, name)
	e.kind, e.memberEvents, e.members = kind, evs, nil
	for _, ev := range evs {
		e.members = append(e.members, memberLine(ev))
	}
	for i, p := range pods {
		if i == 3 {
			break
		}
		if logs, err := fetchLogsForAI(cl, ns, p); err == nil && strings.TrimSpace(logs) != "" {
			e.logsHead = fmt.Sprintf("\nLogs from pod %s, its newest failing Pod (last %d lines):\n", p, getLogTailLines())
			e.logs = logs
			return p
		}
	}
	return ""
}

// render writes the prompt with the newest nEvents events, the first
// nMembers member warnings and nLogLines log lines (-1: the logs as fetched).
func (e *evidence) render(nEvents, nMembers, nLogLines int) string {
	trimmed := func(sb *strings.Builder, n int, what string) {
		if n > 0 {
			fmt.Fprintf(sb, "(%d older %s trimmed to fit the model's context)\n", n, what)
		}
	}
	var sb strings.Builder
	sb.WriteString(e.head)
	if len(e.events) == 0 {
		sb.WriteString("No events recorded for this object.\n")
	} else {
		fmt.Fprintf(&sb, "Events (%d total):\n", len(e.events))
		for _, ev := range newestEvents(e.events, nEvents) {
			fmt.Fprintf(&sb, "- [%s] %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
		trimmed(&sb, len(e.events)-nEvents, pluralWord("event", "events", len(e.events)-nEvents))
	}
	sb.WriteString(e.describe)
	if len(e.members) > 0 {
		fmt.Fprintf(&sb, "\nWarnings on objects this %s owns:\n", e.kind)
		for _, l := range e.members[:nMembers] {
			sb.WriteString(l + "\n")
		}
		trimmed(&sb, len(e.members)-nMembers, pluralWord("warning", "warnings", len(e.members)-nMembers))
	}
	if e.logs != "" {
		sb.WriteString(e.logsHead)
		if nLogLines < 0 {
			sb.WriteString(e.logs)
		} else {
			state, lines := e.logLines()
			if state != "" {
				sb.WriteString(state + "\n")
			}
			for _, l := range pickLogLines(lines, nLogLines) {
				sb.WriteString(l + "\n")
			}
			fmt.Fprintf(&sb, "(log lines trimmed to fit the model's context: %d of %d kept, error lines first)\n",
				min(nLogLines, len(lines)), len(lines))
		}
	}
	sb.WriteString(e.note)
	sb.WriteString(e.linked)
	sb.WriteString(e.tail)
	return sb.String()
}

// logLines splits the logs into the containers' state line, a fact that is
// always kept, and the log lines worth reading.
func (e *evidence) logLines() (state string, lines []string) {
	logs := e.logs
	if strings.HasPrefix(logs, containerStatePrefix) {
		state, logs, _ = strings.Cut(logs, "\n")
	}
	return state, readableLogLines(logs)
}

// fit renders the prompt within budget tokens (0: no limit). It trims log
// lines first (keeping the newest and error-looking ones), then member
// warnings, then the object's oldest events; the describe is never trimmed,
// so the prompt can still exceed a very small budget.
func (e *evidence) fit(budget int) string {
	nEv, nMem := len(e.events), len(e.members)
	full := e.render(nEv, nMem, -1)
	if budget <= 0 || estimateTokens(full) <= budget {
		return full
	}
	fits := func(ev, mem, lg int) bool { return estimateTokens(e.render(ev, mem, lg)) <= budget }
	// most is the largest n in [0, total] for which ok(n) holds, or 0.
	most := func(total int, ok func(n int) bool) int {
		return max(sort.Search(total+1, func(n int) bool { return !ok(n) })-1, 0)
	}
	nLog := -1
	if e.logs != "" {
		_, lines := e.logLines()
		nLog = most(len(lines), func(n int) bool { return fits(nEv, nMem, n) })
		if fits(nEv, nMem, nLog) {
			return e.render(nEv, nMem, nLog)
		}
	}
	nMem = most(nMem, func(n int) bool { return fits(nEv, n, nLog) })
	if fits(nEv, nMem, nLog) {
		return e.render(nEv, nMem, nLog)
	}
	nEv = most(nEv, func(n int) bool { return fits(n, nMem, nLog) })
	return e.render(nEv, nMem, nLog)
}

// buildRCAInitialContext is the RCA's first prompt within budget tokens
// (0: no limit); ctxLen, the loaded context (0: unknown), sizes the linked
// evidence.
func buildRCAInitialContext(cl *clusterState, ns, kind, name string, budget, ctxLen int) rcaInitCtx {
	e := evidence{
		head:   fmt.Sprintf("Perform Root Cause Analysis for %s %q in namespace %q.\n\n", kind, name, ns),
		events: objectEvents(cl, ns, kind, name),
		tail:   "\nUse the available tools to gather additional evidence as needed, then provide the Root Cause Analysis.",
	}

	// Object describe — never trimmed. If the context still overflows, the
	// server returns a clear 400 which we surface to the user with
	// instructions to increase their context length.
	info := cl.fetchObjectInfo(ns, kind, name)
	if !info.NotFound && info.Error == "" {
		e.describe = "\n" + rcaFormatDescribe(info)
	}

	// A workload: what its Pods and Jobs reported, and the newest failing
	// Pod's logs.
	var logsPod, podLogs string
	var podLogErr error
	if !strings.EqualFold(kind, "Pod") {
		logsPod = e.addWorkload(cl, ns, kind, name)
		podLogs = e.logs
	}

	// Pod logs, including previous-run logs for crash loops.
	if strings.EqualFold(kind, "Pod") {
		logsPod = name
		podLogs, podLogErr = fetchLogsForAI(cl, ns, name)
		switch {
		case podLogErr != nil:
			e.note = fmt.Sprintf("\nPod logs: could not be retrieved — %v\n"+
				"(If the pod has crash events, call get_pod_logs as a tool to retry.)\n", podLogErr)
		case strings.TrimSpace(podLogs) != "":
			e.logsHead = fmt.Sprintf("\nPod logs (last %d lines):\n", getLogTailLines())
			e.logs = podLogs
		default:
			e.note = "\nPod logs: no output found — the pod was likely never scheduled " +
				"or has not started a container (FailedScheduling / Pending). " +
				"Do NOT call get_pod_logs for this pod; there is nothing to retrieve.\n"
		}
	}

	// Objects the evidence names, fetched before the model asks (linked.go).
	e.addLinked(cl, ns, kind, name, ctxLen)

	return rcaInitCtx{text: e.fit(budget), logsPod: logsPod, podLogs: podLogs, podLogErr: podLogErr,
		linked: e.linkedItems, linkedTrimmed: e.linkedTrimmed}
}

// ── Agent loop ────────────────────────────────────────────────────────────────

// runRCAAgent keeps the cluster it was started for, even if KubeHealth
// switches while it runs.
func runRCAAgent(cl *clusterState, ns, kind, name string, state *RCAState) {
	now := func() string { return time.Now().Format("2006-01-02 15:04:05 MST") }

	upd := func(fn func()) {
		state.Lock()
		fn()
		state.UpdatedAt = now()
		state.Unlock()
	}

	fail := func(err error) {
		log.Printf("RCA [%s/%s/%s]: %v", ns, kind, name, err)
		upd(func() { state.Status = "error"; state.Error = err.Error() })
	}

	// Build system prompt: user-configured base + RCA-specific instructions
	sp := getSystemPrompt()
	if sp == "" {
		sp = "You are a Kubernetes SRE expert."
	}
	sp += rcaSystemPromptSuffix

	tools := rcaToolDefs()

	// The first prompt (system prompt, evidence, tool definitions) gets
	// promptShare of the loaded context; the evidence is trimmed to fit.
	ctxLen := modelCtxLen()
	toolsJSON, _ := json.Marshal(tools)
	budget := promptBudget(ctxLen, estimateTokens(sp)+estimateTokens(string(toolsJSON)))
	initCtx := buildRCAInitialContext(cl, ns, kind, name, budget, ctxLen)

	msgs := []rcaMsg{
		{Role: "system", Content: sp},
		{Role: "user", Content: initCtx.text},
	}

	// Add a synthetic evidence-trail step for anything gathered in the initial context.
	if initCtx.logsPod != "" {
		step := RCAStep{
			Iter:    0,
			Tool:    "get_pod_logs",
			Label:   ns + " / " + initCtx.logsPod,
			Initial: true,
		}
		switch {
		case initCtx.podLogErr != nil:
			step.Status = "error"
			step.Label += " (error: " + initCtx.podLogErr.Error() + ")"
		case strings.TrimSpace(initCtx.podLogs) != "":
			step.Status = "done"
		default:
			step.Status = "unavailable"
			step.Label += " (not scheduled / no output)"
		}
		upd(func() { state.Steps = append(state.Steps, step) })
	}

	// Each object the linked evidence lists is a step of the trail too.
	upd(func() {
		for _, it := range initCtx.linked {
			state.Linked = append(state.Linked, it.tag())
		}
		state.LinkedTrimmed = initCtx.linkedTrimmed
	})
	for _, it := range initCtx.linked {
		step := RCAStep{Iter: 0, Tool: "linked_evidence", Label: it.Kind + " " + it.Name, Status: "done", Initial: true}
		upd(func() { state.Steps = append(state.Steps, step) })
	}

	estimatedTokens := len(sp)/4 + len(initCtx.text)/4

	maxIter := state.MaxIter

	for iter := 0; iter < maxIter; iter++ {
		upd(func() { state.Iteration = iter + 1 })

		// Stop offering tools if we're using > 70% of the context window
		activeTools := tools
		if ctxLen > 0 && estimatedTokens > ctxLen*70/100 {
			log.Printf("RCA: context budget reached (%d/%d tokens), forcing conclusion", estimatedTokens, ctxLen)
			activeTools = nil
		}

		resp, err := callLMStudioRCA(msgs, activeTools)
		if err != nil {
			if isContextOverflow(err) {
				// Give the user actionable instructions rather than a raw error.
				mi := fetchLMModelInfo()
				ctxNote := ""
				if mi != nil && mi.ctxLen() > 0 {
					ctxNote = fmt.Sprintf(" (model is loaded with only %d tokens)", mi.ctxLen())
				}
				upd(func() {
					state.Status = "error"
					state.ErrorType = "context_overflow"
					state.Error = fmt.Sprintf(
						"The initial context (events + describe + logs) plus tool definitions "+
							"exceed the model's loaded context window%s.\n\n"+
							"Fix in LM Studio: click the ⚙ gear icon next to your loaded model, "+
							"increase Context Length (try 8192 or 16384), then Reload. "+
							"With llama-server, restart it with a larger --ctx-size.",
						ctxNote)
				})
				return
			}
			fail(fmt.Errorf("iter %d: %w", iter, err))
			return
		}

		// Replace estimate with the real prompt-token count from the API response.
		// This makes the budget check precise on subsequent iterations.
		if resp.Usage.PromptTokens > 0 {
			estimatedTokens = resp.Usage.PromptTokens
			log.Printf("RCA iter %d: actual prompt tokens = %d / %d", iter, estimatedTokens, ctxLen)
		}

		choice := resp.Choices[0]
		assistantMsg := choice.Message
		msgs = append(msgs, assistantMsg)

		// No tool calls → the model has concluded
		if len(assistantMsg.ToolCalls) == 0 || choice.FinishReason == "stop" {
			content := ""
			if s, ok := assistantMsg.Content.(string); ok {
				content = cleanLMResponse(s)
			}
			upd(func() { state.Status = "done"; state.Result = content })
			return
		}

		// Execute each requested tool call
		for _, tc := range assistantMsg.ToolCalls {
			var args map[string]string
			json.Unmarshal([]byte(tc.Function.Arguments), &args)
			label := rcaStepLabel(tc.Function.Name, args)

			stepIdx := 0
			upd(func() {
				state.Steps = append(state.Steps, RCAStep{
					Iter:   iter + 1,
					Tool:   tc.Function.Name,
					Label:  label,
					Status: "running",
				})
				stepIdx = len(state.Steps) - 1
			})

			result := executeRCATool(cl, tc, ns)
			estimatedTokens += len(result) / 4

			msgs = append(msgs, rcaMsg{
				Role:       "tool",
				ToolCallID: tc.ID,
				Content:    result,
			})

			upd(func() {
				if stepIdx < len(state.Steps) {
					state.Steps[stepIdx].Status = "done"
				}
			})
		}
	}

	// Exhausted iterations — force a conclusion with what was gathered
	msgs = append(msgs, rcaMsg{
		Role:    "user",
		Content: "You have reached the maximum investigation steps. Produce the final Root Cause Analysis now based on the evidence gathered so far.",
	})
	resp, err := callLMStudioRCA(msgs, nil)
	if err != nil {
		fail(fmt.Errorf("final conclusion: %w", err))
		return
	}
	content := ""
	if s, ok := resp.Choices[0].Message.Content.(string); ok {
		content = cleanLMResponse(s)
	}
	upd(func() { state.Status = "done"; state.Result = content })
}
