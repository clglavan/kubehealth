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

When you have enough evidence, produce the final RCA in EXACTLY this format:

**Root Cause**: <one specific technical sentence>

**Evidence**:
1. <key finding with its source>
2. <key finding with its source>

**Resolution**: <numbered actionable steps>

**Confidence**: High / Medium / Low — <brief reason>`

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
	var sb strings.Builder
	fmt.Fprintf(&sb, "Describe %s/%s/%s:\n", info.Namespace, info.Kind, info.Name)
	if info.Age != "" {
		fmt.Fprintf(&sb, "Age: %s\n", info.Age)
	}
	if len(info.Labels) > 0 {
		sb.WriteString("Labels:")
		for k, v := range info.Labels {
			fmt.Fprintf(&sb, " %s=%s", k, v)
		}
		sb.WriteString("\n")
	}
	for _, sec := range info.Sections {
		fmt.Fprintf(&sb, "%s:\n", sec.Title)
		for _, row := range sec.Rows {
			fmt.Fprintf(&sb, "  %s: %s\n", row.Key, row.Value)
		}
	}
	if len(info.Conditions) > 0 {
		sb.WriteString("Conditions:\n")
		for _, c := range info.Conditions {
			if c.Reason != "" || c.Message != "" {
				fmt.Fprintf(&sb, "  %s: %s (%s: %s)\n", c.Type, c.Status, c.Reason, c.Message)
			} else {
				fmt.Fprintf(&sb, "  %s: %s\n", c.Type, c.Status)
			}
		}
	}
	return sb.String()
}

// ── Initial context ───────────────────────────────────────────────────────────

// rcaInitCtx carries the assembled initial prompt plus metadata about what was gathered.
type rcaInitCtx struct {
	text      string
	logsPod   string // the Pod whose logs were read: the anchor itself, or a workload's newest failing Pod
	podLogs   string // "" = not a pod or fetch returned nothing
	podLogErr error  // non-nil = hard fetch error
}

// rcaMemberWarnings caps how many of a workload's member warnings go into the
// first prompt, so a small model's context still has room for logs and tools.
const rcaMemberWarnings = 12

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
		if !seen[key] && len(lines) < rcaMemberWarnings {
			seen[key] = true
			lines = append(lines, fmt.Sprintf("- %s %s %s: %s  (×%d, %s ago)",
				ev.Kind, ev.ObjectName, ev.Reason, ev.Message, ev.Count, ev.Age))
		}
		if ev.Kind == "Pod" && ev.LastTime.After(podLast[ev.ObjectName]) {
			podLast[ev.ObjectName] = ev.LastTime
		}
	}
	for p := range podLast {
		pods = append(pods, p)
	}
	sort.Slice(pods, func(i, j int) bool { return podLast[pods[i]].After(podLast[pods[j]]) })
	return lines, pods
}

func buildRCAInitialContext(cl *clusterState, ns, kind, name string) rcaInitCtx {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Perform Root Cause Analysis for %s %q in namespace %q.\n\n", kind, name, ns)

	// Events for the anchor object
	var events []EventSummary
	for _, ev := range cl.scopeEvents(ns) {
		if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
			events = append(events, ev)
		}
	}
	if len(events) == 0 {
		sb.WriteString("No events recorded for this object.\n")
	} else {
		fmt.Fprintf(&sb, "Events (%d total):\n", len(events))
		for _, ev := range events {
			fmt.Fprintf(&sb, "- [%s] %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
	}

	// Object describe — full, untruncated. If this causes context overflow, LM Studio
	// returns a clear 400 which we surface to the user with instructions to increase
	// their context length rather than silently hiding data.
	info := cl.fetchObjectInfo(ns, kind, name)
	if !info.NotFound && info.Error == "" {
		sb.WriteString("\n")
		sb.WriteString(rcaFormatDescribe(info))
	}

	// A workload: what its Pods and Jobs reported, and the newest failing
	// Pod's logs (a Pod that has since been deleted has none, so try the next).
	var logsPod, podLogs string
	var podLogErr error
	if !strings.EqualFold(kind, "Pod") {
		lines, pods := memberEvidence(cl, ns, kind, name)
		if len(lines) > 0 {
			fmt.Fprintf(&sb, "\nWarnings on objects this %s owns:\n%s\n", kind, strings.Join(lines, "\n"))
		}
		for i, p := range pods {
			if i == 3 {
				break
			}
			if logs, err := fetchLogsForAI(cl, ns, p); err == nil && strings.TrimSpace(logs) != "" {
				fmt.Fprintf(&sb, "\nLogs from pod %s, its newest failing Pod (last %d lines):\n%s", p, getLogTailLines(), logs)
				logsPod, podLogs = p, logs
				break
			}
		}
	}

	// Pod logs — full output, untruncated. If the combined context is too large,
	// LM Studio returns a specific 400 which we detect and surface as a clear
	// "increase your context length" message instead of silently hiding data.
	if strings.EqualFold(kind, "Pod") {
		logsPod = name
		podLogs, podLogErr = fetchLogsForAI(cl, ns, name)
		switch {
		case podLogErr != nil:
			fmt.Fprintf(&sb, "\nPod logs: could not be retrieved — %v\n"+
				"(If the pod has crash events, call get_pod_logs as a tool to retry.)\n", podLogErr)
		case strings.TrimSpace(podLogs) != "":
			fmt.Fprintf(&sb, "\nPod logs (last %d lines):\n%s", getLogTailLines(), podLogs)
		default:
			sb.WriteString("\nPod logs: no output found — the pod was likely never scheduled " +
				"or has not started a container (FailedScheduling / Pending). " +
				"Do NOT call get_pod_logs for this pod; there is nothing to retrieve.\n")
		}
	}

	sb.WriteString("\nUse the available tools to gather additional evidence as needed, then provide the Root Cause Analysis.")
	return rcaInitCtx{text: sb.String(), logsPod: logsPod, podLogs: podLogs, podLogErr: podLogErr}
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

	initCtx := buildRCAInitialContext(cl, ns, kind, name)

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

	tools := rcaToolDefs()

	// Estimate context budget from model info
	ctxLen := 0
	if mi := fetchLMModelInfo(); mi != nil {
		ctxLen = mi.ctxLen()
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
							"increase Context Length (try 8192 or 16384), then Reload.",
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
