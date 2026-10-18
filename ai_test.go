package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
)

// fakeLLM is an OpenAI-compatible server. Each chat request gets the next
// answer: streamed as SSE chunks when the request streams, one JSON
// completion otherwise. Requests' bodies are kept.
type fakeLLM struct {
	sync.Mutex
	answers   []string // "reasoning:" prefixed answers are sent as reasoning_content
	requests  []map[string]any
	modelsCtx int // n_ctx reported on /v1/models; 0: 404
}

func (f *fakeLLM) start(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/models":
			if f.modelsCtx == 0 {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintf(w, `{"object":"list","data":[{"id":"magos","object":"model","owned_by":"llamacpp","meta":{"n_ctx":%d,"n_ctx_train":40960}}]}`, f.modelsCtx)
			return
		case "/v1/chat/completions":
		default:
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		json.Unmarshal(body, &req)
		f.Lock()
		f.requests = append(f.requests, req)
		answer := ""
		if len(f.answers) > 0 {
			answer, f.answers = f.answers[0], f.answers[1:]
		}
		f.Unlock()
		if req["stream"] != true {
			out, _ := json.Marshal(map[string]any{"choices": []map[string]any{{
				"message":       map[string]any{"role": "assistant", "content": answer},
				"finish_reason": "stop",
			}}})
			w.Write(out)
			return
		}
		// "reasoning:R" streams R as reasoning_content; "reasoning:R\x00A"
		// streams R as reasoning_content, then A as content.
		send := func(field, text string) {
			for _, tok := range strings.SplitAfter(text, " ") {
				chunk, _ := json.Marshal(map[string]any{"choices": []map[string]any{{"delta": map[string]string{field: tok}}}})
				fmt.Fprintf(w, "data: %s\n\n", chunk)
			}
		}
		if r, ok := strings.CutPrefix(answer, "reasoning:"); ok {
			r, a, _ := strings.Cut(r, "\x00")
			send("reasoning_content", r)
			if a != "" {
				send("content", a)
			}
		} else {
			send("content", answer)
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	t.Cleanup(srv.Close)
	saved := aiConfig
	aiConfig = aiCfgT{baseURL: srv.URL + "/v1", model: "magos", enabled: true}
	t.Cleanup(func() { aiConfig = saved })
}

// messages returns the chat messages of request i.
func (f *fakeLLM) messages(i int) []map[string]any {
	f.Lock()
	defer f.Unlock()
	var out []map[string]any
	for _, m := range f.requests[i]["messages"].([]any) {
		out = append(out, m.(map[string]any))
	}
	return out
}

func TestIsOverflowMessage(t *testing.T) {
	for _, body := range []string{
		`{"error":"Trying to keep the first 9000 tokens when context the overflows. However, the model is loaded with context length of only 4096 tokens"}`,
		`{"error":{"code":400,"message":"request (12352 tokens) exceeds the available context size (8192 tokens), try increasing it","type":"exceed_context_size_error","n_prompt_tokens":12352,"n_ctx":8192}}`,
	} {
		if !isOverflowMessage(body) {
			t.Errorf("not recognised: %s", body)
		}
	}
	if isOverflowMessage(`{"error":{"code":400,"message":"invalid tool call","type":"invalid_request_error"}}`) {
		t.Error("an unrelated 400 is not a context overflow")
	}
}

func TestPromptBudget(t *testing.T) {
	if got := promptBudget(0, 500); got != 0 {
		t.Errorf("unknown context: %d; want no limit", got)
	}
	if got := promptBudget(8192, 1915); got != 8192*60/100-1915 {
		t.Errorf("8192 ctx: %d", got)
	}
	if got := promptBudget(1000, 5000); got != 1 {
		t.Errorf("overhead larger than the share: %d; want the smallest budget", got)
	}
}

// llama-server reports each slot's context length on /v1/models.
func TestLlamaServerContextLength(t *testing.T) {
	f := &fakeLLM{modelsCtx: 8192}
	f.start(t)
	if got := modelCtxLen(); got != 8192 {
		t.Errorf("ctxLen = %d; want 8192", got)
	}
	f.modelsCtx = 0
	if mi := fetchLMModelInfo(); mi != nil {
		t.Errorf("no model info expected, got %+v", mi)
	}
}

// sampleEvidence is a workload's evidence: 30 events, 12 member warnings and
// 400 log lines, a few of them errors.
func sampleEvidence() evidence {
	now := time.Now()
	e := evidence{
		head:     "Perform Root Cause Analysis for Deployment \"orders-worker\" in namespace \"orders\".\n\n",
		describe: "\nDescribe orders/Deployment/orders-worker:\nReplicas:\n  desired: 3\n  ready: 0\n",
		kind:     "Deployment",
		logsHead: "\nLogs from pod orders-worker-67576b7f96-h5zlx, its newest failing Pod (last 400 lines):\n",
		tail:     "\nUse the available tools to gather additional evidence as needed, then provide the Root Cause Analysis.",
	}
	for i := 0; i < 30; i++ {
		// Newest first, as the caches return them.
		e.events = append(e.events, EventSummary{Type: "Normal", Reason: fmt.Sprintf("Reason%02d", i),
			Message: "Scaled up replica set orders-worker-67576b7f96 to 3 from a rollout that keeps restarting", Count: 1,
			Age: fmt.Sprintf("%dm", i), LastTime: now.Add(-time.Duration(i) * time.Minute)})
	}
	for i := 0; i < 12; i++ {
		e.members = append(e.members, fmt.Sprintf("- Pod orders-worker-67576b7f96-p%02d BackOff: Back-off restarting failed container worker  (×%d, 1m ago)", i, i+1))
	}
	var logs strings.Builder
	logs.WriteString(containerStatePrefix + "worker restarted 9 times, last run ended Error (exit code 1), now CrashLoopBackOff.\n")
	for i := 0; i < 400; i++ {
		if i%50 == 7 {
			fmt.Fprintf(&logs, "2026-10-17T09:%02d:00Z ERROR connection refused to postgres:5432 (attempt %d)\n", i%60, i)
		} else {
			fmt.Fprintf(&logs, "2026-10-17T09:%02d:00Z INFO processed batch %d of orders from the queue\n", i%60, i)
		}
	}
	e.logs = logs.String()
	return e
}

func TestEvidenceFit(t *testing.T) {
	e := sampleEvidence()
	full := e.render(len(e.events), len(e.members), -1)
	if got := e.fit(0); got != full {
		t.Error("no budget: the prompt must be the untrimmed one")
	}
	if got := e.fit(estimateTokens(full)); got != full {
		t.Error("a prompt that fits must not change")
	}
	for _, l := range []string{"Reason29", "p11 BackOff", "processed batch 0 of orders", "Use the available tools"} {
		if !strings.Contains(full, l) {
			t.Fatalf("untrimmed prompt lacks %q", l)
		}
	}

	// Trimming log lines is enough.
	budget := estimateTokens(full) / 2
	got := e.fit(budget)
	if n := estimateTokens(got); n > budget {
		t.Errorf("logs trimmed: %d tokens > budget %d", n, budget)
	}
	for _, want := range []string{
		"Reason00", "Reason29", "p11 BackOff", // every event and member warning kept
		e.describe, containerStatePrefix + "worker restarted 9 times",
		"ERROR connection refused to postgres:5432 (attempt 357)", // error lines kept first
		"processed batch 399 of orders",                           // and the newest lines
		"log lines trimmed to fit the model's context",
		"Use the available tools",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("logs trimmed: prompt lacks %q", want)
		}
	}
	if strings.Contains(got, "processed batch 0 of orders") || strings.Contains(got, "warnings trimmed") || strings.Contains(got, "events trimmed") {
		t.Errorf("logs trimmed: trimmed the wrong part:\n%s", got)
	}

	// Then member warnings, then the oldest events; never the describe.
	noLogs := e.render(len(e.events), len(e.members), 0)
	budget = estimateTokens(noLogs) - 150
	got = e.fit(budget)
	if n := estimateTokens(got); n > budget {
		t.Errorf("members trimmed: %d tokens > budget %d", n, budget)
	}
	if !strings.Contains(got, "older warnings trimmed") || strings.Contains(got, "events trimmed") || !strings.Contains(got, "Reason29") {
		t.Errorf("members trimmed: want only member warnings trimmed:\n%s", got)
	}

	budget = estimateTokens(e.render(len(e.events), 0, 0)) - 200
	got = e.fit(budget)
	if n := estimateTokens(got); n > budget {
		t.Errorf("events trimmed: %d tokens > budget %d", n, budget)
	}
	for _, want := range []string{e.describe, "Reason00", "Events (30 total)", "older events trimmed", "Use the available tools"} {
		if !strings.Contains(got, want) {
			t.Errorf("events trimmed: prompt lacks %q", want)
		}
	}
	if strings.Contains(got, "Reason29") {
		t.Error("events trimmed: the oldest event was kept")
	}

	// The describe stays even when it alone is over budget.
	if got := e.fit(10); !strings.Contains(got, e.describe) || !strings.Contains(got, "30 older events trimmed") {
		t.Errorf("tiny budget:\n%s", got)
	}
}

func TestNewestEvents(t *testing.T) {
	now := time.Now()
	evs := []EventSummary{{Reason: "a", LastTime: now.Add(-3 * time.Minute)}, {Reason: "b", LastTime: now}, {Reason: "c", LastTime: now.Add(-time.Minute)}}
	got := newestEvents(evs, 2)
	if len(got) != 2 || got[0].Reason != "b" || got[1].Reason != "c" {
		t.Errorf("got %+v; want b and c in their order", got)
	}
}

func TestNoteSkipsThinking(t *testing.T) {
	run := func(answers ...string) (*fakeLLM, *briefState) {
		f := &fakeLLM{answers: answers}
		f.start(t)
		c := testCluster()
		if !acquireLLMSlot() {
			t.Fatal("no LLM slot")
		}
		ctx, cancel := context.WithCancel(context.Background())
		p := Problem{Key: "a", Scope: "namespace", Kind: "Deployment", Title: "orders", Namespaces: []string{"orders"},
			Reasons: []ReasonCount{{Reason: "BackOff", Count: 3}}, Since: "5m", Last: "1m"}
		c.runBrief(ctx, cancel, p)
		return f, &c.brief
	}

	f, b := run("<think>The user wants a check.</think>Check the database credentials first.")
	if b.status != "ready" || b.text != "Check the database credentials first." {
		t.Errorf("note: %q (%s: %s)", b.text, b.status, b.reason)
	}
	kw, _ := f.requests[0]["chat_template_kwargs"].(map[string]any)
	if kw == nil || kw["enable_thinking"] != false {
		t.Errorf("request lacks enable_thinking=false: %v", f.requests[0])
	}

	// A server that still reasons: reasoning_content is not the note.
	_, b = run("reasoning:Okay, let me think about what the engineer should check.")
	if b.status != "error" || b.reason != "The model's answer was empty." || b.text != "" {
		t.Errorf("reasoning only: %q (%s: %s)", b.text, b.status, b.reason)
	}
	_, b = run("<think>Okay, the pod is crashing so")
	if b.status != "error" || b.text != "" {
		t.Errorf("unfinished think: %q (%s: %s)", b.text, b.status, b.reason)
	}
}

const goodAnalysis = "The Deployment cannot pull its image: tag 1.27.9 does not exist. Fix the image tag."

// ✦ AI on a workload reads its Pods' warnings and the newest failing Pod's
// logs, as RCA does.
func TestAnalyzeWorkload(t *testing.T) {
	f := &fakeLLM{answers: []string{goodAnalysis}}
	f.start(t)
	c := testCluster()
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "orders-worker-67576b7f96-h5zlx", Namespace: "orders"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}}}
	c.client = fake.NewSimpleClientset(pod)
	c.dynamic = dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	now := time.Now()
	c.nsCache.Store("orders", &nsEntry{events: []EventSummary{
		warn("orders", "Pod", "orders-worker-67576b7f96-h5zlx", "BackOff", 10, now.Add(-5*time.Minute), now.Add(-10*time.Second)),
		normal("orders", "Deployment", "orders-worker", "ScalingReplicaSet", now.Add(-6*time.Minute)),
	}})

	if err := analyzeObjKey(c, "test|obj:orders/Deployment/orders-worker", "orders", "Deployment", "orders-worker"); err != nil {
		t.Fatal(err)
	}
	msgs := f.messages(0)
	prompt := msgs[len(msgs)-1]["content"].(string)
	for _, want := range []string{
		"Analyze events for Deployment \"orders-worker\" in namespace \"orders\".",
		"ScalingReplicaSet",
		"Warnings on objects this Deployment owns:\n- Pod orders-worker-67576b7f96-h5zlx BackOff",
		fmt.Sprintf("Logs from pod orders-worker-67576b7f96-h5zlx, its newest failing Pod (last %d lines):\nfake logs", getLogTailLines()),
		"\nIn a few short lines: is anything wrong with this resource? If so, say what, why, and what to do. If nothing is wrong, say so.",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt lacks %q:\n%s", want, prompt)
		}
	}
	v, _ := aiInsightCache.Load("test|obj:orders/Deployment/orders-worker")
	if in := v.(*AIInsight); in.Content != goodAnalysis || in.Partial {
		t.Errorf("insight: %+v", in)
	}
}

// Reasoning streamed as reasoning_content never becomes part of the answer;
// the finished insight keeps it apart.
func TestAnalysisKeepsReasoningApart(t *testing.T) {
	store := func(answers ...string) *AIInsight {
		f := &fakeLLM{answers: answers}
		f.start(t)
		storeAnalysis("test|reasoning", "prompt")
		v, _ := aiInsightCache.Load("test|reasoning")
		return v.(*AIInsight)
	}
	in := store("reasoning:The tag 1.27.9 does not exist on the registry.\x00" + goodAnalysis)
	if in.Content != goodAnalysis || in.Reasoning != "The tag 1.27.9 does not exist on the registry." || in.Error != "" {
		t.Errorf("reasoning then answer: %+v", in)
	}
	// Inline <think> (servers that don't split it out).
	in = store("<think>Look at the tag.</think>" + goodAnalysis)
	if in.Content != goodAnalysis || in.Reasoning != "Look at the tag." {
		t.Errorf("inline think: %+v", in)
	}
	// Reasoning only (the model never answered): an error, not the reasoning
	// as the answer.
	in = store("reasoning:Okay, let me think about the tag")
	if in.Content != "" || in.Error == "" {
		t.Errorf("reasoning only: %+v", in)
	}
}

// While a model reasons, the streamed insight says it is thinking and has
// no content; once the answer streams, the content is the answer only.
func TestStreamingPartialsHideReasoning(t *testing.T) {
	f := &fakeLLM{answers: []string{"reasoning:" + strings.Repeat("thinking hard ", 50) + "\x00" + goodAnalysis}}
	f.start(t)
	var partials [][2]string
	_, reasoning, err := streamLMStudio("k", "prompt", func(p, r string) { partials = append(partials, [2]string{p, r}) })
	if err != nil || !strings.HasPrefix(reasoning, "thinking hard") {
		t.Fatalf("reasoning %q, %v", reasoning, err)
	}
	for _, p := range partials {
		if strings.Contains(p[0], "thinking") {
			t.Errorf("partial answer contains reasoning: %q", p[0])
		}
	}
}
