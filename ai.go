package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type aiCfgT struct {
	baseURL string
	model   string
	enabled bool
}

// AIInsight is the JSON payload for /api/ai/insights/… responses.
type AIInsight struct {
	Content   string `json:"content"`
	Model     string `json:"model"`
	UpdatedAt string `json:"updated_at"`
	Enabled   bool   `json:"enabled"`
	Partial   bool   `json:"partial,omitempty"` // true while streaming is in progress
	Error     string `json:"error,omitempty"`   // non-empty when the analysis failed
	// Thinking is true while a thinking model reasons and no answer has
	// streamed yet. The reasoning itself is never part of Content; the
	// finished insight keeps it in Reasoning.
	Thinking  bool   `json:"thinking,omitempty"`
	Reasoning string `json:"reasoning,omitempty"`
}

var (
	aiConfig       aiCfgT
	aiInsightCache sync.Map // aiStoreKey → *AIInsight
	aiRunning      sync.Map // aiStoreKey → struct{} (dedup guard)
)

// aiStoreKey scopes a cache key to a cluster: "default" on one cluster and
// "default" on another are different namespaces.
func aiStoreKey(cl *clusterState, key string) string { return cl.name + "|" + key }

func initAI() {
	url := os.Getenv("LMSTUDIO_URL")
	if url == "" {
		url = "http://localhost:1234/v1"
	}
	model := os.Getenv("LMSTUDIO_MODEL")
	if model == "" {
		model = "qwen/qwen3-4b-2507"
	}
	v := os.Getenv("AI_ENABLED")
	enabled := v != "false" && v != "0"
	aiConfig = aiCfgT{baseURL: url, model: model, enabled: enabled}
	initLinkFollow()
	if enabled {
		log.Printf("AI monitoring enabled — model: %s  url: %s", model, url)
		if !getLinkFollow() {
			log.Printf("AI link-following off (AI_LINK_FOLLOW)")
		}
	}
}

// parseCacheKey converts a URL suffix into an internal cache key.
// Supported forms: "ns/{namespace}" or "object/{namespace}/{kind}/{name}"
func parseCacheKey(suffix string) (string, bool) {
	suffix = strings.Trim(suffix, "/")
	if ns, ok := strings.CutPrefix(suffix, "ns/"); ok && ns != "" {
		return "ns:" + ns, true
	}
	if obj, ok := strings.CutPrefix(suffix, "object/"); ok {
		parts := strings.SplitN(obj, "/", 3)
		if len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != "" {
			return "obj:" + obj, true
		}
	}
	return "", false
}

func handleAPIAIInsights(w http.ResponseWriter, r *http.Request) {
	suffix := strings.TrimPrefix(r.URL.Path, "/api/ai/insights/")
	key, ok := parseCacheKey(suffix)
	cl := apiCluster(w, r)
	if cl == nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiModelName()})
		return
	}
	if v, hit := aiInsightCache.Load(aiStoreKey(cl, key)); hit {
		json.NewEncoder(w).Encode(v.(*AIInsight))
		return
	}
	json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiModelName()})
}

func handleAPIAIAnalyze(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !aiConfig.enabled {
		http.Error(w, "AI not enabled", http.StatusServiceUnavailable)
		return
	}
	suffix := strings.TrimPrefix(r.URL.Path, "/api/ai/analyze/")
	key, ok := parseCacheKey(suffix)
	if !ok {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	cl := apiCluster(w, r)
	if cl == nil {
		return
	}
	triggerScopedAnalysis(cl, key)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "triggered"})
}

// triggerScopedAnalysis starts an analysis in the background. It keeps the
// cluster it was started for, even if KubeHealth switches meanwhile.
func triggerScopedAnalysis(cl *clusterState, key string) {
	storeKey := aiStoreKey(cl, key)
	if _, loaded := aiRunning.LoadOrStore(storeKey, struct{}{}); loaded {
		return
	}
	go func() {
		defer aiRunning.Delete(storeKey)
		var err error
		if ns, ok := strings.CutPrefix(key, "ns:"); ok {
			err = analyzeNSKey(cl, storeKey, ns)
		} else if obj, ok := strings.CutPrefix(key, "obj:"); ok {
			parts := strings.SplitN(obj, "/", 3)
			if len(parts) == 3 {
				err = analyzeObjKey(cl, storeKey, parts[0], parts[1], parts[2])
			}
		}
		if err != nil {
			log.Printf("AI [%s]: %v", storeKey, err)
		}
	}()
}

func analyzeNSKey(cl *clusterState, key, ns string) error {
	events := cl.scopeEvents(ns)

	var sb strings.Builder
	if ns == clusterScopeNS {
		sb.WriteString("Analyze all Kubernetes events about cluster-scoped objects (nodes, persistent volumes and similar).\n\n")
	} else {
		fmt.Fprintf(&sb, "Analyze all Kubernetes events in namespace %q.\n\n", ns)
	}
	if len(events) == 0 {
		sb.WriteString("No events recorded in this namespace.\n")
	} else {
		var nw int
		for _, ev := range events {
			if ev.Type == "Warning" {
				nw++
			}
		}
		sb.WriteString(fmt.Sprintf("Events (%d total, %d warnings):\n", len(events), nw))
		for i, ev := range events {
			if i >= 60 {
				sb.WriteString(fmt.Sprintf("... and %d more\n", len(events)-60))
				break
			}
			sb.WriteString(fmt.Sprintf("- [%s] %s/%s  %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Kind, ev.ObjectName, ev.Reason, ev.Message, ev.Count, ev.Age))
		}
	}
	format := analysisFormat("this namespace")
	sb.WriteString(analysisInstructions(format))
	return storeAnalysis(key, sb.String())
}

// analysisFormat is what the ✦ AI analysis asks for. It names no headings:
// nothing reads the answer's structure, and a small model spends its effort
// better on the diagnosis than on a layout.
func analysisFormat(subject string) string {
	return "In a few short lines: is anything wrong with " + subject + "? If so, say what, why, and what to do. If nothing is wrong, say so."
}

// analysisInstructions ends an analysis prompt.
func analysisInstructions(format string) string {
	return "\n" + format
}

func analyzeObjKey(cl *clusterState, key, ns, kind, name string) error {
	ctxLen := modelCtxLen()
	e := analysisEvidence(cl, ns, kind, name, ctxLen)
	budget := promptBudget(ctxLen, estimateTokens(getSystemPrompt()))
	return storeAnalysis(key, e.fit(budget))
}

// analysisEvidence is the ✦ AI analysis prompt of one object, untrimmed;
// ctxLen (0: unknown) sizes the linked evidence.
func analysisEvidence(cl *clusterState, ns, kind, name string, ctxLen int) evidence {
	format := analysisFormat("this resource")
	e := evidence{
		head:   fmt.Sprintf("Analyze events for %s %q in namespace %q.\n\n", kind, name, ns),
		events: objectEvents(cl, ns, kind, name),
		tail:   analysisInstructions(format),
	}

	// Object describe — always included; gives the AI current spec/status/conditions
	info := cl.fetchObjectInfo(ns, kind, name)
	var desc strings.Builder
	switch {
	case info.NotFound:
		desc.WriteString("\nNote: object no longer exists in the cluster.\n")
	case info.Error == "":
		desc.WriteString("\nObject describe:\n")
		desc.WriteString(describeBody(info))
	}
	e.describe = desc.String()

	if strings.EqualFold(kind, "Pod") {
		// Pod logs — actual container output, most useful for crash diagnosis
		logs, err := fetchLogsForAI(cl, ns, name)
		if err == nil && strings.TrimSpace(logs) != "" {
			e.logsHead = fmt.Sprintf("\nPod logs (last %d lines):\n", getLogTailLines())
			e.logs = logs
		} else if err != nil {
			e.note = fmt.Sprintf("\nNote: could not fetch pod logs: %v\n", err)
		}
	} else {
		// A workload's own events rarely say what is wrong; its Pods' do.
		e.addWorkload(cl, ns, kind, name)
	}

	// Objects the evidence names, fetched before the model asks (linked.go).
	e.addLinked(cl, ns, kind, name, ctxLen)
	return e
}

// storeAnalysis streams the answer to prompt into the insight cache.
func storeAnalysis(key, prompt string) error {
	now := func() string { return time.Now().Format("2006-01-02 15:04:05 MST") }

	// Immediately mark as partial so the UI shows "Processing prompt…" instead of the
	// generic "Analyzing…" spinner while LM Studio processes the input tokens.
	aiInsightCache.Store(key, &AIInsight{
		Partial:   true,
		Model:     aiModelName(),
		UpdatedAt: now(),
		Enabled:   true,
	})

	content, reasoning, err := streamLMStudio(key, prompt, func(partial, reasoning string) {
		answer := strings.TrimSpace(stripThink(partial))
		aiInsightCache.Store(key, &AIInsight{
			Content:   answer,
			Partial:   true,
			Thinking:  answer == "" && (reasoning != "" || strings.Contains(partial, "<think>")),
			Model:     aiModelName(),
			UpdatedAt: now(),
			Enabled:   true,
		})
	})
	if err == nil && cleanLMResponse(content) == "" {
		err = fmt.Errorf("the model returned no answer, only its reasoning")
	}

	if err != nil {
		log.Printf("AI analysis [%s]: %v", key, err)
		aiInsightCache.Store(key, &AIInsight{
			Error:     err.Error(),
			Model:     aiModelName(),
			UpdatedAt: now(),
			Enabled:   true,
		})
		return err
	}
	aiInsightCache.Store(key, &AIInsight{
		Content:   cleanLMResponse(content),
		Reasoning: strings.TrimSpace(reasoning + thinkText(content)),
		Model:     aiModelName(),
		UpdatedAt: now(),
		Enabled:   true,
	})
	return nil
}

// thinkText is the reasoning inside a <think> block of an answer, for
// servers that don't split it into reasoning_content.
func thinkText(s string) string {
	i := strings.Index(s, "<think>")
	j := strings.Index(s, "</think>")
	if i < 0 || j < i {
		return ""
	}
	return strings.TrimSpace(s[i+len("<think>") : j])
}

// streamLMStudio sends a streaming chat-completion request to LM Studio and calls
// writeFn with the accumulated answer and reasoning after each ~300ms batch
// of tokens. It returns the answer and, apart, the reasoning_content.
// It enforces the global concurrent LLM call limit.
func streamLMStudio(logKey, prompt string, writeFn func(partial, reasoning string)) (string, string, error) {
	if !acquireLLMSlotForUser() {
		return "", "", fmt.Errorf("too many concurrent AI requests (%d/%d active) — try again shortly",
			getLLMActiveCalls(), getLLMMaxCalls())
	}
	defer releaseLLMSlot()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getLLMTimeout())*time.Minute)
	defer cancel()
	temp := analysisTemperature
	opts := chatOpts{Temperature: &temp}
	out, reasoning, err := streamChatMsgs(ctx, chatMessages(prompt, opts), opts, writeFn)
	return out, reasoning, err
}

// chatMessages is the system prompt (opts.System, or the AI Agent's) and the
// user's prompt.
func chatMessages(prompt string, opts chatOpts) []lmMsg {
	sp := opts.System
	if sp == "" {
		sp = getSystemPrompt()
	}
	msgs := []lmMsg{{Role: "user", Content: prompt}}
	if sp != "" {
		msgs = append([]lmMsg{{Role: "system", Content: sp}}, msgs...)
	}
	return msgs
}

// streamChat sends one streaming chat request and returns the full answer,
// without the reasoning. The caller holds an LLM slot; cancelling ctx stops
// the request.
func streamChat(ctx context.Context, prompt string, opts chatOpts, writeFn func(partial, reasoning string)) (string, error) {
	out, _, err := streamChatMsgs(ctx, chatMessages(prompt, opts), opts, writeFn)
	return out, err
}

// streamChatMsgs sends a whole conversation and returns the answer
// (content) and, apart, the reasoning (reasoning_content), which servers
// such as llama-server stream before the answer. The reasoning is never
// part of the answer; with opts.NoThink it is dropped.
func streamChatMsgs(ctx context.Context, msgs []lmMsg, opts chatOpts, writeFn func(partial, reasoning string)) (string, string, error) {
	if useClaudeCode() {
		return ccChat(ctx, msgs, opts, writeFn)
	}
	r := lmReq{
		Model:       aiConfig.model,
		Messages:    msgs,
		Stream:      true,
		Temperature: opts.Temperature,
		MaxTokens:   opts.MaxTokens,
	}
	if opts.NoThink {
		r.ChatTemplateKwargs = map[string]any{"enable_thinking": false}
	}
	body, _ := json.Marshal(r)

	req, err := http.NewRequestWithContext(ctx, "POST", aiConfig.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("LM Studio: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var buf [1024]byte
		n, _ := resp.Body.Read(buf[:])
		body := strings.TrimSpace(string(buf[:n]))
		if isOverflowMessage(body) {
			return "", "", &contextOverflowError{detail: body}
		}
		return "", "", fmt.Errorf("LM Studio returned %d: %s", resp.StatusCode, body)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	var full, reasoning strings.Builder
	var lastWrite time.Time

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) > 0 {
			d := chunk.Choices[0].Delta
			full.WriteString(d.Content)
			if !opts.NoThink {
				reasoning.WriteString(d.ReasoningContent)
			}
			if (d.Content != "" || d.ReasoningContent != "") && writeFn != nil && time.Since(lastWrite) >= 300*time.Millisecond {
				writeFn(full.String(), reasoning.String())
				lastWrite = time.Now()
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return full.String(), reasoning.String(), fmt.Errorf("stream read: %w", err)
	}
	return full.String(), reasoning.String(), nil
}

type lmMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type lmReq struct {
	Model       string   `json:"model"`
	Messages    []lmMsg  `json:"messages"`
	Stream      bool     `json:"stream"`
	Temperature *float64 `json:"temperature,omitempty"`
	MaxTokens   int      `json:"max_tokens,omitempty"`
	// Passed to the chat template by llama-server and other OpenAI-compatible
	// servers; servers that don't know it ignore it.
	ChatTemplateKwargs map[string]any `json:"chat_template_kwargs,omitempty"`
}

// chatOpts tunes one request. The zero value uses the AI Agent's system
// prompt and the server's defaults.
type chatOpts struct {
	System      string // replaces the AI Agent system prompt when set
	Temperature *float64
	MaxTokens   int
	// NoThink asks a thinking model (Qwen3) to answer without reasoning
	// first, and keeps reasoning_content out of the answer.
	NoThink bool
}

type lmResp struct {
	Choices []struct {
		Message lmMsg `json:"message"`
	} `json:"choices"`
}

// ── Context-overflow sentinel ─────────────────────────────────────────────────

// contextOverflowError is returned when the server rejects a request because the
// input tokens exceed the model's loaded context length.
type contextOverflowError struct{ detail string }

func (e *contextOverflowError) Error() string {
	return "context window exceeded: " + e.detail
}

func isContextOverflow(err error) bool {
	_, ok := err.(*contextOverflowError)
	return ok
}

// isOverflowMessage recognises a server's context-overflow error body: LM
// Studio's "Trying to keep the first N tokens" (bug-tracker issue #237) and
// llama-server's exceed_context_size_error ("request (N tokens) exceeds the
// available context size (M tokens)").
func isOverflowMessage(body string) bool {
	return strings.Contains(body, "Trying to keep the first") ||
		strings.Contains(body, "exceed_context_size_error") ||
		strings.Contains(body, "exceeds the available context size")
}

// ── Tool-calling types (RCA agent only) ───────────────────────────────────────

type rcaToolProp struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

type rcaToolParam struct {
	Type       string                 `json:"type"`
	Properties map[string]rcaToolProp `json:"properties"`
	Required   []string               `json:"required"`
}

type rcaTool struct {
	Type     string `json:"type"` // always "function"
	Function struct {
		Name        string       `json:"name"`
		Description string       `json:"description"`
		Parameters  rcaToolParam `json:"parameters"`
	} `json:"function"`
}

type rcaToolCallFn struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"` // JSON-encoded args
}

type rcaToolCall struct {
	ID       string        `json:"id"`
	Type     string        `json:"type"`
	Function rcaToolCallFn `json:"function"`
}

// rcaMsg is the richer message type for the tool-calling conversation.
// Content is interface{} so it round-trips as null when the model omits it.
type rcaMsg struct {
	Role       string        `json:"role"`
	Content    interface{}   `json:"content"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
	ToolCalls  []rcaToolCall `json:"tool_calls,omitempty"`
}

type rcaRequest struct {
	Model       string    `json:"model"`
	Messages    []rcaMsg  `json:"messages"`
	Tools       []rcaTool `json:"tools,omitempty"`
	Stream      bool      `json:"stream"`
	Temperature float64   `json:"temperature"`
}

// Analysis and RCA read evidence; a low temperature keeps a small model from
// wandering off it. Servers default to about 0.8.
const (
	rcaTemperature      = 0.2
	analysisTemperature = 0.3
)

type rcaRespChoice struct {
	Message      rcaMsg `json:"message"`
	FinishReason string `json:"finish_reason"`
}

type rcaUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type rcaResponse struct {
	Choices []rcaRespChoice `json:"choices"`
	Usage   rcaUsage        `json:"usage"`
}

// callLMStudioRCA sends a tool-calling request and returns the raw response.
// It is intentionally separate from callLMStudio to keep the RCA flow independent.
func callLMStudioRCA(msgs []rcaMsg, tools []rcaTool) (*rcaResponse, error) {
	if !acquireLLMSlotForUser() {
		return nil, fmt.Errorf("too many concurrent AI requests (%d/%d active) — try again shortly",
			getLLMActiveCalls(), getLLMMaxCalls())
	}
	defer releaseLLMSlot()
	body, _ := json.Marshal(rcaRequest{
		Model:       aiConfig.model,
		Messages:    msgs,
		Tools:       tools,
		Stream:      false,
		Temperature: rcaTemperature,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getLLMTimeout())*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", aiConfig.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LM Studio RCA: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var buf [1024]byte
		n, _ := resp.Body.Read(buf[:])
		body := strings.TrimSpace(string(buf[:n]))
		if isOverflowMessage(body) {
			return nil, &contextOverflowError{detail: body}
		}
		return nil, fmt.Errorf("LM Studio returned %d: %s", resp.StatusCode, body)
	}
	var out rcaResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if len(out.Choices) == 0 {
		return nil, fmt.Errorf("empty response from LM Studio")
	}
	return &out, nil
}

func callLMStudio(prompt string) (string, error) {
	msgs := []lmMsg{{Role: "user", Content: prompt}}
	if sp := getSystemPrompt(); sp != "" {
		msgs = append([]lmMsg{{Role: "system", Content: sp}}, msgs...)
	}
	body, _ := json.Marshal(lmReq{
		Model:    aiConfig.model,
		Messages: msgs,
		Stream:   false,
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(getLLMTimeout())*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "POST", aiConfig.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("LM Studio: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var buf [1024]byte
		n, _ := resp.Body.Read(buf[:])
		return "", fmt.Errorf("LM Studio returned %d: %s", resp.StatusCode, strings.TrimSpace(string(buf[:n])))
	}
	var out lmResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("decode: %w", err)
	}
	if len(out.Choices) == 0 {
		return "", fmt.Errorf("empty response from LM Studio")
	}
	return out.Choices[0].Message.Content, nil
}

// cleanLMResponse strips <think>…</think> blocks emitted by chain-of-thought models (e.g. Qwen3).
func cleanLMResponse(s string) string {
	if i := strings.Index(s, "</think>"); i >= 0 {
		s = strings.TrimSpace(s[i+8:])
	}
	return s
}
