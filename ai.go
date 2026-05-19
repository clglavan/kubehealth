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
}

var (
	aiConfig       aiCfgT
	aiInsightCache sync.Map // cacheKey → *AIInsight
	aiRunning      sync.Map // cacheKey → struct{} (dedup guard)
)

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
	if enabled {
		log.Printf("AI monitoring enabled — model: %s  url: %s", model, url)
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
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiConfig.model})
		return
	}
	if v, hit := aiInsightCache.Load(key); hit {
		json.NewEncoder(w).Encode(v.(*AIInsight))
		return
	}
	json.NewEncoder(w).Encode(AIInsight{Enabled: aiConfig.enabled, Model: aiConfig.model})
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
	triggerScopedAnalysis(key)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "triggered"})
}

func triggerScopedAnalysis(key string) {
	if _, loaded := aiRunning.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	go func() {
		defer aiRunning.Delete(key)
		var err error
		if ns, ok := strings.CutPrefix(key, "ns:"); ok {
			err = analyzeNSKey(key, ns)
		} else if obj, ok := strings.CutPrefix(key, "obj:"); ok {
			parts := strings.SplitN(obj, "/", 3)
			if len(parts) == 3 {
				err = analyzeObjKey(key, parts[0], parts[1], parts[2])
			}
		}
		if err != nil {
			log.Printf("AI [%s]: %v", key, err)
		}
	}()
}

func analyzeNSKey(key, ns string) error {
	var events []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		events = append(events, e.events...)
		e.RUnlock()
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Analyze all Kubernetes events in namespace %q.\n\n", ns)
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
	sb.WriteString("\nRespond exactly:\n**Summary**: <one sentence about overall namespace health>\n**Issues**:\n- <issue or 'None'>\n**Actions**:\n- <action or 'None'>\n\nBe concise and actionable.")
	return storeAnalysis(key, sb.String())
}

func analyzeObjKey(key, ns, kind, name string) error {
	var events []EventSummary
	if v, ok := nsCache.Load(ns); ok {
		e := v.(*nsEntry)
		e.RLock()
		for _, ev := range e.events {
			if strings.EqualFold(ev.Kind, kind) && ev.ObjectName == name {
				events = append(events, ev)
			}
		}
		e.RUnlock()
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Analyze events for %s %q in namespace %q.\n\n", kind, name, ns)
	if len(events) == 0 {
		sb.WriteString("No events recorded for this object.\n")
	} else {
		fmt.Fprintf(&sb, "Events (%d total):\n", len(events))
		for _, ev := range events {
			fmt.Fprintf(&sb, "- [%s] %s: %s  (×%d, %s ago)\n",
				ev.Type, ev.Reason, ev.Message, ev.Count, ev.Age)
		}
	}

	// Object describe — always included; gives the AI current spec/status/conditions
	info := fetchObjectInfo(ns, kind, name)
	switch {
	case info.NotFound:
		sb.WriteString("\nNote: object no longer exists in the cluster.\n")
	case info.Error == "":
		sb.WriteString("\nObject describe:\n")
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
	}

	// Pod logs — actual container output, most useful for crash diagnosis
	if strings.EqualFold(kind, "Pod") {
		logs, err := fetchLogsForAI(ns, name)
		if err == nil && strings.TrimSpace(logs) != "" {
			fmt.Fprintf(&sb, "\nPod logs (last %d lines):\n", getLogTailLines())
			sb.WriteString(logs)
		} else if err != nil {
			fmt.Fprintf(&sb, "\nNote: could not fetch pod logs: %v\n", err)
		}
	}

	sb.WriteString("\nRespond exactly:\n**Summary**: <one sentence about this specific resource>\n**Issues**:\n- <issue or 'None'>\n**Actions**:\n- <action or 'None'>\n\nBe concise and actionable.")
	return storeAnalysis(key, sb.String())
}

func storeAnalysis(key, prompt string) error {
	now := func() string { return time.Now().Format("2006-01-02 15:04:05 MST") }

	// Immediately mark as partial so the UI shows "Processing prompt…" instead of the
	// generic "Analyzing…" spinner while LM Studio processes the input tokens.
	aiInsightCache.Store(key, &AIInsight{
		Partial:   true,
		Model:     aiConfig.model,
		UpdatedAt: now(),
		Enabled:   true,
	})

	content, err := streamLMStudio(prompt, func(partial string) {
		aiInsightCache.Store(key, &AIInsight{
			Content:   cleanLMResponse(partial),
			Partial:   true,
			Model:     aiConfig.model,
			UpdatedAt: now(),
			Enabled:   true,
		})
	})

	if err != nil {
		log.Printf("AI analysis [%s]: %v", key, err)
		aiInsightCache.Store(key, &AIInsight{
			Error:     err.Error(),
			Model:     aiConfig.model,
			UpdatedAt: now(),
			Enabled:   true,
		})
		return err
	}
	aiInsightCache.Store(key, &AIInsight{
		Content:   cleanLMResponse(content),
		Model:     aiConfig.model,
		UpdatedAt: now(),
		Enabled:   true,
	})
	return nil
}

// streamLMStudio sends a streaming chat-completion request to LM Studio and calls
// writeFn with the accumulated content after each ~300ms batch of tokens.
// It enforces the global concurrent LLM call limit.
func streamLMStudio(prompt string, writeFn func(partial string)) (string, error) {
	if !acquireLLMSlot() {
		return "", fmt.Errorf("too many concurrent AI requests (%d/%d active) — try again shortly",
			getLLMActiveCalls(), getLLMMaxCalls())
	}
	defer releaseLLMSlot()

	sp := getSystemPrompt()
	msgs := []lmMsg{{Role: "user", Content: prompt}}
	if sp != "" {
		msgs = append([]lmMsg{{Role: "system", Content: sp}}, msgs...)
	}
	body, _ := json.Marshal(lmReq{
		Model:    aiConfig.model,
		Messages: msgs,
		Stream:   true,
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

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 64*1024)

	var full strings.Builder
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
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) > 0 {
			if tok := chunk.Choices[0].Delta.Content; tok != "" {
				full.WriteString(tok)
				if writeFn != nil && time.Since(lastWrite) >= 300*time.Millisecond {
					writeFn(full.String())
					lastWrite = time.Now()
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return full.String(), fmt.Errorf("stream read: %w", err)
	}
	return full.String(), nil
}

type lmMsg struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type lmReq struct {
	Model    string  `json:"model"`
	Messages []lmMsg `json:"messages"`
	Stream   bool    `json:"stream"`
}

type lmResp struct {
	Choices []struct {
		Message lmMsg `json:"message"`
	} `json:"choices"`
}

// ── Context-overflow sentinel ─────────────────────────────────────────────────

// contextOverflowError is returned when LM Studio rejects a request because the
// input tokens exceed the model's loaded context length.
// Detection uses LM Studio's documented error substring (bug-tracker issue #237).
type contextOverflowError struct{ detail string }

func (e *contextOverflowError) Error() string {
	return "context window exceeded: " + e.detail
}

func isContextOverflow(err error) bool {
	_, ok := err.(*contextOverflowError)
	return ok
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
	Model    string    `json:"model"`
	Messages []rcaMsg  `json:"messages"`
	Tools    []rcaTool `json:"tools,omitempty"`
	Stream   bool      `json:"stream"`
}

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
	if !acquireLLMSlot() {
		return nil, fmt.Errorf("too many concurrent AI requests (%d/%d active) — try again shortly",
			getLLMActiveCalls(), getLLMMaxCalls())
	}
	defer releaseLLMSlot()
	body, _ := json.Marshal(rcaRequest{
		Model:    aiConfig.model,
		Messages: msgs,
		Tools:    tools,
		Stream:   false,
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
		// LM Studio uses this specific phrase for context-window overflow (issue #237)
		if strings.Contains(body, "Trying to keep the first") {
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
